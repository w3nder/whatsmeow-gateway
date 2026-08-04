# Integração meowcaller — chamadas de voz e vídeo no whatsmeow-gateway

Data: 2026-08-04
Status: aprovado para planejamento

## Objetivo

Expor no gateway a superfície completa da biblioteca [`github.com/purpshell/meowcaller`](https://github.com/purpshell/meowcaller):
chamadas 1:1 de áudio e vídeo, chamadas de grupo, call links com waiting room, reações,
hand raise e screen share. O gateway não faz mídia ao vivo com humano: ele **sinaliza e
grava**. A gravação sobe para o S3 e é entregue no evento pela `key`, exatamente como a
mídia das mensagens já é entregue hoje.

Quem decide o que fazer com a chamada é o backend. O gateway é transporte de comandos e
eventos, mais o gravador.

## Fatos que restringem o desenho

1. `meowcaller.NewClient` recebe um `*whatsmeow.Client` concreto e **precisa ser construído
   antes do `Connect()`** — ele instala a interceptação de `<call>`/`<ack>` no receive loop.
   Hoje `session.WAClient` é uma interface que esconde o cliente concreto.
2. A meowcaller **não codifica nem decodifica vídeo**. `Call.SendVideo` exige access units
   H.264 Annex-B prontos; `Call.ReceiveVideo` entrega Annex-B cru.
3. A imagem é `distroless/static` com `CGO_ENABLED=0`. Não há ffmpeg nem encoder nativo
   disponível no gateway, e não haverá.
4. Áudio é PCM `[]float32` mono (codec MLow, puro Go). 30 min de gravação a 16 kHz s16
   dá ≈ 57 MB — não cabe em buffer de memória por chamada.
5. `S3Store.Put` só aceita `[]byte`.
6. `gateway.send` é uma fila única compartilhada por todas as instâncias, e
   `ownership.ClaimAll` faz cada instância reivindicar todos os shards.
7. As deps da meowcaller batem com as nossas: mesmo pin de
   `go.mau.fi/whatsmeow v0.0.0-20260722203353-e9a033b24933`. Entram novas: `pion/dtls/v3`,
   `pion/sctp`, `pion/opus`, `pion/datachannel`, `rs/zerolog`, `hajimehoshi/go-mp3`.

## Decisões

| Decisão | Escolha | Motivo |
|---|---|---|
| Plano de mídia | Sinalização + gravação | Reaproveita `CallCdr`, upload S3 e transcrição que já existem no backend |
| Escopo | 100% da API pública da meowcaller, incluindo o experimental | Contrato AMQP nasce completo e não quebra depois |
| Vídeo de saída | Backend fornece URL de arquivo H.264 Annex-B | Único caminho sem CGO/ffmpeg no gateway |
| Gravação de áudio | Um WAV mixado por chamada | Direto para transcrição, casa com o `CallCdr` atual |
| Fila de comandos | Exchange e fila novas, separadas de `gateway.send` | Chamada é longa; compartilhar prefetch travaria envio de mensagem |

## Arquitetura

### `internal/session` — expor o cliente de chamadas

`session.NewWAClient` passa a construir o `meowcaller.Client` junto com o `whatsmeow.Client`,
antes de qualquer `Connect()`, e guarda os dois no `waClient`.

A interface `WAClient` ganha um método:

```go
Calls() call.Caller
```

`call.Caller` é uma interface **nossa**, não o tipo da lib, para que os fakes em
`test/fake_test.go` e `internal/mapper` continuem construíveis sem uma stack VoIP real:

```go
type Caller interface {
    Call(ctx context.Context, target string) (LiveCall, error)
    CallWithOptions(ctx context.Context, target string, video bool) (LiveCall, error)
    GroupCall(ctx context.Context, targets []string, opts GroupOptions) (LiveCall, error)
    GroupCallByID(ctx context.Context, groupID string, opts GroupOptions) (LiveCall, error)
    CreateCallLink(ctx context.Context, video bool) (Link, error)
    PreviewCallLink(ctx context.Context, tokenOrURL string, video bool) (LinkPreview, error)
    JoinCallLink(ctx context.Context, tokenOrURL string, video bool) (LiveCall, error)
    OnIncomingCall(fn func(LiveCall))
}
```

`LiveCall` espelha os métodos de `*meowcaller.Call` que o gateway usa. Um adaptador fino
em `internal/session` embrulha os tipos concretos da lib.

A meowcaller loga em `zerolog`; passamos `meowcaller.WithLogger` com um writer que
reencaminha para o `slog` do gateway, no mesmo nível dos outros logs de canal.

### `internal/call` — pacote novo

Responsabilidades, em unidades separadas:

- **`registry.go`** — `Registry`: `channelID → callID → *liveCall`. `liveCall` guarda peer,
  direção, fase, `startedAt`/`answeredAt`/`endedAt`, flags de vídeo e os gravadores.
  Operações: `Insert`, `Get`, `Remove`, `AbortChannel`.
- **`manager.go`** — `Manager.Attach(channelID, caller)` registra `OnIncomingCall` e assina
  todos os callbacks de uma chamada. Traduz callback → `CallEvent` e publica.
- **`command.go`** — traduz `GatewayCallCommand` → ação em `LiveCall`/`Caller`. Uma função
  por ação, tabela de despacho por `action`.
- **`recorder.go`** — gravação de áudio e vídeo para arquivo temporário, upload no fim.

Callbacks assinados: `OnReady`, `OnEnd`, `OnStateChange`, `OnPeerAccept`, `OnMuteState`,
`OnVideoState`, `OnReaction`, `OnGroupState`, `OnWaitingRoomState`, `OnHandRaise`,
`OnScreenShare`.

**Não publicados no broker:** `OnParticipantVideoFrame` e `OnVideoKeyframeRequest` disparam
por frame. Vão para o gravador e para o loop de keyframe, nunca para o AMQP — publicá-los
inundaria o `sender.events`.

### Ciclo de vida e teardown

`Manager.Attach` é chamado em `session.Manager.register`, junto com o `AddEventHandler`
existente. Em `LoggedOut`, `drop` e `DisconnectAll`, o registry do canal é abortado: os
gravadores fecham, o que já foi gravado sobe para o S3, e cada chamada viva publica
`ended` com `reason: "disconnected"`. Nenhuma chamada fica órfã segurando arquivo temp.

## Contrato AMQP — comandos

Topologia nova, mesmo padrão de `declareCommandTopology` (quorum + DLX + DLQ):

```
whatsapp.gateway.call.v1  (exchange)
gateway.call              (queue)
gateway.call.dlx / gateway.call.dlq
```

```go
type GatewayCallCommand struct {
    TenantID   string   `json:"tenantId"`
    ChannelID  string   `json:"channelId"`
    CommandID  string   `json:"commandId"`
    CallID     string   `json:"callId,omitempty"`
    Action     string   `json:"action"`
    To         string   `json:"to,omitempty"`
    Targets    []string `json:"targets,omitempty"`
    GroupID    string   `json:"groupId,omitempty"`
    Video      bool     `json:"video,omitempty"`
    MediaURL   string   `json:"mediaUrl,omitempty"`
    Emoji      string   `json:"emoji,omitempty"`
    Orientation int     `json:"orientation,omitempty"`
    Enabled    bool     `json:"enabled,omitempty"`
    Raised     bool     `json:"raised,omitempty"`
    Participant string  `json:"participant,omitempty"`
    LinkToken  string   `json:"linkToken,omitempty"`
    Record     *bool    `json:"record,omitempty"`
}
```

Ações, cobrindo 1:1 a API pública:

| Grupo | Ações |
|---|---|
| Discagem | `dial`, `group.dial`, `group.dial_by_id` |
| Atendimento | `answer`, `reject`, `hangup` |
| Áudio | `play` (`mediaUrl`: mp3/wav/opus) |
| Vídeo | `video.start`, `video.accept`, `video.stop`, `video.enable`, `video.orientation`, `video.play` (`mediaUrl`: `.h264` Annex-B) |
| Interação | `reaction`, `hand.raise`, `screenshare.start`, `screenshare.stop` |
| Grupo | `participant.add`, `participant.ring` |
| Call link | `link.create`, `link.preview`, `link.join` |
| Waiting room | `approval.set`, `participant.admit`, `participant.deny` |

Regras:

- Comandos de chamada **não passam pelo `dedupe.Store`**. São imperativos e não são
  idempotentes por `messageId`: reexecutar um `hangup` ou um `reaction` não é o mesmo que
  reexecutar um envio de mensagem.
- `callId` desconhecido → publica `command.failed` com código `call_not_found` e faz
  `Nack(requeue=false)`. Requeue causaria loop até o DLQ.
- `sendTimeout` não se aplica a `dial`: a chamada toca no destino por dezenas de segundos.
  Timeout próprio de ring, configurável (`GATEWAY_CALL_RING_TIMEOUT`, default 45 s).
- Toda ação responde com um evento `command.ack` ou `command.failed` carregando o
  `commandId`, para o backend correlacionar.

## Contrato AMQP — eventos

Routing key nova no `sender.events` já existente: `whatsapp.call.v1`.

```go
type CallEvent struct {
    PhoneNumberID string          `json:"phoneNumberId"`
    TenantID      string          `json:"tenantId"`
    ChannelID     string          `json:"channelId"`
    CallID        string          `json:"callId"`
    CommandID     string          `json:"commandId,omitempty"`
    From          string          `json:"from,omitempty"`
    SenderLid     string          `json:"senderLid,omitempty"`
    SenderPn      string          `json:"senderPn,omitempty"`
    Direction     string          `json:"direction"`           // inbound | outbound
    Type          string          `json:"type"`
    Timestamp     string          `json:"timestamp"`
    IsVideo       bool            `json:"isVideo,omitempty"`
    Duration      int             `json:"duration,omitempty"`  // segundos, só em ended
    Reason        string          `json:"reason,omitempty"`
    Media         *CallMedia      `json:"media,omitempty"`
    VideoMedia    *CallMedia      `json:"videoMedia,omitempty"`
    Video         *CallVideoState `json:"video,omitempty"`
    Reaction      *CallReaction   `json:"reaction,omitempty"`
    Group         *CallGroupState `json:"group,omitempty"`
    WaitingRoom   *CallWaitingRoom `json:"waitingRoom,omitempty"`
    Hand          *CallHandState  `json:"hand,omitempty"`
    ScreenShare   *CallScreenShare `json:"screenShare,omitempty"`
    Link          *CallLink       `json:"link,omitempty"`
    Error         *StatusError    `json:"error,omitempty"`
}

type CallMedia struct {
    Key      string `json:"key"`
    MimeType string `json:"mimeType"`
    Filename string `json:"filename,omitempty"`
    Duration int    `json:"duration,omitempty"`
}
```

`CallMedia` repete de propósito a forma de `InboundMedia`: `key` do S3, não URL. O backend
resolve a URL do mesmo jeito que já faz para mídia de mensagem.

Tipos de evento: `incoming`, `ringing`, `accepted`, `rejected`, `ended`, `state`,
`video.state`, `reaction`, `group.state`, `waitingroom.state`, `hand`, `screenshare`,
`link.created`, `link.preview`, `command.ack`, `command.failed`.

O campo `state` carrega a `CallPhase` da lib, normalizada em string:
`idle`, `calling`, `ringing`, `connecting`, `active`, `ended`, `waiting_room`.

## Gravação

Ativada por default para toda chamada atendida; `record: false` no comando de
`answer`/`dial` desliga por chamada.

- **Áudio** — `meowcaller.SinkFunc` recebe frames `[]float32`, converte para PCM s16
  little-endian e escreve num arquivo temporário. No fim, escreve o header WAV e sobe.
  Key: `calls/<channelId>/<callId>.wav`, mime `audio/wav`.
- **Vídeo** — `meowcaller.VideoSinkFunc` grava os access units Annex-B como recebidos.
  Key: `calls/<channelId>/<callId>.h264`, mime `video/h264`.

Uma chamada com vídeo produz dois objetos. O evento `ended` carrega o áudio em `media` e,
quando houver vídeo, o vídeo em `videoMedia` — mesmo tipo `CallMedia`, dois campos nomeados,
sem lista e sem evento extra.

Duas mudanças de infra que isso exige:

1. `S3Store` ganha `PutStream(ctx, key, mime string, r io.Reader) error` usando o manager de
   upload do SDK. `mapper.MediaStore` continua com `Put`; o gravador usa uma interface
   própria `call.RecordingStore` com `PutStream`, para não forçar todos os consumidores de
   `MediaStore` a implementar streaming.
2. Diretório de trabalho para os temporários: `GATEWAY_CALL_TMPDIR`, default `os.TempDir()`.
   A imagem distroless tem `/tmp`; em produção convém montar um volume. Arquivo temp é
   removido com `defer` mesmo em erro de upload.

Limite de segurança: `GATEWAY_CALL_MAX_DURATION` (default 2 h). Ao estourar, o gateway
derruba a chamada com `hangup`, sobe o que gravou e publica `ended` com
`reason: "max_duration"`.

## Vídeo de saída

`video.play` recebe `mediaUrl` apontando para um arquivo H.264 Annex-B já codificado. O
gateway baixa com o mesmo `fetchMediaURL` que já usa para mídia de mensagem, fatia em access
units pelos start codes e alimenta `Call.SendVideoWithDuration` no ritmo do arquivo.

Não há encoder no gateway e não haverá: quem produz o H.264 é o backend. Um `dial` com
`video: true` sem `video.play` posterior negocia vídeo e não envia frame nenhum — é uma
chamada de vídeo em que só recebemos.

## Estado, ownership e concorrência

Chamada é estado vivo, preso à instância que tem o socket do canal. `gateway.call` é fila
compartilhada, então um comando pode cair na instância errada. Hoje isso não acontece na
prática porque `ClaimAll` faz cada instância reivindicar todos os shards, ou seja, opera-se
com uma instância. O comportamento definido é: comando para um `callId` que não existe
localmente → `command.failed` com `call_not_found`, sem requeue.

Roteamento de comando por shard fica **registrado como limitação conhecida**, fora do escopo
desta entrega. Quando houver mais de uma instância ativa, será necessário publicar o comando
com routing key derivada do shard e cada instância bindar só os seus.

Um canal pode ter mais de uma chamada viva (grupo + 1:1). O registry é por canal, chaveado
por `callID`; nada assume chamada única.

## Erros

- Falha ao subir a gravação não derruba o `ended`: publica-se `ended` sem `media` e um
  `command.failed` com `recording_upload_failed`. Perder a gravação não pode esconder o fim
  da chamada do backend.
- Callback de lib que entra em pânico não pode derrubar o gateway: o dispatcher do
  `internal/call` recupera, loga com `callId` e segue.
- `dial` para número inválido falha na hora, com `command.failed` e código
  `invalid_target` — não vira chamada no registry.

## Testes

Unitários, no estilo do que já existe em `internal/mapper`:

- `internal/call/command_test.go` — cada `action` chama o método certo do `LiveCall` fake,
  com os argumentos certos; ações desconhecidas e `callId` ausente falham com o código certo.
- `internal/call/manager_test.go` — cada callback vira o `CallEvent` esperado; ciclo
  completo incoming → answer → ended com duração calculada; teardown de canal aborta as
  chamadas e publica `ended`.
- `internal/call/recorder_test.go` — frames `[]float32` viram WAV com header correto e
  amostras s16 esperadas; access units Annex-B saem byte a byte como entraram; `PutStream`
  recebe o conteúdo completo.
- `internal/session/client_test.go` — o adaptador expõe `Calls()` e o `meowcaller.Client` é
  construído antes do `Connect()`.

End-to-end em `test/`, no padrão dos testes atuais com testcontainers:

- `test/gateway_call_test.go` — publica comando em `gateway.call`, verifica o `CallEvent`
  correspondente em `sender.events` e o objeto gravado no MinIO.
- Reconexão: canal cai no meio da chamada, e o `ended` com `reason: "disconnected"` sai.

## Fora de escopo

- Áudio bidirecional ao vivo com humano (ponte para ramal/tecnovoz ou WebRTC no browser).
- Transcodificação para mp4/opus no gateway.
- Faixas de áudio separadas por participante em chamada de grupo.
- Roteamento de comando de chamada por shard entre múltiplas instâncias.
- Mensagens de chamada agendada (a lib também não suporta; o call link em si suporta).
