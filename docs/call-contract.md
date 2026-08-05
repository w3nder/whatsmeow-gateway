# Contrato de chamadas — comandos e eventos

O gateway expõe a stack de voz e vídeo do WhatsApp por AMQP. **O áudio e o vídeo nunca
trafegam pelo broker**: a mídia fica dentro do processo do gateway, é gravada em arquivo
temporário e sobe para o S3; o broker carrega apenas JSON pequeno com o ciclo de vida da
chamada e a `key` da gravação.

Para o operador **ouvir e falar na chamada ao vivo** existe um segundo transporte,
separado do broker: um WebSocket autenticado por token, servido pelo próprio gateway
(ver [Mídia ao vivo](#mídia-ao-vivo-websocket)).

```
WhatsApp ──RTP/mídia──▶ gateway ──arquivo──▶ S3
                          │  │
                          │  └──WebSocket (PCM/H.264)──▶ front (operador)
                          │
                          └──JSON──▶ RabbitMQ ──▶ backend ──▶ front
```

## Onde publicar e onde ouvir

| | Exchange | Routing key |
|---|---|---|
| Comando | `whatsapp.gateway.call.v1` | qualquer (a fila usa `#`) |
| Evento | `sender.events` | `whatsapp.call.v1` |

A fila de comandos é `gateway.call` (quorum, com DLX `gateway.call.dlx` e DLQ
`gateway.call.dlq`). É separada de `gateway.send` porque uma chamada é longa e
compartilhar prefetch travaria o envio de mensagens.

## Comando

```json
{
  "tenantId": "tenant-1",
  "channelId": "channel-1",
  "commandId": "cmd-1",
  "callId": "A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4",
  "action": "answer"
}
```

| Campo | Tipo | Quando |
|---|---|---|
| `tenantId` | string | sempre |
| `channelId` | string | sempre |
| `commandId` | string | sempre — volta no `command.ack`/`command.failed` para correlação |
| `callId` | string | toda ação sobre uma chamada existente |
| `action` | string | sempre |
| `to` | string | `dial` |
| `targets` | string[] | `group.dial` (mínimo 2) |
| `groupId` | string | `group.dial_by_id`, opcional em `group.dial` |
| `video` | bool | `dial`, `group.dial*`, `link.*` |
| `mediaUrl` | string | `play`, `video.play` |
| `emoji` | string | `reaction` |
| `orientation` | int (0..3) | `video.orientation` |
| `enabled` | bool | `video.enable`, `approval.set` |
| `raised` | bool | `hand.raise` |
| `participant` | string | `participant.add`, `participant.ring`, `participant.admit`, `participant.deny` |
| `linkToken` | string | `link.preview`, `link.join` |
| `record` | bool | opcional; `false` desliga a gravação só desta chamada |

### Ações

| Ação | Campos obrigatórios | Efeito |
|---|---|---|
| `dial` | `to` | Liga para um número ou JID |
| `group.dial` | `targets` (≥2) | Liga para vários destinos; `groupId` vincula ao grupo |
| `group.dial_by_id` | `groupId` | Resolve o grupo e liga para todos os membros remotos |
| `answer` | `callId` | Atende |
| `reject` | `callId` | Rejeita |
| `hangup` | `callId` | Desliga |
| `play` | `callId`, `mediaUrl` | Toca áudio PCM na chamada (toma o canal do microfone do operador enquanto durar) |
| `video.start` | `callId` | Inicia vídeo (upgrade) |
| `video.accept` | `callId` | Aceita o upgrade do peer |
| `video.stop` | `callId` | Encerra o vídeo |
| `video.enable` | `callId`, `enabled` | Liga/desliga a câmera |
| `video.orientation` | `callId`, `orientation` | Informa a orientação |
| `video.play` | `callId`, `mediaUrl` | Transmite um arquivo H.264 Annex-B |
| `reaction` | `callId`, `emoji` | Envia reação (emoji vazio remove) |
| `hand.raise` | `callId`, `raised` | Levanta/abaixa a mão |
| `screenshare.start` | `callId` | Começa compartilhamento de tela |
| `screenshare.stop` | `callId` | Encerra o compartilhamento |
| `participant.add` | `callId`, `participant` | Adiciona à chamada de grupo |
| `participant.ring` | `callId`, `participant` | Toca de novo para um participante |
| `approval.set` | `callId`, `enabled` | Liga/desliga a sala de espera |
| `participant.admit` | `callId`, `participant` | Admite da sala de espera |
| `participant.deny` | `callId`, `participant` | Recusa da sala de espera |
| `link.create` | — | Cria um call link reutilizável |
| `link.preview` | `linkToken` | Lê os metadados do link sem entrar |
| `link.join` | `linkToken` | Entra pelo link (ou na sala de espera) |

### Formato da mídia nos comandos

- **`play`** — `mediaUrl` aponta para **PCM cru**: signed 16-bit little-endian, mono, 16 kHz.
  O gateway não decodifica mp3/wav/opus; a conversão é do backend. O `command.ack` diz que
  o áudio foi enfileirado, não que já tocou: ele sai na cadência de 60 ms da chamada.
- **`video.play`** — `mediaUrl` aponta para um arquivo **H.264 Annex-B já codificado**.
  O gateway não tem encoder (imagem distroless, `CGO_ENABLED=0`): ele fatia o arquivo em
  access units e transmite. Quem codifica é o backend.

## Evento

```json
{
  "phoneNumberId": "channel-1",
  "tenantId": "tenant-1",
  "channelId": "channel-1",
  "callId": "A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4",
  "from": "5511888888888@s.whatsapp.net",
  "direction": "inbound",
  "type": "ended",
  "timestamp": "1754300100",
  "duration": 42,
  "reason": "hangup"
}
```

`direction` é `inbound` ou `outbound`. `timestamp` é epoch em segundos, como string.
Campos ausentes são omitidos: dá para distinguir "sem gravação" de "gravação vazia".

`phoneNumberId` **não é o número de telefone do dispositivo** — é o `channelId`.
O backend resolve o canal por esse campo, e para um canal do gateway a coluna
`phoneNumberId` guarda o próprio UUID do canal, não um número. O gateway
preenche os dois campos com o mesmo valor; não popule `phoneNumberId` com o
JID do dispositivo nem com o número real da linha.

### Tipos de evento

| `type` | Quando | Campos além do comum |
|---|---|---|
| `incoming` | Chegou uma chamada | `isVideo` |
| `ringing` | Chamada de saída na linha | `isVideo`, `commandId` |
| `accepted` | Mídia começou | — |
| `ended` | Chamada encerrada | `duration`, `reason` |
| `recording` | Gravação da chamada subiu para o S3 | `media`, `peerMedia`, `operatorMedia`, `videoMedia` |
| `state` | Fase mudou ou mute mudou | `state`, `muted` |
| `video.state` | Estado de vídeo do peer | `video` |
| `reaction` | Reação recebida | `reaction` |
| `group.state` | Roster do grupo | `group` |
| `waitingroom.state` | Sala de espera | `waitingRoom` |
| `hand` | Mão levantada/abaixada | `hand` |
| `screenshare` | Compartilhamento de tela | `screenShare` |
| `link.created` | Resposta de `link.create` | `link`, `commandId` |
| `link.preview` | Resposta de `link.preview` | `link`, `commandId` |
| `command.ack` | Comando executado | `commandId` |
| `command.failed` | Comando falhou | `commandId`, `error` |

`state` assume: `idle`, `calling`, `ringing`, `connecting`, `active`, `ended`,
`waiting_room` e `unknown`.

### Gravação

Os campos de mídia viajam num evento `recording` separado, **nunca no `ended`**.
Isso é deliberado, não uma omissão: a gravação sobe para o S3 depois que a chamada
já terminou, e esse upload pode ser lento ou falhar (bucket fora do ar, rede ruim).
Se fossem campos do `ended`, publicar o `ended` teria que esperar o upload — e um S3
lento passaria a atrasar, ou numa parada longa efetivamente segurar, o evento que diz
ao backend que a chamada acabou. Um caso real: o upload levou 350 ms depois do
`hangup`, e nesse intervalo o backend viu a chamada como "ainda ativa" mesmo com o
áudio já parado dos dois lados. `ended` sai imediatamente, sem mídia; `recording`
chega depois, quando o upload terminar. **Não mova a mídia de volta para o `ended`.**

```json
{
  "phoneNumberId": "channel-1",
  "tenantId": "tenant-1",
  "channelId": "channel-1",
  "callId": "A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4",
  "direction": "inbound",
  "type": "recording",
  "timestamp": "1754300100",
  "media": {
    "key": "calls/channel-1/A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4.wav",
    "mimeType": "audio/wav",
    "filename": "A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4.wav",
    "duration": 42
  },
  "peerMedia": {
    "key": "calls/channel-1/A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4-peer.wav",
    "mimeType": "audio/wav",
    "filename": "A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4-peer.wav",
    "duration": 42
  },
  "operatorMedia": {
    "key": "calls/channel-1/A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4-operator.wav",
    "mimeType": "audio/wav",
    "filename": "A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4-operator.wav",
    "duration": 42
  }
}
```

O `recording` carrega até quatro objetos — **três faixas de áudio e uma de vídeo**:

| Campo | Key | Conteúdo |
|---|---|---|
| `media` | `calls/<channelId>/<callId>.wav` | Os dois lados **misturados** em mono — é a faixa que o player do chat toca |
| `peerMedia` | `calls/<channelId>/<callId>-peer.wav` | Só o cliente |
| `operatorMedia` | `calls/<channelId>/<callId>-operator.wav` | Só o operador (inclui o que o `play` transmitiu) |
| `videoMedia` | `calls/<channelId>/<callId>.h264` | Vídeo do peer, H.264 Annex-B cru (`video/h264`) |

As três faixas de áudio são sempre WAV PCM 16 kHz mono (`audio/wav`). Todos os quatro
carregam a **`key` do S3, nunca uma URL**. O backend resolve a key do mesmo jeito que já
resolve a mídia das mensagens (`InboundEvent.media.key`).

**Por que separar.** Mandadas individualmente para a transcrição, `peerMedia` e
`operatorMedia` já dizem quem falou, sem nenhuma etapa de diarização: o que está no
arquivo do cliente é do cliente, por construção. O `media` continua sendo a mistura
porque é o que uma pessoa escuta.

**As três faixas têm exatamente o mesmo tamanho e a mesma linha do tempo.** O gateway
não escreve cada lado conforme ele chega — os dois chegam em ritmos diferentes (o do
cliente quando o relay entrega, o do operador quando o navegador manda), e escrever na
chegada faria as faixas derivarem uma da outra. Um relógio interno escreve um frame de
60 ms de cada lado por tique, silêncio quando um lado não tem nada. Um lado calado por
dez segundos rende dez segundos de silêncio na faixa dele, no lugar certo. Alinhar
`peerMedia` e `operatorMedia` pelo índice da amostra é seguro.

A mistura é a **soma dos dois lados com clamp**, não a média. Dividir por dois cortaria
o volume pela metade na maior parte de qualquer chamada, em que só um lado fala por vez;
o clamp só custa alguma coisa nos trechos em que os dois falam alto ao mesmo tempo.

Uma chamada que não capturou **nada** não gera objeto nenhum, e nenhum `recording` é
publicado — não vem um evento com campos vazios. Mas uma chamada em que só um lado falou
gera **as três** faixas: a faixa do lado calado é silêncio, não ausência. Um 404 em
`-operator.wav` não distinguiria "o operador não falou" de "o upload falhou"; uma faixa
silenciosa do tamanho certo diz isso sem ambiguidade. Se o upload falhar, também não sai
`recording`; sai só o `command.failed` com `recording_upload_failed` (ver abaixo).

O `.h264` cru toca em ffmpeg e VLC, **não em browser**. A conversão para mp4 é do backend.

`duration` no `ended` é medido do atendimento até o fim, não da oferta: uma chamada que
tocou 40 s e conversou 10 s reporta 10. Uma chamada não atendida reporta 0.

### Códigos de erro

| Código | Significado |
|---|---|
| `call_not_found` | Não existe chamada viva com esse `callId` nesta instância |
| `unknown_action` | `action` não reconhecida |
| `invalid_target` | Faltou `to`, `targets`, `groupId` ou `linkToken` |
| `action_failed` | A stack de chamadas recusou a ação; `reason` traz o motivo |
| `no_caller` | O canal não tem cliente de chamadas |
| `media_fetch_failed` | `mediaUrl` não baixou, veio vazio ou não é H.264 válido |
| `recording_upload_failed` | O upload da gravação falhou; sai mesmo assim, sem `recording` |

Um comando malformado nunca é reenfileirado: ele é reportado como `command.failed` e
acked. Reenfileirar faria loop até o DLQ.

`hangup` e `reject` são exceção a `call_not_found`: os dois expressam um estado final
desejado ("essa chamada não deve mais existir"), então se o `callId` já não está mais
registrado esse estado já vale — o comando sai como `command.ack`, não como
`command.failed`. Isso importa porque é uma corrida fácil de cair: o front pode mandar
`hangup` no mesmo instante em que o peer desliga por conta própria, e as duas coisas
terminam a chamada de qualquer forma. Toda outra ação sobre um `callId` inexistente
continua falhando com `call_not_found`, porque para elas (`answer`, `video.*`, `play`,
`reaction`, etc.) uma chamada ausente realmente significa que a ação não pode acontecer.

## Mídia ao vivo (WebSocket)

O operador ouve e fala na chamada por um WebSocket servido pelo gateway. Ele é **um
transporte separado do broker**: nenhum byte de mídia passa por AMQP.

```
GET ws://<CALL_MEDIA_ADDR>/calls/{callID}/media?token=<jwt>
```

O listener só existe quando `CALL_MEDIA_ADDR` está configurado. Sem ele o gateway roda
como antes: sinaliza e grava, sem mídia ao vivo.

### Token

O token é um JWT **HS256** assinado com `CALL_MEDIA_TOKEN_SECRET` — quem emite é o
backend, não o gateway. `exp` é obrigatório (um token sem expiração é recusado), e todas
as claims abaixo precisam estar presentes e não vazias:

| Claim | Conteúdo |
|---|---|
| `tenantId` | tenant do operador |
| `channelId` | canal dono da chamada |
| `callId` | chamada autorizada — precisa bater com o `{callID}` da URL |
| `userId` | operador, para auditoria |
| `exp` | obrigatório; emita com validade curta |

O token vale para **uma** chamada: um token de `A` não abre a mídia de `B` (403). Token
inválido/expirado é 401; chamada que não existe (ou já acabou) é 404. Todas as recusas
acontecem **antes** do upgrade, então um handshake que completa já significa que a
chamada existia.

`CALL_MEDIA_ALLOWED_ORIGINS` precisa listar a origem do front, senão o navegador é
recusado pela checagem same-origin do servidor.

### Formato dos frames

Cada mensagem binária do WebSocket é **um** frame: um byte de tipo seguido do payload.
Não há prefixo de tamanho — a fronteira da mensagem é a fronteira do frame.

| Byte | Tipo | Direção | Payload |
|---|---|---|---|
| `0x01` | áudio | ambas | PCM s16le mono 16 kHz |
| `0x02` | vídeo | ambas | access unit H.264 Annex-B |
| `0x03` | keyframe | gateway → operador | vazio — "seu encoder precisa emitir um IDR agora" |
| `0x04` | end | gateway → operador | vazio — a chamada acabou; o gateway fecha em seguida |

O `0x03` chega logo na conexão (quem entra no meio da chamada perdeu todos os keyframes
anteriores) e de novo sempre que o peer pedir um IDR depois de perda de pacote.

Frames do operador são limitados a 1 MiB — um IDR de webcam passa folgado dos 32 KiB
default da biblioteca de WebSocket.

### Precedência do áudio de saída

A chamada tem **uma** fonte de áudio de saída e dois produtores. A regra, explícita:

- Um `play` (prompt de URA, mensagem de espera) **toma o canal** enquanto durar, e
  substitui um `play` anterior que ainda esteja tocando.
- O microfone do operador volta sozinho assim que o `play` termina. Os frames que ele
  mandou durante o `play` são descartados, não enfileirados: áudio ao vivo atrasado é
  pior que áudio perdido.
- Sem operador e sem `play`, o gateway transmite silêncio — não para de transmitir. Isso
  é obrigatório: o relay do WhatsApp só devolve a mídia do peer depois de ver o nosso
  fluxo, então um operador que só escuta não pode fazer o gateway parar de emitir.

Conectar e desconectar o operador não interrompe nada disso.

É dessa fonte única que sai o `operatorMedia` da gravação: o que é gravado como "lado do
operador" é exatamente o que o peer ouviu, `play` incluído. Frame que o operador
enfileirou mas a chamada nunca transmitiu (descartado durante um `play`, ou perdido no
limite da fila) não entra na gravação, porque nunca foi ouvido.

## Configuração

| Variável | Default | Efeito |
|---|---|---|
| `GATEWAY_CALL_RECORD` | `true` | `false` desliga a gravação de todas as chamadas |
| `GATEWAY_CALL_TMPDIR` | temp do sistema | Onde a gravação é montada antes de subir |
| `CALL_MEDIA_ADDR` | vazio (desligado) | Endereço do listener de mídia ao vivo, ex. `:8081` |
| `CALL_MEDIA_TOKEN_SECRET` | — | Segredo HS256 dos tokens; **obrigatório** se `CALL_MEDIA_ADDR` estiver setado |
| `CALL_MEDIA_ALLOWED_ORIGINS` | vazio (só same-origin) | Origens do front autorizadas a fazer upgrade, separadas por vírgula |

## Limitações conhecidas

- **A biblioteca marca o caminho de mídia de vídeo como não validado**, tanto no envio
  quanto na recepção. A sinalização de vídeo é exercitada; o transporte dos frames precisa
  de uma chamada real de ponta a ponta para ser confirmado. Áudio não tem essa ressalva.
- **Só o vídeo do peer é gravado.** O vídeo que o operador transmite não entra no
  `.h264`. O áudio dos dois lados entra, em três faixas (ver [Gravação](#gravação)).
- **Chamada de grupo mistura todos os participantes remotos numa faixa só**: o
  `peerMedia` de um grupo não separa quem falou entre eles. A separação garantida é
  entre o lado remoto e o operador, que é o que a stack de mídia dá.
- **`gateway.call` é fila compartilhada** e a chamada é estado preso à instância dona do
  socket. Com mais de uma instância ativa, um comando pode cair na instância errada e
  falhar com `call_not_found`. Hoje não acontece porque cada instância reivindica todos os
  shards. Roteamento por shard fica para quando houver escala horizontal.
