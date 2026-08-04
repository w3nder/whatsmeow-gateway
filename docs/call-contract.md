# Contrato de chamadas — comandos e eventos

O gateway expõe a stack de voz e vídeo do WhatsApp por AMQP. Ele **sinaliza e grava**:
o áudio e o vídeo nunca trafegam pelo broker. A mídia fica dentro do processo do gateway,
é gravada em arquivo temporário e sobe para o S3; o broker carrega apenas JSON pequeno com
o ciclo de vida da chamada e a `key` da gravação.

```
WhatsApp ──RTP/mídia──▶ gateway ──arquivo──▶ S3
                          │
                          └──JSON──▶ RabbitMQ ──▶ backend ──▶ front
```

Para o operador **ouvir a chamada ao vivo** seria preciso um segundo transporte
(WebSocket ou WebRTC) entre gateway e front. Isso não existe hoje e está fora deste
contrato.

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
| `play` | `callId`, `mediaUrl` | Toca áudio PCM na chamada |
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
  O gateway não decodifica mp3/wav/opus; a conversão é do backend.
- **`video.play`** — `mediaUrl` aponta para um arquivo **H.264 Annex-B já codificado**.
  O gateway não tem encoder (imagem distroless, `CGO_ENABLED=0`): ele fatia o arquivo em
  access units e transmite. Quem codifica é o backend.

## Evento

```json
{
  "phoneNumberId": "5511999999999",
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

### Tipos de evento

| `type` | Quando | Campos além do comum |
|---|---|---|
| `incoming` | Chegou uma chamada | `isVideo` |
| `ringing` | Chamada de saída na linha | `isVideo`, `commandId` |
| `accepted` | Mídia começou | — |
| `ended` | Chamada encerrada | `duration`, `reason` |
| `recording` | Gravação da chamada subiu para o S3 | `media`, `videoMedia` |
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

`media`/`videoMedia` viajam num evento `recording` separado, **nunca no `ended`**.
Isso é deliberado, não uma omissão: a gravação sobe para o S3 depois que a chamada
já terminou, e esse upload pode ser lento ou falhar (bucket fora do ar, rede ruim).
Se `media`/`videoMedia` fossem campos do `ended`, publicar o `ended` teria que
esperar o upload — e um S3 lento passaria a atrasar, ou numa parada longa
efetivamente segurar, o evento que diz ao backend que a chamada acabou. Um caso
real: o upload levou 350 ms depois do `hangup`, e nesse intervalo o backend viu a
chamada como "ainda ativa" mesmo com o áudio já parado dos dois lados. `ended` sai
imediatamente, sem mídia; `recording` chega depois, quando o upload terminar. **Não
mova `media`/`videoMedia` de volta para o `ended`.**

```json
{
  "phoneNumberId": "5511999999999",
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
  }
}
```

O `recording` carrega até dois objetos:

- `media` — áudio, sempre WAV PCM 16 kHz mono (`audio/wav`), em `calls/<channelId>/<callId>.wav`
- `videoMedia` — vídeo, H.264 Annex-B cru (`video/h264`), em `calls/<channelId>/<callId>.h264`

Ambos carregam a **`key` do S3, nunca uma URL**. O backend resolve a key do mesmo jeito
que já resolve a mídia das mensagens (`InboundEvent.media.key`).

Uma chamada que não capturou nada não gera objeto, e nenhum `recording` é publicado —
não vem um evento com campos vazios. Se o upload falhar, também não sai `recording`;
sai só o `command.failed` com `recording_upload_failed` (ver abaixo).

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

## Configuração

| Variável | Default | Efeito |
|---|---|---|
| `GATEWAY_CALL_RECORD` | `true` | `false` desliga a gravação de todas as chamadas |
| `GATEWAY_CALL_TMPDIR` | temp do sistema | Onde a gravação é montada antes de subir |

## Limitações conhecidas

- **A biblioteca marca o caminho de mídia de vídeo como não validado**, tanto no envio
  quanto na recepção. A sinalização de vídeo é exercitada; o transporte dos frames precisa
  de uma chamada real de ponta a ponta para ser confirmado. Áudio não tem essa ressalva.
- **Só o áudio do peer é gravado.** O que o gateway toca com `play` não entra no WAV.
- **Chamada de grupo grava uma faixa só**, sem separação por participante.
- **`gateway.call` é fila compartilhada** e a chamada é estado preso à instância dona do
  socket. Com mais de uma instância ativa, um comando pode cair na instância errada e
  falhar com `call_not_found`. Hoje não acontece porque cada instância reivindica todos os
  shards. Roteamento por shard fica para quando houver escala horizontal.
