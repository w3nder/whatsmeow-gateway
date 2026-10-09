# Contrato de falha de envio (`whatsapp.status.v1`)

Quando um comando de envio (`GatewaySendCommand`, qualquer `kind`: texto, mídia, reação, edição, revogação) não chega ao WhatsApp, o gateway publica em `whatsapp.status.v1` um recibo `status: "failed"` e dá `ack` no comando (só erro de infraestrutura vai para a DLQ). O recibo mantém `providerMessageId`, `opaqueMessageId`, `timestamp` e `error { code, reason }`.

## `error.code`

| code | Quando sai |
|---|---|
| `session_missing` | O canal não tem sessão viva nem sessão guardada: o gateway nunca o pareou ou ele foi deslogado. Reenviar sem parear de novo não adianta. `reason` termina em `...is not paired: session: channel has no live session: <channelId>`. |
| `session_down` | A sessão existe (viva ou retomada sob demanda), mas o socket continuou caído depois de `connectWait` (10 s). O auto-reconnect segue rodando; é transitório. `reason` contém `session: socket still down after 10s, auto-reconnect still running`. |
| `gateway_send_error` | Todo o resto: falha ao montar a mensagem, mídia, recusa do WhatsApp, timeout de envio, falha ao retomar a sessão. |

A classificação é por erro tipado (`session.ErrNoSession`, `session.ErrSocketDown`, via `errors.Is`), nunca por texto. `error.reason` continua com o texto livre de sempre, inalterado, para consumidores que ainda casam o texto.

## Efeitos que não mudam

- Ack do comando depois de publicar o `failed`.
- Dedupe por `messageId`: a linha do ledger fica `pending` depois de um `failed`, então o reenvio do mesmo `messageId` é processado de novo.

## Respostas de RPC de grupos

`group.info` e demais RPCs de grupos não usam esses códigos: sem sessão devolvem `not_found` (`gateway: channel <id> is not paired: ...`) e socket caído devolve `unavailable`. O contrato RPC não mudou.

## `channel.status`

O gateway publica `connected` ao parear e a cada `events.Connected`; `disconnected` em `events.Disconnected`; `error` em `StreamReplaced`, `TemporaryBan`, `ClientOutdated`, `ConnectFailure`, `StreamError` e falha de pareamento. O formato do evento não mudou; só há dois casos novos, com `reason` estável:

| status | reason | Quando sai |
|---|---|---|
| `disconnected` | `device_logged_out` | `events.LoggedOut`: o aparelho desvinculou a sessão. Sai uma vez por logout, antes de a sessão ser apagada do registry; o `Disconnected` que o socket emite em seguida é suprimido e o canal só volta a publicar depois de um novo pareamento. O canal precisa parear de novo. |
| `error` | `resume_failed` | Falha ao retomar a sessão guardada no boot (JID inválido, factory ou `Connect` com erro). Só aquele canal; o boot e as outras sessões seguem. |

Situações que continuam sem `channel.status`:

- Falha ao retomar sob demanda no envio: o recibo `failed` com `session_missing`, `session_down` ou `gateway_send_error` já cobre.
- `KeepAliveTimeout`: apenas log; o `disconnected` só sai se o socket de fato cair.
- Gateway que cai ou reinicia: nada é publicado enquanto estiver fora; o canal volta com `connected` quando a sessão for retomada.
