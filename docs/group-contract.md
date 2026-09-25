# Contrato de grupos — RPC, comando e eventos

O gateway expõe três formas de falar sobre grupos de WhatsApp: quatro operações **RPC**
(pedir/responder, para o backend consultar ou criar um grupo na hora), um **comando**
assíncrono para ações em lote sobre grupos já existentes, e dois **eventos** publicados
quando o estado de um grupo muda. Os três reaproveitam os mesmos tipos e a mesma
publicação de `sender.events` já descritos em [`call-contract.md`](./call-contract.md).

```
backend ──RPC (rpc.gateway.group.*)──▶ gateway ──▶ WhatsApp
backend ──comando (whatsapp.gateway.group.v1)──▶ gateway ──▶ WhatsApp
                                                      │
                                                      └──JSON──▶ RabbitMQ (sender.events) ──▶ backend
```

## RPC

As quatro operações usam o transporte RPC genérico do gateway: fila
`rpc.gateway.<operação>` (quorum), o pedido chega no corpo da mensagem, a resposta volta
por `replyTo`/`correlationId` como `{"ok":true,"result":...}` ou
`{"ok":false,"error":{"code":...,"message":...}}`. O timeout é o `expiration` (em
milissegundos) da própria mensagem AMQP; sem `expiration` válido o gateway usa **30 s**.
Uma mensagem que já passou do próprio `expiration` quando o gateway a lê é confirmada
sem resposta (`ack` silencioso) — republicar depois do timeout não adianta.

### Códigos de erro

| Código | Significado |
|---|---|
| `invalid_request` | Payload não é JSON válido, falta campo obrigatório ou `groupJid` não é um JID de grupo |
| `not_found` | O canal não tem sessão pareada nesta instância |
| `unavailable` | O canal está pareado mas não está conectado agora |
| `bad_gateway` | O WhatsApp recusou o pedido (ex.: grupo inexistente, sem permissão) |
| `internal` | O handler entrou em pânico ou a resposta não pôde ser serializada |

### `group.create`

Fila: `rpc.gateway.group.create`.

Pedido:

```json
{"tenantId":"tenant-status-roundtrip","channelId":"channel-groups","name":"GRUPO #1","description":"Regras","announce":true}
```

| Campo | Tipo | Quando |
|---|---|---|
| `tenantId` | string | sempre |
| `channelId` | string | sempre |
| `name` | string | sempre — obrigatório, `invalid_request` se vazio |
| `description` | string | opcional — falha em definir o tópico só gera log, não erro |
| `photoUrl` | string | opcional — falha em definir a foto só gera log, não erro |
| `announce` | bool | opcional — grupo nasce só-admin-envia quando `true` |

Resposta:

```json
{"groupJid":"120363422547615282@g.us","inviteUrl":"https://chat.whatsapp.com/ABCDEFGHIJKLMNOPQRSTUV","participantCount":1,"createdAt":"2026-09-24T12:00:00Z"}
```

Timeout sugerido: **10 s** (cria o grupo e já busca o link de convite).

### `group.invite_link`

Fila: `rpc.gateway.group.invite_link`.

Pedido:

```json
{"tenantId":"t","channelId":"channel-groups","groupJid":"120363422547615282@g.us","reset":true}
```

| Campo | Tipo | Quando |
|---|---|---|
| `tenantId` | string | sempre |
| `channelId` | string | sempre |
| `groupJid` | string | sempre |
| `reset` | bool | opcional — `true` revoga o link atual e gera um novo |

Resposta:

```json
{"inviteUrl":"https://chat.whatsapp.com/ZYXWVUTSRQPONMLKJIHGFE"}
```

Timeout sugerido: **10 s**.

### `group.info`

Fila: `rpc.gateway.group.info`.

Pedido:

```json
{"tenantId":"t","channelId":"channel-groups","groupJid":"120363422547615282@g.us"}
```

| Campo | Tipo | Quando |
|---|---|---|
| `tenantId` | string | sempre |
| `channelId` | string | sempre |
| `groupJid` | string | sempre |

Resposta:

```json
{"groupJid":"120363422547615282@g.us","name":"G","announce":false,"participantCount":1,"createdAt":"2026-09-24T12:00:00Z"}
```

Timeout sugerido: **10 s**.

### `group.joined`

Fila: `rpc.gateway.group.joined`.

Pedido:

```json
{"tenantId":"t","channelId":"channel-groups"}
```

| Campo | Tipo | Quando |
|---|---|---|
| `tenantId` | string | sempre |
| `channelId` | string | sempre |

Resposta — lista todos os grupos em que o canal está, cada um no mesmo formato de
`group.info`:

```json
{"groups":[{"groupJid":"120363422547615282@g.us","name":"G","announce":false,"participantCount":1,"createdAt":"2026-09-24T12:00:00Z"}]}
```

Timeout sugerido: **15 s** — a lista pode ter muitos grupos.

## Comando

| | Exchange | Routing key |
|---|---|---|
| Comando | `whatsapp.gateway.group.v1` | qualquer (a fila usa `#`) |
| Evento | `sender.events` | `whatsapp.group.action.v1` / `whatsapp.group.participants.v1` |

A fila de comandos é `gateway.group` (quorum, com DLX `gateway.group.dlx` e DLQ
`gateway.group.dlq`). É separada de `gateway.send` e `gateway.call` pelo mesmo motivo:
uma ação em lote sobre vários grupos não pode disputar prefetch com o envio de mensagens
nem com sinalização de chamada.

```json
{"commandId":"01J9ZK3S3Y2Q0N4R8T6V1W5X7Z","tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","action":"remove_participants","groupJids":["120363422547615282@g.us","120363422547615283@g.us"],"params":{"phones":["5511999887766"]}}
```

| Campo | Tipo | Quando |
|---|---|---|
| `commandId` | string | sempre — chave de deduplicação, volta em cada `whatsapp.group.action.v1` |
| `tenantId` | string | sempre |
| `channelId` | string | sempre |
| `action` | string | sempre |
| `groupJids` | string[] | sempre — um ou mais grupos; a ação roda em cada um e publica um evento por grupo |
| `params` | object | conforme a ação, ver abaixo |

O comando é idempotente por `(commandId, groupJid)`: reentregar o mesmo `commandId` para
o mesmo grupo não repete a ação no WhatsApp — o gateway republica o mesmo
`whatsapp.group.action.v1` (com o mesmo `removed`, quando houver) sem chamar a API de
novo.

### Ações

| Ação | `params` usados | Efeito |
|---|---|---|
| `lock` | — | Liga "somente admin envia mensagem" (`announce`) |
| `unlock` | — | Desliga "somente admin envia mensagem" |
| `remove_participants` | `phones` (obrigatório) | Remove os números listados; participantes não encontrados no grupo são ignorados, sem erro |
| `set_name` | `name` (obrigatório) | Renomeia o grupo; `invalid_request` se vazio |
| `set_description` | `description` | Define o tópico do grupo (aceita string vazia, que limpa o tópico) |
| `set_photo` | `photoUrl` (obrigatório) | Baixa a imagem, converte para JPEG e define como foto do grupo; `invalid_request` se a URL não vier ou a imagem não converter |

`phones` aceita o número com ou sem `+`; a correspondência tenta o `phoneNumber` do
participante, o `user` do JID e, quando o participante só tem LID, resolve o PN antes de
comparar.

## Eventos

### `whatsapp.group.action.v1`

Um evento por grupo do comando, publicado depois que a ação foi aplicada (ou já estava
feita, no caso de reentrega).

```json
{"tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","commandId":"01J9ZK3S3Y2Q0N4R8T6V1W5X7Z","groupJid":"120363422547615282@g.us","action":"remove_participants","ok":true,"removed":1}
```

| Campo | Tipo | Quando |
|---|---|---|
| `tenantId` | string | sempre |
| `channelId` | string | sempre |
| `commandId` | string | sempre — para o backend correlacionar com o comando |
| `groupJid` | string | sempre |
| `action` | string | sempre |
| `ok` | bool | sempre |
| `error` | string | só quando `ok` é `false` |
| `removed` | int | só em `remove_participants` — quantos participantes foram de fato removidos (pode ser `0` se nenhum telefone bateu com o grupo) |

`ok: false` não derruba o consumidor: o gateway registra o `error` e segue para o
próximo `groupJid` da lista. Não há reentrega automática de uma falha de negócio (ex.:
`set_name` num grupo em que o canal não é admin) — quem decide se tenta de novo é o
backend, com um novo `commandId`.

### `whatsapp.group.participants.v1`

Publicado sempre que o roster de um grupo muda — entrada, saída, remoção, promoção ou
rebaixamento a admin — vindo de um evento nativo do WhatsApp (`GroupInfo`), não do
comando acima. Um único evento nativo pode gerar mais de uma mensagem: uma por tipo de
mudança presente nele (ex. uma pessoa promovida ao mesmo tempo em que outra entra gera
um `join` e um `promoted` separados).

```json
{"tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","groupJid":"120363422547615282@g.us","type":"join","participants":[{"jid":"2002125877314@lid","lid":"2002125877314@lid","phone":"5511999887766"}],"eventId":"3f1c2a9b8d7e6f5a4b3c2d1e0f9a8b7c","occurredAt":"2026-09-24T12:00:00Z"}
```

| Campo | Tipo | Quando |
|---|---|---|
| `tenantId` | string | sempre |
| `channelId` | string | sempre |
| `groupJid` | string | sempre |
| `type` | string | sempre — `join`, `leave`, `removed`, `promoted` ou `demoted` |
| `participants` | array | sempre — um ou mais participantes afetados por esse `type` |
| `eventId` | string | sempre — determinístico (hash do grupo, tipo, timestamp e JIDs ordenados); usar para deduplicar |
| `occurredAt` | string | sempre — RFC3339, UTC |

`type` distingue **saída voluntária** (`leave`) de **remoção por um admin** (`removed`):
o gateway compara quem saiu do grupo com quem disparou o evento nativo — se o próprio
participante que saiu é o remetente do evento, é `leave`; senão é `removed`. Sem
remetente identificável no evento nativo, o gateway assume `removed`.

Cada `participants[]` traz `jid` sempre; `lid` só quando o participante tem LID (JID
oculto do `@lid`); `phone` quando o gateway consegue resolver o número — via o próprio
JID (`@s.whatsapp.net`) ou, para participantes só com LID, via a resolução PN-por-LID.
Um participante sem número resolvível vem só com `jid`.

## Limitações conhecidas

- **`gateway.group` é fila compartilhada**, como `gateway.call`: a sessão do canal é
  estado preso à instância dona do socket. Hoje não causa `not_found` incorreto porque
  cada instância reivindica todos os shards; roteamento por shard fica para quando
  houver mais de uma instância ativa.
- **`whatsapp.group.participants.v1` depende do evento nativo `GroupInfo` do
  whatsmeow.** Uma mudança de roster feita fora da janela em que o canal está conectado
  (ex.: alguém remove um participante enquanto o canal estava offline) só aparece quando
  a sessão reconecta e a lib re-sincroniza — não há um evento retroativo por mudança
  perdida.
