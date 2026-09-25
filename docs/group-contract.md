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
`{"ok":false,"error":{"code":...,"message":...}}`.

Prazo do pedido:

- O timeout é o `expiration` (em milissegundos) da própria mensagem AMQP; sem
  `expiration` válido o gateway usa **30 s**.
- O cliente manda também a propriedade AMQP `timestamp` — o instante do envio, em
  **segundos** (o `RpcClient` do backend manda `Math.floor(Date.now() / 1000)`). Com
  `timestamp` e `expiration`, o prazo conta do envio: `timestamp + 1 s + expiration`
  (o segundo a mais cobre o arredondamento para baixo do `timestamp`). Sem
  `timestamp`, o prazo conta de quando o gateway lê o pedido.
- Um pedido lido depois do próprio prazo é confirmado sem resposta (`ack` silencioso) —
  o cliente já desistiu e republicar não adianta. O handler roda com esse mesmo prazo.

Resposta: publicada com *publisher confirms* e prazo de 5 s. Se o broker recusar a
resposta (ou o confirm não chegar), o gateway registra o erro em log e confirma o pedido
mesmo assim — a operação já rodou, então repeti-la (ex. `group.create`) seria pior; o
cliente termina por timeout. No desligamento, cada operação em andamento tem até
`RpcDrainTimeout` (30 s) para terminar, independente do prazo de drenagem do consumidor
de comandos.

### Códigos de erro

| Código | Significado |
|---|---|
| `invalid_request` | Payload não é JSON válido, falta campo obrigatório ou `groupJid` não é um JID de grupo |
| `not_found` | O canal não tem sessão pareada nesta instância |
| `unavailable` | O canal está pareado mas não está conectado agora, o WhatsApp respondeu com limite de taxa (429), erro de servidor (500/503) ou não respondeu no prazo, ou a instância está desligando (ela não reabre o canal nesse momento) — transitório, vale tentar de novo depois |
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
| `photoUrl` | string | opcional — mesmo tratamento de `set_photo` (download de até 20 s e 8 MiB, JPEG com lado maior de 640 px); falha só gera log, não erro |
| `announce` | bool | opcional — grupo nasce só-admin-envia quando `true` |

Resposta:

```json
{"groupJid":"120363422547615282@g.us","inviteUrl":"https://chat.whatsapp.com/ABCDEFGHIJKLMNOPQRSTUV","participantCount":1,"createdAt":"2026-09-24T12:00:00Z"}
```

`participantCount` do `group.create` é o que o WhatsApp devolveu e nunca menos de 1 (o
próprio canal, criador do grupo). Em `group.info` e `group.joined` é o número que o
WhatsApp informa, sem piso — `0` quando ele não informa nenhum.

`inviteUrl` pode vir **vazio** com `ok: true`: o grupo foi criado e `groupJid` é o real,
mas o WhatsApp não entregou o link mesmo depois de três novas tentativas (200 ms, 500 ms
e 1 s, dentro do prazo do pedido). Nesse caso o cliente guarda o grupo como criado e
busca o link depois com `group.invite_link` (`reset: false`) — nunca chama
`group.create` de novo, que criaria um segundo grupo.

Timeout sugerido: **30 s** (cria o grupo, aplica descrição e foto — só o download da
foto pode levar até 20 s — e busca o link de convite, com as novas tentativas). É o
padrão do backend (`GATEWAY_RPC_CREATE_TIMEOUT_MS`).

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

Timeout sugerido: **10 s**.

## Comando

| | Exchange | Routing key |
|---|---|---|
| Comando | `whatsapp.gateway.group.v1` | o shard do canal, `crc32(channelId) % 1024` em decimal, como `gateway.send` (a fila usa `#`, então hoje qualquer chave chega) |
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

Paralelismo, ritmo e interrupção:

- Comandos de **canais diferentes rodam em paralelo** (até o prefetch da instância);
  comandos do **mesmo canal** rodam um depois do outro, na ordem em que chegaram.
- Entre dois grupos do mesmo canal o gateway espera de 300 a 800 ms (sorteado), mesmo
  quando são de comandos seguidos, para não disparar dezenas de alterações no WhatsApp
  em rajada. Grupos já concluídos numa reentrega não esperam.
- Cada grupo tem prazo de **30 s**. Em `set_photo` a imagem é baixada e convertida
  **uma vez** por comando, antes do primeiro grupo (download limitado a 20 s e 8 MiB).
- **Limite de taxa (429)** — e também erro 500/503, timeout ou o canal caindo no meio do
  comando — é `unavailable`: o gateway **para o comando** e o rejeita (nack sem requeue),
  o que o leva para a DLQ `gateway.group.dlq`, sem publicar falso `ok: false`. O gateway
  não reinjeta a DLQ: quem devolve o comando para `gateway.group` é o `DeadLetterWatch`
  do backend (no boot, quando a DLQ cresce e a cada hora). Na reentrega, os grupos já
  concluídos só republicam o resultado pelo ledger e os pendentes são aplicados.
- **Desligamento** no meio do comando: o grupo em andamento termina (ou falha por
  `unavailable` porque o canal já foi desconectado) e o comando volta para a fila
  `gateway.group` (nack com requeue depois de cancelar o consumidor) — nunca para a DLQ
  e nunca como `ok: false`; a próxima instância retoma do mesmo ponto pelo ledger. O
  requeue só acontece nesse caso: falha de um grupo fora do desligamento nunca devolve
  o comando à fila. O consumidor tem até `ShutdownDrainTimeout` (20 s) para drenar.

### Ações

| Ação | `params` usados | Efeito |
|---|---|---|
| `lock` | — | Liga "somente admin envia mensagem" (`announce`) |
| `unlock` | — | Desliga "somente admin envia mensagem" |
| `remove_participants` | `phones` (obrigatório) | Remove os números listados; participantes não encontrados no grupo são ignorados, sem erro; o próprio número do canal nunca é removido |
| `set_name` | `name` (obrigatório) | Renomeia o grupo; `invalid_request` se vazio |
| `set_description` | `description` | Define o tópico do grupo (aceita string vazia, que limpa o tópico). O gateway lê o grupo antes e manda o id do tópico atual como anterior; se o tópico já é o pedido, não reenvia |
| `set_photo` | `photoUrl` (obrigatório) | Baixa a imagem, reduz para o lado maior de **640 px** (limite da foto de grupo do WhatsApp), recodifica em JPEG e define como foto do grupo; JPEG que já cabe vai intacto. `invalid_request` se a URL não vier, a imagem não decodificar ou passar de 50 megapixels |

`phones` aceita o número com ou sem `+`; a correspondência tenta o `phoneNumber` do
participante, o `user` do JID e, quando o participante só tem LID, resolve o PN antes de
comparar. Número brasileiro (`55` com 12 ou 13 dígitos) casa também com a forma irmã do
nono dígito: `551188887777` encontra `5511988887777` e vice-versa.

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
| `removed` | int | só em `remove_participants` — quantos participantes o WhatsApp aceitou remover (entradas sem erro na resposta; pode ser `0` se nenhum telefone bateu com o grupo) |

`ok: false` não derruba o consumidor: o gateway registra o `error` e segue para o
próximo `groupJid` da lista. Com o canal offline **no início** do comando (não conecta
nem retomando a sessão), cada grupo sai com `ok: false` e `error` começando por
`unavailable:`, e o comando é confirmado — um canal fora do ar por horas não pode ficar
indo e voltando da DLQ. Se o canal cai **no meio** do comando, vale a regra de
`unavailable` acima (DLQ, sem `ok: false`). Não há reentrega automática de uma falha de negócio (ex.:
`set_name` num grupo em que o canal não é admin) — quem decide se tenta de novo é o
backend, com um novo `commandId`.

### `whatsapp.group.participants.v1`

Publicado sempre que o roster de um grupo muda — entrada, saída, remoção, promoção ou
rebaixamento a admin — vindo de um evento nativo do WhatsApp (`GroupInfo`), não do
comando acima. Um único evento nativo pode gerar mais de uma mensagem: uma por tipo de
mudança presente nele (ex. uma pessoa promovida ao mesmo tempo em que outra entra gera
um `join` e um `promoted` separados). Numa saída com vários participantes, cada um
recebe o próprio tipo: quem saiu por conta própria vai num `leave` e os demais num
`removed`, cada evento com seu `eventId`.

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
Um participante sem número resolvível vem só com `jid` (e `lid`); falha na resolução
gera log com canal, grupo e LID.

## Limitações conhecidas

- **`gateway.group` é fila compartilhada**, como `gateway.call`: a sessão do canal é
  estado preso à instância dona do socket. Hoje não causa `not_found` incorreto porque
  cada instância reivindica todos os shards; o backend já publica com o shard do canal
  como routing key, e a fila por shard fica para quando houver mais de uma instância
  ativa.
- **`whatsapp.group.participants.v1` depende do evento nativo `GroupInfo` do
  whatsmeow.** Uma mudança de roster feita fora da janela em que o canal está conectado
  (ex.: alguém remove um participante enquanto o canal estava offline) só aparece quando
  a sessão reconecta e a lib re-sincroniza — não há um evento retroativo por mudança
  perdida.
