# Contrato do histórico — importação ao parear

Quando o backend pede o pareamento de um canal com `importHistory: true`, o gateway pede o
histórico ao telefone, traduz cada mensagem pelo mesmo caminho do ao vivo e publica o
histórico em lotes. O backend importa os lotes num caminho próprio, sem disparar nada.

```
backend ──pair (importHistory, importId)──▶ gateway ──pareia com DeviceProps de histórico──▶ telefone
telefone ──pedaços do histórico──▶ gateway ──whatsapp.history.v1 (lotes + fim)──▶ RabbitMQ ──▶ backend
```

## Comando de pareamento

Troca `whatsapp.gateway.pair.v1`, fila `gateway.pair` (sem mudança de topologia). O payload
de hoje ganha dois campos:

```json
{"tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","userId":"4d6e8f0a-1b2c-4d3e-9f5a-6b7c8d9e0f1a","importHistory":true,"importId":"3c5d7e9f-1a2b-4c3d-8e4f-5a6b7c8d9e0f"}
```

| Campo | Tipo | Efeito |
|---|---|---|
| `importHistory` | bool | `true`: pareia com os `DeviceProps` de histórico e importa. Ausente ou `false`: pareia como hoje e não publica nada |
| `importId` | string | obrigatório com `importHistory: true`; sem ele não há importação |

Os literais do comando (com importação, sem importação e o de antes do histórico) vivem em
`internal/amqp/pair_contract_test.go` e não mudam: o backend os copia de lá byte a byte.

A importação é gravada no registro **antes** do QR. Um segundo pedido com importação para o
mesmo canal substitui o `importId`: daí em diante nada (lote ou fim) é publicado para o
anterior. Canal que já estava pareado (conecta sem QR) não recebe histórico: o gateway publica
o fim com `totalBatches: 0` e `skippedChats: 0` na hora.

Um pedido **sem** importação apaga a importação do canal no registro (inclusive uma encerrada
ou esquecida de um pareamento anterior), para que nenhuma retomada volte a baixar histórico que
o backend descarta. Se um pareamento com importação estiver mostrando o QR para o mesmo canal,
ele é encerrado e recomeça sem histórico, com os props de sempre (o espelho do caminho
contrário: um pedido com importação encerra o pareamento sem histórico em curso e recomeça com
os props de histórico). Pedidos iguais ao que está em curso entram nele e recebem o QR vigente.

### Exclusividade por instância

O pareamento que importa usa os `DeviceProps` de histórico do primeiro `Connect` até o fim do
pareamento (sucesso, erro ou fim do QR) e é exclusivo **na instância do gateway**: nenhum outro
pareamento começa enquanto ele mostra o QR, e ele espera os que estão em curso. Pareamentos sem
histórico continuam simultâneos entre si, com os props de sempre.

A espera pela vez tem teto de 90 segundos (`deviceprops.SlotWait`), para pareamentos com e sem
histórico. Estourado o teto, o pareamento falha como toda falha de pareamento, com
`channel.status` `error`, e o comando é confirmado (ack), nunca vai para a DLQ nem prende o
prefetch:

| Quem esperou | `reason` |
|---|---|
| pareamento sem histórico, atrás de um com histórico | `Outro pareamento com histórico está em andamento neste servidor. Tente novamente em instantes.` |
| pareamento com histórico, atrás de outros em curso | `Há outros pareamentos em andamento neste servidor e o pareamento com histórico precisa acontecer sozinho. Tente novamente em instantes.` |

A fila de espera é por ordem de chegada: um pareamento sem histórico que chega depois de um com
histórico ainda na fila espera por ele, mas nunca mais que o mesmo teto. O custo da escolha:
pareamentos com histórico são serializados por instância, e um QR de histórico que ninguém lê
segura os demais pareamentos daquela instância até o fim do QR. O tempo de vida do QR é o do
whatsmeow (primeiro código 60 s, os seguintes 20 s, cerca de 160 s ao todo) e não foi encurtado:
quem espera desiste em 90 s e tenta de novo.

## Onde publicar e onde ouvir

| | Exchange | Routing key |
|---|---|---|
| Lote e fim | `sender.events` | `whatsapp.history.v1` |

Publicação persistente e com confirmação do broker, um lote por vez por canal, com intervalo
entre lotes. Os pedaços de um canal são tratados por um único trabalhador, estritamente na ordem
em que chegaram do telefone; canais diferentes andam em paralelo (até 2 pedaços ao mesmo tempo
por instância). O backend declara a fila `ingestion.history` (DLX, DLQ e retry).

## Lote

```json
{"kind":"batch","tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","importId":"3c5d7e9f-1a2b-4c3d-8e4f-5a6b7c8d9e0f","source":"gateway","chunkOrder":4,"sourceProgress":55,"batchIndex":2,"batchesInChunk":3,"chats":[{"phone":"5511999998888","lid":null,"profileName":"Maria","messages":[{"providerMessageId":"3EB0A1","text":{"body":"Olá"},"timestamp":"1739230955","type":"text"}]}]}
```

| Campo | Significado |
|---|---|
| `kind` | `batch` |
| `source` | `gateway` |
| `chunkOrder`, `sourceProgress` | ordem e percentual (0 a 100) do pedaço, como o `HistorySync` do whatsmeow informa. **`chunkOrder` pode repetir entre tipos de sincronização** (`INITIAL_BOOTSTRAP`, `RECENT`, `FULL` numeram cada um os seus pedaços): nenhum consumidor pode usá-lo, nem `(importId, chunkOrder, batchIndex)`, como chave de lote; o backend mantém o próprio livro de lotes |
| `batchIndex`, `batchesInChunk` | fatia do pedaço, **começando em 1**: `batchIndex` vai de 1 a `batchesInChunk` |
| `chats[].phone` | sempre presente; chat sem telefone resolvível nunca é enviado |
| `chats[].lid`, `chats[].profileName` | `null` quando desconhecidos; `profileName` é o nome da primeira mensagem recebida do chat |
| `chats[].messages[]` | o `InboundEvent` de `whatsapp.inbound.v1` **sem** `phoneNumberId`, `from`, `senderPn`, `senderLid` e `profileName` |

O último lote de um pedaço de três e o lote único de um pedaço pequeno (os mesmos literais do
teste de contrato `internal/history/contract_internal_test.go`):

```json
{"kind":"batch","tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","importId":"3c5d7e9f-1a2b-4c3d-8e4f-5a6b7c8d9e0f","source":"gateway","chunkOrder":4,"sourceProgress":55,"batchIndex":3,"batchesInChunk":3,"chats":[{"phone":"5511999998888","lid":null,"profileName":"Maria","messages":[{"providerMessageId":"3EB0A1","text":{"body":"Olá"},"timestamp":"1739230955","type":"text"}]}]}
```

```json
{"kind":"batch","tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","importId":"3c5d7e9f-1a2b-4c3d-8e4f-5a6b7c8d9e0f","source":"gateway","chunkOrder":4,"sourceProgress":55,"batchIndex":1,"batchesInChunk":1,"chats":[{"phone":"5511999998888","lid":null,"profileName":"Maria","messages":[{"providerMessageId":"3EB0A1","text":{"body":"Olá"},"timestamp":"1739230955","type":"text"}]}]}
```

Cada mensagem tem exatamente o formato do ao vivo (texto em `text.body`, `fromMe` omitido quando
falso), com as chaves em ordem alfabética. O backend reconstrói o evento com `phoneNumberId = channelId`
e `from`/`senderPn`/`senderLid`/`profileName` do chat.

Limites: até 500 mensagens e cerca de 1 MB por lote; um chat que atravessa lotes repete o
cabeçalho. Uma mensagem que sozinha não cabe num lote é descartada antes da divisão (log `warn`
com o `providerMessageId`, nunca o conteúdo): nunca vira lote e nunca conta em `totalBatches`. Só os últimos 90 dias. Nunca: grupos, status, listas de transmissão, newsletters,
reações, edições, revogações e votos de enquete, mensagens de sistema sem conteúdo.

### Mídia

A mídia é baixada e gravada no S3 como no ao vivo (`inbound-media/<tenantId>/<providerMessageId>`)
antes de o lote ser publicado, com concorrência limitada e prazo por mídia e por pedaço. Mídia
que não baixou (saiu da CDN, prazo, S3 fora) vem com `media` **sem `key`**, mantendo
`mimeType`, `caption` e `filename`: o backend grava `mediaDownloadFailedAt`.

## Fim

```json
{"kind":"done","tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","importId":"3c5d7e9f-1a2b-4c3d-8e4f-5a6b7c8d9e0f","source":"gateway","totalBatches":9,"skippedChats":2}
```

| Campo | Significado |
|---|---|
| `totalBatches` | o contador persistido no registro no momento do fim (cada lote publicado conta uma vez, inclusive os de um pedaço reenviado pelo telefone) |
| `skippedChats` | quantos chats privados o gateway deixou de fora por não conseguir resolver um telefone (chats só com LID). Cada chat conta uma vez na importação, mesmo que apareça em vários pedaços. Grupos ficam de fora por definição e **não** contam. `0` no fim vazio de um canal já pareado |

Publicado uma única vez, depois do último lote do pedaço com `sourceProgress` 100, quando todos
os lotes da importação até ali tiveram confirmação. O backend fecha a importação quando os lotes
processados alcançam `totalBatches`, em qualquer ordem, e continua importando lote que chegue
depois do fim.

### Lotes depois do fim

O telefone pode mandar pedaços depois do que chegou a 100 (um `FULL` depois do
`INITIAL_BOOTSTRAP`, por exemplo). Eles continuam traduzidos e publicados com o mesmo
`importId`, contam no registro, mas **não** entram no `totalBatches` do fim já enviado, e nenhum
segundo fim é publicado. O backend fecha a importação pelo `totalBatches` e importa os lotes
tardios assim mesmo.

### Falha do registro

Se o registro não confirmar a contagem de um lote já publicado (ou a gravação dos chats
ignorados, ou a marcação do fim), o gateway tenta de novo com espera crescente, até 5 vezes. Se
ainda falhar, registra um `error` e segue publicando os lotes restantes e o fim, sem perder
histórico: o `totalBatches` pode ficar diferente do que o backend recebeu, e o prazo de 24 horas
do backend resolve essa diferença. Se o que falhou foi a marcação do fim, um pedaço seguinte
com `sourceProgress` 100 publica outro fim, com o mesmo `importId` e o total daquele momento.

## Persistência e ciclo de vida

Tabela `gateway_history_imports` (`channel_id` PK, `tenant_id`, `import_id`, `batches`,
`skipped_chats`, `started_at`, `finished_at`, `touched_at`), criada no boot; colunas novas entram
por `ADD COLUMN IF NOT EXISTS`, e uma importação em curso antes da atualização continua. Um novo
pareamento com importação substitui a anterior e zera os contadores.

O fim **não** apaga a linha: marca `finished_at`, para que os pedaços que chegam depois dele
ainda sejam publicados. A linha deixa de valer:

- num novo pareamento com importação (substituída) ou sem importação (apagada);
- no logout do canal (apagada com a sessão);
- depois de 30 minutos sem lote publicado desde o fim (`registry.HistoryQuietPeriod`, medido
  por `touched_at`, que avança no início, a cada lote contado, a cada chat ignorado gravado e no
  fim). A verificação é feita no próprio SQL, na chegada de cada pedaço e na retomada do canal,
  sem timer em memória: a linha vencida fica inerte (não é lida como importação, não faz a
  retomada baixar histórico) até ser substituída ou apagada. Uma importação que ainda não chegou
  ao fim nunca vence por silêncio: quem a encerra é o prazo de 24 horas do backend.

Só o cliente do canal com importação válida baixa o histórico por conta própria
(`ManualHistorySyncDownload`, ligado antes do `Connect` no pareamento que importa e na retomada
de um canal com importação válida). Todo outro canal fica exatamente como antes: o whatsmeow
baixa, grava e apaga cada pedaço sozinho, e nada é publicado. Um pedaço que chegue a um cliente
em modo manual sem importação válida (vencida ou apagada) é baixado (mapeamentos LID, nomes e
segredos gravados, como no automático) e liberado sem publicar.

Cada pedaço só é apagado do servidor do WhatsApp depois de todos os seus lotes confirmados. Se
o gateway parar no meio, o resto daquele pedaço não volta (o WhatsApp não reenvia): o prazo de
24 horas do backend encerra a importação como falha.
