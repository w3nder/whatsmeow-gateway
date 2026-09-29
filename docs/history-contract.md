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

A importação é gravada no registro **antes** do QR. Um segundo pedido com importação para o
mesmo canal substitui o `importId`: daí em diante nada (lote ou fim) é publicado para o
anterior. Canal que já estava pareado (conecta sem QR) não recebe histórico: o gateway publica
o fim com `totalBatches: 0` na hora.

O pareamento que importa usa os `DeviceProps` de histórico do primeiro `Connect` até o fim do
pareamento (sucesso, erro ou fim do QR) e é exclusivo: nenhum outro pareamento começa enquanto
ele mostra o QR, e ele espera os que estão em curso. Pareamentos sem histórico continuam
simultâneos entre si, com os props de sempre.

## Onde publicar e onde ouvir

| | Exchange | Routing key |
|---|---|---|
| Lote e fim | `sender.events` | `whatsapp.history.v1` |

Publicação persistente e com confirmação do broker, um lote por vez por canal, com intervalo
entre lotes. O backend declara a fila `ingestion.history` (DLX, DLQ e retry).

## Lote

```json
{"kind":"batch","tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","importId":"3c5d7e9f-1a2b-4c3d-8e4f-5a6b7c8d9e0f","source":"gateway","chunkOrder":4,"sourceProgress":55,"batchIndex":2,"batchesInChunk":3,"chats":[{"phone":"5511999998888","lid":null,"profileName":"Maria","messages":[{"providerMessageId":"3EB0A1","text":{"body":"Olá"},"timestamp":"1739230955","type":"text"}]}]}
```

| Campo | Significado |
|---|---|
| `kind` | `batch` |
| `source` | `gateway` |
| `chunkOrder`, `sourceProgress` | ordem e percentual (0 a 100) do pedaço, como o `HistorySync` do whatsmeow informa. **`chunkOrder` pode repetir entre tipos de sincronização** (`INITIAL_BOOTSTRAP`, `RECENT`, `FULL` numeram cada um os seus pedaços): nenhum consumidor pode usá-lo, nem `(importId, chunkOrder, batchIndex)`, como chave de lote; o backend mantém o próprio livro de lotes |
| `batchIndex`, `batchesInChunk` | fatia do pedaço, de 1 a `batchesInChunk` |
| `chats[].phone` | sempre presente; chat sem telefone resolvível nunca é enviado |
| `chats[].lid`, `chats[].profileName` | `null` quando desconhecidos; `profileName` é o nome da primeira mensagem recebida do chat |
| `chats[].messages[]` | o `InboundEvent` de `whatsapp.inbound.v1` **sem** `phoneNumberId`, `from`, `senderPn`, `senderLid` e `profileName` |

Cada mensagem tem exatamente o formato do ao vivo (texto em `text.body`, `fromMe` omitido quando
falso), com as chaves em ordem alfabética. O backend reconstrói o evento com `phoneNumberId = channelId`
e `from`/`senderPn`/`senderLid`/`profileName` do chat.

Limites: até 500 mensagens e cerca de 1 MB por lote; um chat que atravessa lotes repete o
cabeçalho. Só os últimos 90 dias. Nunca: grupos, status, listas de transmissão, newsletters,
reações, edições, revogações e votos de enquete, mensagens de sistema sem conteúdo.

### Mídia

A mídia é baixada e gravada no S3 como no ao vivo (`inbound-media/<tenantId>/<providerMessageId>`)
antes de o lote ser publicado, com concorrência limitada e prazo por mídia e por pedaço. Mídia
que não baixou (saiu da CDN, prazo, S3 fora) vem com `media` **sem `key`**, mantendo
`mimeType`, `caption` e `filename`: o backend grava `mediaDownloadFailedAt`.

## Fim

```json
{"kind":"done","tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","importId":"3c5d7e9f-1a2b-4c3d-8e4f-5a6b7c8d9e0f","source":"gateway","totalBatches":9}
```

Publicado depois do último lote do pedaço com `sourceProgress` 100, quando todos os lotes da
importação tiveram confirmação. `totalBatches` é o contador persistido no registro (cada lote
publicado conta uma vez, inclusive os de um pedaço reenviado pelo telefone). O backend fecha a
importação quando os lotes processados alcançam `totalBatches`, em qualquer ordem, e continua
importando lote que chegue depois do fim.

## Persistência e ciclo de vida

Tabela `gateway_history_imports` (`channel_id` PK, `tenant_id`, `import_id`, `batches`,
`started_at`), criada no boot. Um novo pareamento com importação substitui a anterior e zera o
contador; o fim apaga a linha; o logout do canal também.

Só o cliente do canal com importação ativa baixa o histórico por conta própria
(`ManualHistorySyncDownload`, ligado antes do `Connect` no pareamento que importa e na retomada
de um canal com importação ativa). Todo outro canal fica exatamente como antes: o whatsmeow
baixa, grava e apaga cada pedaço sozinho, e nada é publicado. Depois do fim, o cliente daquele
canal continua em modo manual até a próxima retomada; um pedaço que chegue nesse intervalo é
baixado (mapeamentos LID, nomes e segredos gravados, como no automático) e liberado sem
publicar.

Cada pedaço só é apagado do servidor do WhatsApp depois de todos os seus lotes confirmados. Se
o gateway parar no meio, o resto daquele pedaço não volta (o WhatsApp não reenvia): o prazo de
24 horas do backend encerra a importação como falha.
