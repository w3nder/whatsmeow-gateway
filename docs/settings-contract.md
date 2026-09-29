# Contrato de opções do canal — comando

O backend diz ao gateway como o canal deve se comportar: se ouve grupos, se recebe
chamadas e, quando não recebe, que texto responder a quem ligou. É um **comando**
assíncrono; o gateway guarda o resultado no registro (`gateway_channel_sessions`) e num
mapa em memória, e o carrega de novo ao retomar cada canal.

```
backend ──outbox──▶ RabbitMQ (whatsapp.gateway.settings) ──▶ gateway ──▶ registro + mapa em memória
```

## Onde publicar e onde ouvir

| | Exchange | Routing key |
|---|---|---|
| Comando | `whatsapp.gateway.settings` | qualquer (a fila usa `#`; o backend manda o shard do canal, como nos outros comandos) |

A fila é `gateway.settings` (quorum, DLX `gateway.settings.dlx`, DLQ `gateway.settings.dlq`),
consumida em série por `whatsmeow-gateway.settings`. Comando malformado, sem `tenantId` ou
`channelId`, ou de um cliente diferente do dono do canal, vai para a DLQ.

## Comando

```json
{"tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","listenGroups":true,"receiveCalls":false,"callRejectMessage":"Não atendemos ligações, escreva aqui.","settingsVersion":3}
```

| Campo | Tipo | Efeito |
|---|---|---|
| `tenantId` | string | obrigatório; precisa ser o dono do canal |
| `channelId` | string | obrigatório |
| `listenGroups` | bool | `false`: o canal descarta mensagem, recibo e evento de participantes de qualquer grupo (`@g.us`) antes de mapear; comandos de saída para grupo continuam funcionando. Ausente: `true` |
| `receiveCalls` | bool | `false`: a chamada é recusada no ato, sem tocar para o operador. Ausente: `true` |
| `callRejectMessage` | string ou `null` | texto (sem espaços nas pontas) enviado a quem ligou, só com `receiveCalls: false`. `null`, ausente ou vazio: nenhuma mensagem |
| `settingsVersion` | inteiro | último campo do JSON; contador crescente por canal. O gateway descarta (com ack) o comando de versão menor que a vigente e aplica versão igual ou maior. Ausente: `0` |

O comando não carrega segredo e é **idempotente**: aplicar duas vezes dá o mesmo estado.
O estado vale para o canal inteiro (vence a maior `settingsVersion`; versão igual reaplica). O backend o
reenvia sempre que o canal reporta `connected`, o que cobre o gateway reiniciado e o
comando perdido.

## Persistência

`gateway_channel_sessions` ganha `listen_groups boolean NOT NULL DEFAULT true`,
`receive_calls boolean NOT NULL DEFAULT true` `call_reject_message text` e `settings_version bigint NOT NULL DEFAULT 0`, criadas com
`ALTER TABLE … ADD COLUMN IF NOT EXISTS` na abertura do registro. Canal sem linha (ainda
não pareado) fica só no mapa em memória até o backend reenviar o comando depois do
`connected`. Canal sem colunas ou sem registro vale o padrão: ouve grupos, recebe
chamadas, sem mensagem.
