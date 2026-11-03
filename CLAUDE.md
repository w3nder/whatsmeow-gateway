# CLAUDE.md

Regras do workspace em `../CLAUDE.md`; este arquivo manda dentro do `whatsmeow-gateway`.

## O que é

Gateway Go (`go 1.26`, módulo `github.com/w3nder/whatsmeow-gateway`) que liga o WhatsApp (whatsmeow, canal lite) ao `sender-go`. Mantém as sessões, publica mensagens recebidas no RabbitMQ e consome comandos de envio, grupos, histórico, configurações e chamadas de voz/vídeo (`meowcaller`). Sessões em Postgres (`SESSION_DATABASE_URL`), Redis, mídia em S3/MinIO.

## Comandos

```bash
make build     # go build -o bin/gateway ./cmd/gateway
make vet       # go vet ./...
make lint      # golangci-lint run (errcheck, govet, staticcheck, unused; gofmt)
make test      # go test -timeout 30m ./...
go test ./internal/mapper/...                          # um pacote
go test ./test -run TestGatewayEndToEnd -timeout 30m   # um teste
```

Os testes de `test/` usam testcontainers (Postgres, RabbitMQ, MinIO reais): exigem Docker/OrbStack ativo e são lentos.

## Portões antes do PR

O CI (`.github/workflows/ci.yml`, runner arm64) roda `go build ./...`, `go vet ./...` e `go test -timeout 30m ./...`. Rode também `make lint`, que o CI não cobre.

## Estrutura

- `cmd/gateway`: `main`.
- `internal/config`: lê e valida o env. Obrigatórias: `GATEWAY_INSTANCE_ID`, `AMQP_URL`, `SESSION_DATABASE_URL`, `REDIS_URL`. Opcionais: `S3_*`, `CALL_MEDIA_*` (WebSocket de mídia de chamadas, 8081 local), `GATEWAY_CALL_RECORD`, `GATEWAY_CALL_TMPDIR`, `GROUP_PREFETCH`, `GATEWAY_DEBUG_RAW`.
- `internal/amqp`: topologia, publicação e consumo.
- `internal/session`, `registry`, `ownership`, `store`: sessões, dono de cada canal, persistência.
- `internal/mapper`: evento whatsmeow para o contrato de mensagem do Sender.
- `internal/call`, `groups`, `groupinfo`, `history`, `channelsettings`, `media`, `avatar`, `dedupe`, `senderid`, `deviceprops`: um assunto por pacote.
- `test/`: integração/E2E com contêineres reais e um cliente WhatsApp falso (`fake_test.go`).
- `docs/`: contratos (abaixo) e `superpowers/` (histórico de design). `Dockerfile` e `docker-image.yml` publicam a imagem.

## Contratos

Gateway e `sender-go` só se falam por RabbitMQ; os contratos em `docs/` são a fonte de verdade:

- `call-contract.md` (comandos/eventos de chamada e mídia ao vivo por WebSocket)
- `group-contract.md`, `history-contract.md`, `send-failure-contract.md`, `settings-contract.md`

O lado Node está em `sender-go/packages/worker-core`. Mudou exchange, fila, routing key ou payload? Atualize o `.md` e o `worker-core` no mesmo ciclo (agente `contratos`). Nunca replique lógica do gateway no Node.

## Armadilhas

- `gateway.call` é fila separada de `gateway.send` de propósito: chamada é longa e travaria o prefetch de envio. Não una.
- Áudio e vídeo nunca passam pelo broker; só JSON com o ciclo de vida e a `key` da gravação no S3.
- Consumidores precisam ser idempotentes (`internal/dedupe`); reentrega não pode duplicar efeito.
- `.env` local não é commitado; não exponha segredos em log nem em relatório.

## Git

Sem `develop`: `feature/*` ou `fix/*` a partir de `origin/main`, PR para `main`. Nunca push direto em `main`. Commit conventional com escopo, sem trailer do Claude.
