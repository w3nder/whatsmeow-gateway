# Grupos no gateway — plano de implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** o gateway passa a criar grupos, entregar o link de convite, aplicar ações em massa (fechar, abrir, renomear, descrição, foto, remover participante) e publicar entrada/saída de participantes, para o gerenciador de grupos do `sender-vectax`.

**Architecture:** um servidor RPC pela fila (`rpc.gateway.<operação>`, mesmo protocolo do `RpcServer` Node: `replyTo` + `correlationId`, resposta `{ok, result | error}`) para o que precisa de resposta; um consumidor de comando (`whatsapp.gateway.group.v1` → fila `gateway.group`) para as ações em massa, com um evento de resultado por grupo; e um mapeador de `events.GroupInfo` que publica `whatsapp.group.participants.v1`. A lógica de grupo fica em `internal/groups` sobre uma interface estreita `GroupClient`, testável com dublê local; o transporte fica em `internal/amqp`; a cola fica em `internal/gateway`.

**Tech Stack:** Go 1.26, whatsmeow (fixado no `go.mod`), amqp091-go, pgx, testcontainers (RabbitMQ, Postgres, Redis). Testes de integração em `test/` sobem broker real, como os existentes.

**Spec:** `sender-vectax/docs/superpowers/specs/2026-09-24-gerenciador-de-grupos-design.md` (seções 3 e 11).

## Global Constraints

- Sem comentários no código, nunca. Nomes e estrutura explicam.
- Sem mock de infraestrutura: RabbitMQ, Postgres e Redis reais via testcontainers (`startRabbitMQ`, `startPostgresForGateway`, `startRedis` em `test/`). O único dublê é o `WAClient` (o WhatsApp é o terceiro).
- Zero código morto; só o que estas tarefas usam.
- Um único lugar para cada verdade: constantes de fila/exchange em `internal/amqp/topology.go`, contratos em `internal/amqp/contracts.go`.
- Códigos de erro do RPC são exatamente os do `worker-core` (`RPC_ERROR_CODES`): `invalid_request`, `not_found`, `unavailable`, `bad_gateway`, `internal`.
- Fila RPC: `rpc.gateway.<operação>`, quorum, durável. Resposta na fila `replyTo` com `correlationId`, `persistent=false`, `contentType=application/json`, e `ack` **depois** de publicar a resposta.
- Commit por tarefa, mensagem em português, sem trailer de atribuição.
- `go build ./... && go vet ./...` limpos antes de cada commit. Testes rodam **um arquivo por vez**: `go test ./test -run 'Nome' -count=1`.
- Branch: `feat/grupos` (já criada a partir de `origin/main`).

## Review Focus

1. Um `events.GroupInfo` com `Join` e `Leave` ao mesmo tempo (raro, mas o WhatsApp manda) deve gerar **dois** eventos, não um. Teste na Tarefa 7.
2. Um comando em massa reentregue depois de o gateway cair no meio: os grupos já concluídos recebem só a republicação do resultado, os pendentes são aplicados de novo. Teste na Tarefa 9.
3. Um pedido RPC cuja `expiration` já venceu quando o gateway o pega não deve executar nada no WhatsApp; o cliente já desistiu. Teste na Tarefa 3.
4. `group.create` com a sessão pareada mas o socket caído deve responder `unavailable` e não `not_found`, porque o backend usa a diferença para decidir entre retentar e falhar. Teste na Tarefa 8.
5. Foto em PNG deve virar JPEG antes de `SetGroupPhoto`; PNG cru é recusado com `ErrInvalidImageFormat`. Teste na Tarefa 5.

---

### Tarefa 1: Contratos e topologia

**Files:**
- Modify: `internal/amqp/contracts.go`
- Modify: `internal/amqp/topology.go`
- Test: `internal/amqp/group_contract_test.go`

**Interfaces:**
- Produces: `GatewayGroupCommand`, `GroupActionEvent`, `GroupParticipantsEvent`, `GroupParticipant`, e as constantes `GatewayGroupExchange/Queue/DLX/DLQ/Consumer`, `GroupActionRoutingKey`, `GroupParticipantsRoutingKey`, `RpcQueuePrefix`.

- [ ] **Passo 1: escrever o teste de contrato (falha ao compilar)**

```go
package amqp_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

const groupCommandLiteral = `{"commandId":"01J9ZK3S3Y2Q0N4R8T6V1W5X7Z","tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","action":"remove_participants","groupJids":["120363422547615282@g.us","120363422547615283@g.us"],"params":{"phones":["5511999887766"]}}`

const groupActionEventLiteral = `{"tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","commandId":"01J9ZK3S3Y2Q0N4R8T6V1W5X7Z","groupJid":"120363422547615282@g.us","action":"remove_participants","ok":true,"removed":1}`

const groupParticipantsEventLiteral = `{"tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","groupJid":"120363422547615282@g.us","type":"join","participants":[{"jid":"2002125877314@lid","lid":"2002125877314@lid","phone":"5511999887766"}],"eventId":"3f1c2a9b8d7e6f5a4b3c2d1e0f9a8b7c","occurredAt":"2026-09-24T12:00:00Z"}`

func TestGroupCommandContractLiteral(t *testing.T) {
	var cmd amqp.GatewayGroupCommand
	if err := json.Unmarshal([]byte(groupCommandLiteral), &cmd); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	expected := amqp.GatewayGroupCommand{
		CommandID: "01J9ZK3S3Y2Q0N4R8T6V1W5X7Z",
		TenantID:  "1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c",
		ChannelID: "2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d",
		Action:    "remove_participants",
		GroupJIDs: []string{"120363422547615282@g.us", "120363422547615283@g.us"},
		Params:    amqp.GroupActionParams{Phones: []string{"5511999887766"}},
	}
	if !reflect.DeepEqual(cmd, expected) {
		t.Fatalf("decoded %+v, want %+v", cmd, expected)
	}
}

func TestGroupActionEventContractLiteral(t *testing.T) {
	removed := 1
	evt := amqp.GroupActionEvent{
		TenantID:  "1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c",
		ChannelID: "2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d",
		CommandID: "01J9ZK3S3Y2Q0N4R8T6V1W5X7Z",
		GroupJID:  "120363422547615282@g.us",
		Action:    "remove_participants",
		OK:        true,
		Removed:   &removed,
	}
	body, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(body) != groupActionEventLiteral {
		t.Fatalf("wire shape\n got %s\nwant %s", body, groupActionEventLiteral)
	}
}

func TestGroupParticipantsEventContractLiteral(t *testing.T) {
	evt := amqp.GroupParticipantsEvent{
		TenantID:  "1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c",
		ChannelID: "2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d",
		GroupJID:  "120363422547615282@g.us",
		Type:      "join",
		Participants: []amqp.GroupParticipant{{
			JID:   "2002125877314@lid",
			LID:   "2002125877314@lid",
			Phone: "5511999887766",
		}},
		EventID:    "3f1c2a9b8d7e6f5a4b3c2d1e0f9a8b7c",
		OccurredAt: "2026-09-24T12:00:00Z",
	}
	body, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(body) != groupParticipantsEventLiteral {
		t.Fatalf("wire shape\n got %s\nwant %s", body, groupParticipantsEventLiteral)
	}
}

func TestGroupTopologyNames(t *testing.T) {
	if amqp.GatewayGroupExchange != "whatsapp.gateway.group.v1" || amqp.GatewayGroupQueue != "gateway.group" {
		t.Fatalf("group command topology: %s / %s", amqp.GatewayGroupExchange, amqp.GatewayGroupQueue)
	}
	if amqp.GroupActionRoutingKey != "whatsapp.group.action.v1" || amqp.GroupParticipantsRoutingKey != "whatsapp.group.participants.v1" {
		t.Fatalf("group event routing keys: %s / %s", amqp.GroupActionRoutingKey, amqp.GroupParticipantsRoutingKey)
	}
	if amqp.RpcQueueName("group.create") != "rpc.gateway.group.create" {
		t.Fatalf("rpc queue name: %s", amqp.RpcQueueName("group.create"))
	}
}
```

- [ ] **Passo 2: rodar e ver falhar**

Run: `cd /Users/wenderteixeira/whatsmeow-gateway && go test ./internal/amqp -run 'TestGroup' -count=1`
Expected: erro de compilação (`undefined: amqp.GatewayGroupCommand`).

- [ ] **Passo 3: contratos**

Acrescentar ao fim de `internal/amqp/contracts.go`:

```go
type GroupActionParams struct {
	Phones      []string `json:"phones,omitempty"`
	Name        string   `json:"name,omitempty"`
	Description string   `json:"description,omitempty"`
	PhotoURL    string   `json:"photoUrl,omitempty"`
}

type GatewayGroupCommand struct {
	CommandID string            `json:"commandId"`
	TenantID  string            `json:"tenantId"`
	ChannelID string            `json:"channelId"`
	Action    string            `json:"action"`
	GroupJIDs []string          `json:"groupJids"`
	Params    GroupActionParams `json:"params"`
}

type GroupActionEvent struct {
	TenantID  string `json:"tenantId"`
	ChannelID string `json:"channelId"`
	CommandID string `json:"commandId"`
	GroupJID  string `json:"groupJid"`
	Action    string `json:"action"`
	OK        bool   `json:"ok"`
	Error     string `json:"error,omitempty"`
	Removed   *int   `json:"removed,omitempty"`
}

type GroupParticipant struct {
	JID   string `json:"jid"`
	LID   string `json:"lid,omitempty"`
	Phone string `json:"phone,omitempty"`
}

type GroupParticipantsEvent struct {
	TenantID     string             `json:"tenantId"`
	ChannelID    string             `json:"channelId"`
	GroupJID     string             `json:"groupJid"`
	Type         string             `json:"type"`
	Participants []GroupParticipant `json:"participants"`
	EventID      string             `json:"eventId"`
	OccurredAt   string             `json:"occurredAt"`
}
```

- [ ] **Passo 4: topologia**

Em `internal/amqp/topology.go`, acrescentar às constantes de eventos:

```go
	GroupActionRoutingKey       = "whatsapp.group.action.v1"
	GroupParticipantsRoutingKey = "whatsapp.group.participants.v1"
```

e ao bloco de comandos:

```go
	GatewayGroupExchange = "whatsapp.gateway.group.v1"
	GatewayGroupQueue    = "gateway.group"
	GatewayGroupDLX      = "gateway.group.dlx"
	GatewayGroupDLQ      = "gateway.group.dlq"
	GatewayGroupConsumer = "whatsmeow-gateway.group"
```

e, abaixo das constantes:

```go
const RpcQueuePrefix = "rpc.gateway."

func RpcQueueName(operation string) string {
	return RpcQueuePrefix + operation
}
```

- [ ] **Passo 5: rodar e ver passar**

Run: `go test ./internal/amqp -run 'TestGroup' -count=1`
Expected: `ok`.

- [ ] **Passo 6: commit**

```bash
git add internal/amqp/contracts.go internal/amqp/topology.go internal/amqp/group_contract_test.go
git commit -m "feat(grupos): contratos e topologia dos comandos e eventos de grupo"
```

---

### Tarefa 2: Publisher e consumidor do comando de grupo

**Files:**
- Modify: `internal/amqp/publisher.go`
- Modify: `internal/amqp/consumer.go`
- Test: `test/amqp_group_test.go`

**Interfaces:**
- Consumes: constantes e tipos da Tarefa 1.
- Produces: `Publisher.PublishGroupAction(ctx, GroupActionEvent)`, `Publisher.PublishGroupParticipants(ctx, GroupParticipantsEvent)`, `type GroupHandler func(ctx, GatewayGroupCommand) error`, `Consumer.StartGroup(ctx, GroupHandler) error`.

- [ ] **Passo 1: teste de integração (broker real)**

```go
package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

func TestConsumerHandlesGatewayGroupCommand(t *testing.T) {
	conn := startRabbitMQ(t)

	consumer, err := gatewayamqp.NewConsumer(conn, gatewayamqp.ConsumerConfig{Prefetch: 10})
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil {
			t.Errorf("consumer.Close failed: %v", err)
		}
	})

	received := make(chan gatewayamqp.GatewayGroupCommand, 1)
	if err := consumer.StartGroup(context.Background(), func(_ context.Context, cmd gatewayamqp.GatewayGroupCommand) error {
		received <- cmd
		return nil
	}); err != nil {
		t.Fatalf("StartGroup failed: %v", err)
	}

	publishCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("open publish channel: %v", err)
	}
	t.Cleanup(func() { _ = publishCh.Close() })

	cmd := gatewayamqp.GatewayGroupCommand{
		CommandID: "cmd-1",
		TenantID:  "tenant-1",
		ChannelID: "channel-1",
		Action:    "lock",
		GroupJIDs: []string{"120363422547615282@g.us"},
	}
	body, _ := json.Marshal(cmd)
	if err := publishCh.PublishWithContext(context.Background(), gatewayamqp.GatewayGroupExchange, "17", false, false, rabbitmq.Publishing{
		ContentType: "application/json", DeliveryMode: rabbitmq.Persistent, Body: body,
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case got := <-received:
		if got.CommandID != "cmd-1" || got.Action != "lock" || len(got.GroupJIDs) != 1 {
			t.Fatalf("handler got %+v", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the group command")
	}
}

func TestPublisherPublishesGroupEvents(t *testing.T) {
	conn := startRabbitMQ(t)

	publisher, err := gatewayamqp.NewPublisher(conn)
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	probeCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("open probe channel: %v", err)
	}
	t.Cleanup(func() { _ = probeCh.Close() })
	q, err := probeCh.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("declare probe queue: %v", err)
	}
	for _, rk := range []string{gatewayamqp.GroupActionRoutingKey, gatewayamqp.GroupParticipantsRoutingKey} {
		if err := probeCh.QueueBind(q.Name, rk, gatewayamqp.EventsExchange, false, nil); err != nil {
			t.Fatalf("bind %s: %v", rk, err)
		}
	}
	deliveries, err := probeCh.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume probe: %v", err)
	}

	ctx := context.Background()
	if err := publisher.PublishGroupAction(ctx, gatewayamqp.GroupActionEvent{TenantID: "t", ChannelID: "c", CommandID: "cmd-1", GroupJID: "g@g.us", Action: "lock", OK: true}); err != nil {
		t.Fatalf("PublishGroupAction: %v", err)
	}
	if err := publisher.PublishGroupParticipants(ctx, gatewayamqp.GroupParticipantsEvent{TenantID: "t", ChannelID: "c", GroupJID: "g@g.us", Type: "join", EventID: "e1", OccurredAt: "2026-09-24T12:00:00Z"}); err != nil {
		t.Fatalf("PublishGroupParticipants: %v", err)
	}

	action := waitForDelivery(t, deliveries, gatewayamqp.GroupActionRoutingKey, 10*time.Second)
	var got gatewayamqp.GroupActionEvent
	if err := json.Unmarshal(action.Body, &got); err != nil || got.CommandID != "cmd-1" || !got.OK {
		t.Fatalf("group action on the wire: %s (%v)", action.Body, err)
	}
	participants := waitForDelivery(t, deliveries, gatewayamqp.GroupParticipantsRoutingKey, 10*time.Second)
	var gotP gatewayamqp.GroupParticipantsEvent
	if err := json.Unmarshal(participants.Body, &gotP); err != nil || gotP.EventID != "e1" || gotP.Participants == nil {
		t.Fatalf("participants on the wire: %s (%v)", participants.Body, err)
	}
}
```

`waitForDelivery` já existe em `test/gateway_e2e_test.go`. Note o `Participants == nil` no fim: o evento deve ir com `"participants":[]`, nunca `null` — o publisher garante isso (passo 3).

- [ ] **Passo 2: rodar e ver falhar**

Run: `go test ./test -run 'TestConsumerHandlesGatewayGroupCommand|TestPublisherPublishesGroupEvents' -count=1`
Expected: erro de compilação (`StartGroup`, `PublishGroupAction` indefinidos).

- [ ] **Passo 3: publisher**

Em `internal/amqp/publisher.go`, acrescentar:

```go
func (p *Publisher) PublishGroupAction(ctx context.Context, evt GroupActionEvent) error {
	return p.publish(ctx, GroupActionRoutingKey, evt)
}

func (p *Publisher) PublishGroupParticipants(ctx context.Context, evt GroupParticipantsEvent) error {
	if evt.Participants == nil {
		evt.Participants = []GroupParticipant{}
	}
	return p.publish(ctx, GroupParticipantsRoutingKey, evt)
}
```

- [ ] **Passo 4: consumidor**

Em `internal/amqp/consumer.go`: acrescentar `type GroupHandler func(ctx context.Context, cmd GatewayGroupCommand) error`; no struct `Consumer`, os campos `groupCh *rabbitmq.Channel` e `groupStarted bool`; em `NewConsumer`, depois do bloco do `callCh`, abrir `groupCh` com `declareCommandTopology(groupCh, GatewayGroupExchange, GatewayGroupQueue, GatewayGroupDLX, GatewayGroupDLQ)` e `Qos(cfg.Prefetch, 0, false)`, fechando os três anteriores em caso de erro, e incluir `groupCh: groupCh` no literal de retorno. Acrescentar:

```go
func (c *Consumer) StartGroup(ctx context.Context, handler GroupHandler) error {
	deliveries, err := c.groupCh.Consume(GatewayGroupQueue, GatewayGroupConsumer, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("amqp: consume %s: %w", GatewayGroupQueue, err)
	}
	c.groupStarted = true
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for d := range deliveries {
			var cmd GatewayGroupCommand
			handlerErr := json.Unmarshal(d.Body, &cmd)
			if handlerErr == nil {
				handlerErr = handler(ctx, cmd)
			}
			settle(d, handlerErr)
		}
		c.reportFailure(fmt.Errorf("amqp: %s consumer stopped: broker closed the delivery channel", GatewayGroupQueue))
	}()
	return nil
}
```

Em `Close()`, cancelar `GatewayGroupConsumer` quando `groupStarted` e fechar `groupCh`, no mesmo padrão dos outros três.

- [ ] **Passo 5: rodar e ver passar**

Run: `go test ./test -run 'TestConsumerHandlesGatewayGroupCommand|TestPublisherPublishesGroupEvents' -count=1`
Expected: `ok`. Depois `go test ./test -run 'TestConsumer|TestPublisher' -count=1` para garantir que os consumidores existentes continuam fechando limpo.

- [ ] **Passo 6: commit**

```bash
git add internal/amqp/publisher.go internal/amqp/consumer.go test/amqp_group_test.go
git commit -m "feat(grupos): fila gateway.group e eventos de ação e participantes"
```

---

### Tarefa 3: Servidor RPC pela fila

**Files:**
- Create: `internal/amqp/rpc.go`
- Test: `test/amqp_rpc_test.go`

**Interfaces:**
- Produces:
  - `type RpcError struct { Code, Message string }` com `Error()`; construtores `RpcNotFound(msg)`, `RpcUnavailable(msg)`, `RpcBadGateway(msg)`, `RpcInvalidRequest(msg)`.
  - `type RpcHandler func(ctx context.Context, payload json.RawMessage) (any, error)`
  - `NewRpcServer(conn *rabbitmq.Connection, prefetch int) *RpcServer`
  - `(*RpcServer) Handle(ctx context.Context, operation string, handler RpcHandler) error`
  - `(*RpcServer) Failed() <-chan error`, `(*RpcServer) Close() error`

- [ ] **Passo 1: teste de integração**

```go
package test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

type rpcProbe struct {
	ch         *rabbitmq.Channel
	replyQueue string
	replies    <-chan rabbitmq.Delivery
}

func newRpcProbe(t *testing.T, conn *rabbitmq.Connection) *rpcProbe {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open probe channel: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close() })
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("declare reply queue: %v", err)
	}
	replies, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume reply queue: %v", err)
	}
	return &rpcProbe{ch: ch, replyQueue: q.Name, replies: replies}
}

func (p *rpcProbe) call(t *testing.T, operation, correlationID string, payload string, timeout time.Duration) map[string]any {
	t.Helper()
	if _, err := p.ch.QueueDeclare(gatewayamqp.RpcQueueName(operation), true, false, false, false, rabbitmq.Table{"x-queue-type": "quorum"}); err != nil {
		t.Fatalf("declare rpc queue: %v", err)
	}
	if err := p.ch.PublishWithContext(context.Background(), "", gatewayamqp.RpcQueueName(operation), false, false, rabbitmq.Publishing{
		ContentType:   "application/json",
		ReplyTo:       p.replyQueue,
		CorrelationId: correlationID,
		Expiration:    strconv.FormatInt(timeout.Milliseconds(), 10),
		Body:          []byte(payload),
	}); err != nil {
		t.Fatalf("publish rpc request: %v", err)
	}
	select {
	case d := <-p.replies:
		if d.CorrelationId != correlationID {
			t.Fatalf("reply correlationId %q, want %q", d.CorrelationId, correlationID)
		}
		if d.ContentType != "application/json" || d.DeliveryMode == rabbitmq.Persistent {
			t.Fatalf("reply must be json and transient, got %q / %d", d.ContentType, d.DeliveryMode)
		}
		var reply map[string]any
		if err := json.Unmarshal(d.Body, &reply); err != nil {
			t.Fatalf("reply is not json: %s", d.Body)
		}
		return reply
	case <-time.After(timeout + 5*time.Second):
		t.Fatalf("no reply for %s", operation)
		return nil
	}
}

func TestRpcServerRoundTrip(t *testing.T) {
	conn := startRabbitMQ(t)
	server := gatewayamqp.NewRpcServer(conn, 4)
	t.Cleanup(func() { _ = server.Close() })

	err := server.Handle(context.Background(), "echo.test", func(_ context.Context, payload json.RawMessage) (any, error) {
		var in struct{ N int `json:"n"` }
		if err := json.Unmarshal(payload, &in); err != nil {
			return nil, err
		}
		return map[string]int{"doubled": in.N * 2}, nil
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	probe := newRpcProbe(t, conn)
	reply := probe.call(t, "echo.test", "corr-1", `{"n":21}`, 5*time.Second)
	if reply["ok"] != true || reply["result"].(map[string]any)["doubled"] != float64(42) {
		t.Fatalf("reply %v", reply)
	}
}

func TestRpcServerReportsDomainAndInternalErrors(t *testing.T) {
	conn := startRabbitMQ(t)
	server := gatewayamqp.NewRpcServer(conn, 4)
	t.Cleanup(func() { _ = server.Close() })

	_ = server.Handle(context.Background(), "fail.test", func(_ context.Context, payload json.RawMessage) (any, error) {
		var in struct{ Kind string `json:"kind"` }
		_ = json.Unmarshal(payload, &in)
		if in.Kind == "domain" {
			return nil, gatewayamqp.RpcNotFound("channel is not paired")
		}
		return nil, errBoom
	})

	probe := newRpcProbe(t, conn)
	domain := probe.call(t, "fail.test", "corr-d", `{"kind":"domain"}`, 5*time.Second)
	if domain["ok"] != false || domain["error"].(map[string]any)["code"] != "not_found" {
		t.Fatalf("domain reply %v", domain)
	}
	internal := probe.call(t, "fail.test", "corr-i", `{"kind":"other"}`, 5*time.Second)
	if internal["ok"] != false || internal["error"].(map[string]any)["code"] != "internal" {
		t.Fatalf("internal reply %v", internal)
	}
	invalid := probe.call(t, "fail.test", "corr-j", `{not json`, 5*time.Second)
	if invalid["ok"] != false || invalid["error"].(map[string]any)["code"] != "invalid_request" {
		t.Fatalf("invalid reply %v", invalid)
	}
}

func TestRpcServerSkipsRequestsThatAlreadyExpired(t *testing.T) {
	conn := startRabbitMQ(t)
	probe := newRpcProbe(t, conn)

	if _, err := probe.ch.QueueDeclare(gatewayamqp.RpcQueueName("late.test"), true, false, false, false, rabbitmq.Table{"x-queue-type": "quorum"}); err != nil {
		t.Fatalf("declare: %v", err)
	}
	if err := probe.ch.PublishWithContext(context.Background(), "", gatewayamqp.RpcQueueName("late.test"), false, false, rabbitmq.Publishing{
		ContentType: "application/json", ReplyTo: probe.replyQueue, CorrelationId: "corr-late", Expiration: "60000", Body: []byte(`{}`),
		Timestamp: time.Now().Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	handled := make(chan struct{}, 1)
	server := gatewayamqp.NewRpcServer(conn, 4)
	t.Cleanup(func() { _ = server.Close() })
	_ = server.Handle(context.Background(), "late.test", func(context.Context, json.RawMessage) (any, error) {
		handled <- struct{}{}
		return map[string]bool{"ran": true}, nil
	})

	select {
	case <-handled:
		t.Fatal("an expired request must not reach the handler")
	case d := <-probe.replies:
		t.Fatalf("an expired request must not be answered, got %s", d.Body)
	case <-time.After(3 * time.Second):
	}
}

var errBoom = errors.New("boom")
```

Adicionar `"errors"` aos imports. A expiração vencida é detectada por `Timestamp + Expiration < now` quando o `Timestamp` vem preenchido; o `RpcClient` do Node não manda `timestamp`, então no caminho real a proteção é a expiração do próprio broker (a mensagem morre na fila) — este teste pina o comportamento para quem mandar o carimbo.

- [ ] **Passo 2: rodar e ver falhar**

Run: `go test ./test -run 'TestRpcServer' -count=1`
Expected: erro de compilação.

- [ ] **Passo 3: implementar `internal/amqp/rpc.go`**

```go
package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"
)

const (
	RpcCodeInvalidRequest = "invalid_request"
	RpcCodeNotFound       = "not_found"
	RpcCodeUnavailable    = "unavailable"
	RpcCodeBadGateway     = "bad_gateway"
	RpcCodeInternal       = "internal"
)

type RpcError struct {
	Code    string
	Message string
}

func (e *RpcError) Error() string {
	return e.Code + ": " + e.Message
}

func RpcNotFound(message string) error       { return &RpcError{Code: RpcCodeNotFound, Message: message} }
func RpcUnavailable(message string) error    { return &RpcError{Code: RpcCodeUnavailable, Message: message} }
func RpcBadGateway(message string) error     { return &RpcError{Code: RpcCodeBadGateway, Message: message} }
func RpcInvalidRequest(message string) error { return &RpcError{Code: RpcCodeInvalidRequest, Message: message} }

type RpcHandler func(ctx context.Context, payload json.RawMessage) (any, error)

type rpcReply struct {
	OK     bool          `json:"ok"`
	Result any           `json:"result,omitempty"`
	Error  *rpcErrorBody `json:"error,omitempty"`
}

type rpcErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type RpcServer struct {
	conn     *rabbitmq.Connection
	prefetch int

	mu       sync.Mutex
	channels map[string]*rabbitmq.Channel

	closing atomic.Bool
	failed  chan error
	wg      sync.WaitGroup
}

func NewRpcServer(conn *rabbitmq.Connection, prefetch int) *RpcServer {
	return &RpcServer{conn: conn, prefetch: prefetch, channels: make(map[string]*rabbitmq.Channel), failed: make(chan error, 1)}
}

func (s *RpcServer) Failed() <-chan error {
	return s.failed
}

func (s *RpcServer) Handle(ctx context.Context, operation string, handler RpcHandler) error {
	ch, err := s.conn.Channel()
	if err != nil {
		return fmt.Errorf("amqp: open rpc channel for %s: %w", operation, err)
	}
	queue := RpcQueueName(operation)
	if _, err := ch.QueueDeclare(queue, true, false, false, false, rabbitmq.Table{"x-queue-type": "quorum"}); err != nil {
		_ = ch.Close()
		return fmt.Errorf("amqp: declare %s: %w", queue, err)
	}
	if err := ch.Qos(s.prefetch, 0, false); err != nil {
		_ = ch.Close()
		return fmt.Errorf("amqp: set qos on %s: %w", queue, err)
	}
	deliveries, err := ch.Consume(queue, "whatsmeow-gateway.rpc."+operation, false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return fmt.Errorf("amqp: consume %s: %w", queue, err)
	}

	s.mu.Lock()
	s.channels[operation] = ch
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		var inflight sync.WaitGroup
		defer inflight.Wait()
		for d := range deliveries {
			inflight.Add(1)
			go func(d rabbitmq.Delivery) {
				defer inflight.Done()
				s.serve(ctx, ch, d, handler)
			}(d)
		}
		s.reportFailure(fmt.Errorf("amqp: %s consumer stopped: broker closed the delivery channel", queue))
	}()
	return nil
}

func (s *RpcServer) serve(ctx context.Context, ch *rabbitmq.Channel, d rabbitmq.Delivery, handler RpcHandler) {
	if expired(d, time.Now()) {
		_ = d.Ack(false)
		return
	}
	callCtx, cancel := context.WithTimeout(ctx, requestTimeout(d))
	defer cancel()

	reply := answer(callCtx, d.Body, handler)
	if d.ReplyTo != "" {
		body, _ := json.Marshal(reply)
		if err := ch.PublishWithContext(ctx, "", d.ReplyTo, false, false, rabbitmq.Publishing{
			ContentType:   "application/json",
			CorrelationId: d.CorrelationId,
			DeliveryMode:  rabbitmq.Transient,
			Body:          body,
		}); err != nil {
			_ = d.Nack(false, false)
			return
		}
	}
	_ = d.Ack(false)
}

func answer(ctx context.Context, body []byte, handler RpcHandler) rpcReply {
	if !json.Valid(body) {
		return rpcReply{OK: false, Error: &rpcErrorBody{Code: RpcCodeInvalidRequest, Message: "request is not valid JSON"}}
	}
	result, err := handler(ctx, body)
	if err == nil {
		return rpcReply{OK: true, Result: result}
	}
	var rpcErr *RpcError
	if errors.As(err, &rpcErr) {
		return rpcReply{OK: false, Error: &rpcErrorBody{Code: rpcErr.Code, Message: rpcErr.Message}}
	}
	return rpcReply{OK: false, Error: &rpcErrorBody{Code: RpcCodeInternal, Message: err.Error()}}
}

const defaultRpcTimeout = 30 * time.Second

func requestTimeout(d rabbitmq.Delivery) time.Duration {
	ms, err := strconv.ParseInt(d.Expiration, 10, 64)
	if err != nil || ms <= 0 {
		return defaultRpcTimeout
	}
	return time.Duration(ms) * time.Millisecond
}

func expired(d rabbitmq.Delivery, now time.Time) bool {
	if d.Timestamp.IsZero() || d.Expiration == "" {
		return false
	}
	return now.After(d.Timestamp.Add(requestTimeout(d)))
}

func (s *RpcServer) reportFailure(err error) {
	if s.closing.Load() {
		return
	}
	select {
	case s.failed <- err:
	default:
	}
}

func (s *RpcServer) Close() error {
	s.closing.Store(true)
	s.mu.Lock()
	channels := s.channels
	s.channels = make(map[string]*rabbitmq.Channel)
	s.mu.Unlock()

	var errs []error
	for operation, ch := range channels {
		if err := ch.Cancel("whatsmeow-gateway.rpc."+operation, false); err != nil {
			errs = append(errs, fmt.Errorf("amqp: cancel rpc %s: %w", operation, err))
		}
	}
	s.wg.Wait()
	for operation, ch := range channels {
		if err := ch.Close(); err != nil {
			errs = append(errs, fmt.Errorf("amqp: close rpc channel %s: %w", operation, err))
		}
	}
	return errors.Join(errs...)
}
```

- [ ] **Passo 4: rodar e ver passar**

Run: `go test ./test -run 'TestRpcServer' -count=1`
Expected: `ok` (3 testes).

- [ ] **Passo 5: commit**

```bash
git add internal/amqp/rpc.go test/amqp_rpc_test.go
git commit -m "feat(grupos): servidor RPC pela fila rpc.gateway.<operacao>"
```

---

### Tarefa 4: `WAClient` expõe as operações de grupo

**Files:**
- Modify: `internal/session/client.go`
- Modify: `test/fake_test.go`
- Test: `test/session_client_test.go` (já existe; só ganha a asserção de interface se não houver)

**Interfaces:**
- Produces, na interface `WAClient`:
  - `CreateGroup(ctx, req whatsmeow.ReqCreateGroup) (*types.GroupInfo, error)`
  - `GetGroupInviteLink(ctx, jid types.JID, reset bool) (string, error)`
  - `SetGroupAnnounce(ctx, jid types.JID, announce bool) error`
  - `SetGroupName(ctx, jid types.JID, name string) error`
  - `SetGroupTopic(ctx, jid types.JID, topic string) error`
  - `SetGroupPhoto(ctx, jid types.JID, jpeg []byte) (string, error)`
  - `UpdateGroupParticipants(ctx, jid types.JID, participants []types.JID, change whatsmeow.ParticipantChange) ([]types.GroupParticipant, error)`
  - `GetJoinedGroups(ctx) ([]*types.GroupInfo, error)`
- Produces no dublê `fakeWAClient` (pacote `test`): campos `groups map[string]*types.GroupInfo`, `createdGroups []whatsmeow.ReqCreateGroup`, `announceCalls map[string]bool`, `nameCalls map[string]string`, `topicCalls map[string]string`, `photoCalls map[string][]byte`, `participantCalls []participantCall`, `inviteLinks map[string]string`, `groupErr error`, mais os métodos.

- [ ] **Passo 1: estender a interface e o adaptador**

Em `internal/session/client.go`, acrescentar à interface `WAClient`, logo após `GetGroupInfo`:

```go
	CreateGroup(ctx context.Context, req whatsmeow.ReqCreateGroup) (*types.GroupInfo, error)
	GetGroupInviteLink(ctx context.Context, jid types.JID, reset bool) (string, error)
	SetGroupAnnounce(ctx context.Context, jid types.JID, announce bool) error
	SetGroupName(ctx context.Context, jid types.JID, name string) error
	SetGroupTopic(ctx context.Context, jid types.JID, topic string) error
	SetGroupPhoto(ctx context.Context, jid types.JID, jpeg []byte) (string, error)
	UpdateGroupParticipants(ctx context.Context, jid types.JID, participants []types.JID, change whatsmeow.ParticipantChange) ([]types.GroupParticipant, error)
	GetJoinedGroups(ctx context.Context) ([]*types.GroupInfo, error)
```

e os métodos do `waClient`:

```go
func (w *waClient) CreateGroup(ctx context.Context, req whatsmeow.ReqCreateGroup) (*types.GroupInfo, error) {
	return w.client.CreateGroup(ctx, req)
}

func (w *waClient) GetGroupInviteLink(ctx context.Context, jid types.JID, reset bool) (string, error) {
	return w.client.GetGroupInviteLink(ctx, jid, reset)
}

func (w *waClient) SetGroupAnnounce(ctx context.Context, jid types.JID, announce bool) error {
	return w.client.SetGroupAnnounce(ctx, jid, announce)
}

func (w *waClient) SetGroupName(ctx context.Context, jid types.JID, name string) error {
	return w.client.SetGroupName(ctx, jid, name)
}

func (w *waClient) SetGroupTopic(ctx context.Context, jid types.JID, topic string) error {
	return w.client.SetGroupTopic(ctx, jid, "", "", topic)
}

func (w *waClient) SetGroupPhoto(ctx context.Context, jid types.JID, jpeg []byte) (string, error) {
	return w.client.SetGroupPhoto(ctx, jid, jpeg)
}

func (w *waClient) UpdateGroupParticipants(ctx context.Context, jid types.JID, participants []types.JID, change whatsmeow.ParticipantChange) ([]types.GroupParticipant, error) {
	return w.client.UpdateGroupParticipants(ctx, jid, participants, change)
}

func (w *waClient) GetJoinedGroups(ctx context.Context) ([]*types.GroupInfo, error) {
	return w.client.GetJoinedGroups(ctx)
}
```

- [ ] **Passo 2: compilar e ver o dublê quebrar**

Run: `go build ./... && go vet ./test/`
Expected: `*fakeWAClient does not implement session.WAClient (missing method CreateGroup)`.

- [ ] **Passo 3: estender o dublê em `test/fake_test.go`**

Acrescentar ao struct `fakeWAClient`:

```go
	groups           map[string]*types.GroupInfo
	createdGroups    []whatsmeow.ReqCreateGroup
	announceCalls    map[string]bool
	nameCalls        map[string]string
	topicCalls       map[string]string
	photoCalls       map[string][]byte
	participantCalls []participantCall
	inviteLinks      map[string]string
	groupErr         error
	nextGroupSeq     int
```

e o tipo auxiliar:

```go
type participantCall struct {
	group        string
	participants []types.JID
	change       whatsmeow.ParticipantChange
}
```

Métodos (todos sob `f.mu`):

```go
func (f *fakeWAClient) CreateGroup(ctx context.Context, req whatsmeow.ReqCreateGroup) (*types.GroupInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.groupErr != nil {
		return nil, f.groupErr
	}
	f.nextGroupSeq++
	jid := types.NewJID(fmt.Sprintf("12036342254761%04d", f.nextGroupSeq), types.GroupServer)
	info := &types.GroupInfo{JID: jid, GroupCreated: time.Now()}
	info.Name = req.Name
	info.IsAnnounce = req.IsAnnounce
	info.Participants = []types.GroupParticipant{{JID: types.NewJID("15550000000", types.DefaultUserServer), IsSuperAdmin: true}}
	if f.groups == nil {
		f.groups = map[string]*types.GroupInfo{}
	}
	f.groups[jid.String()] = info
	f.createdGroups = append(f.createdGroups, req)
	return info, nil
}

func (f *fakeWAClient) GetGroupInfo(ctx context.Context, jid types.JID) (*types.GroupInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if info, ok := f.groups[jid.String()]; ok {
		return info, nil
	}
	return nil, whatsmeow.ErrGroupNotFound
}

func (f *fakeWAClient) GetGroupInviteLink(ctx context.Context, jid types.JID, reset bool) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.groupErr != nil {
		return "", f.groupErr
	}
	if _, ok := f.groups[jid.String()]; !ok {
		return "", whatsmeow.ErrGroupNotFound
	}
	if f.inviteLinks == nil {
		f.inviteLinks = map[string]string{}
	}
	if link, ok := f.inviteLinks[jid.String()]; ok && !reset {
		return link, nil
	}
	link := whatsmeow.InviteLinkPrefix + jid.User + "-" + strconv.Itoa(len(f.inviteLinks)+1)
	f.inviteLinks[jid.String()] = link
	return link, nil
}

func (f *fakeWAClient) SetGroupAnnounce(ctx context.Context, jid types.JID, announce bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.groupErr != nil {
		return f.groupErr
	}
	if f.announceCalls == nil {
		f.announceCalls = map[string]bool{}
	}
	f.announceCalls[jid.String()] = announce
	if info, ok := f.groups[jid.String()]; ok {
		info.IsAnnounce = announce
	}
	return nil
}

func (f *fakeWAClient) SetGroupName(ctx context.Context, jid types.JID, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.nameCalls == nil {
		f.nameCalls = map[string]string{}
	}
	f.nameCalls[jid.String()] = name
	return f.groupErr
}

func (f *fakeWAClient) SetGroupTopic(ctx context.Context, jid types.JID, topic string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.topicCalls == nil {
		f.topicCalls = map[string]string{}
	}
	f.topicCalls[jid.String()] = topic
	return f.groupErr
}

func (f *fakeWAClient) SetGroupPhoto(ctx context.Context, jid types.JID, jpeg []byte) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.photoCalls == nil {
		f.photoCalls = map[string][]byte{}
	}
	f.photoCalls[jid.String()] = jpeg
	return "pic-1", f.groupErr
}

func (f *fakeWAClient) UpdateGroupParticipants(ctx context.Context, jid types.JID, participants []types.JID, change whatsmeow.ParticipantChange) ([]types.GroupParticipant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.participantCalls = append(f.participantCalls, participantCall{group: jid.String(), participants: participants, change: change})
	out := make([]types.GroupParticipant, 0, len(participants))
	for _, p := range participants {
		out = append(out, types.GroupParticipant{JID: p})
	}
	return out, f.groupErr
}

func (f *fakeWAClient) GetJoinedGroups(ctx context.Context) ([]*types.GroupInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*types.GroupInfo, 0, len(f.groups))
	for _, info := range f.groups {
		out = append(out, info)
	}
	return out, f.groupErr
}
```

Remover o `GetGroupInfo` antigo do dublê (que devolvia `ErrIQTimedOut`) — o novo o substitui. Adicionar `"fmt"` e `"strconv"` aos imports. Conferir com `grep -rn "ErrIQTimedOut" test/` se algum teste dependia do comportamento antigo; se sim, esse teste passa a preencher `groupErr = whatsmeow.ErrIQTimedOut` no dublê.

- [ ] **Passo 4: compilar tudo e rodar um teste existente que usa o dublê**

Run: `go build ./... && go vet ./... && go test ./test -run 'TestGatewaySendHandlerPublishesOpaqueMessageIdOnSent' -count=1`
Expected: `ok`.

- [ ] **Passo 5: commit**

```bash
git add internal/session/client.go test/fake_test.go
git commit -m "feat(grupos): WAClient expoe criacao, convite, anuncio, participantes e grupos do numero"
```

---

### Tarefa 5: `internal/groups` — serviço de grupo sobre `GroupClient`

**Files:**
- Create: `internal/groups/client.go`
- Create: `internal/groups/errors.go`
- Create: `internal/groups/photo.go`
- Create: `internal/groups/service.go`
- Test: `internal/groups/photo_test.go`, `internal/groups/service_test.go`

**Interfaces:**
- Consumes: `amqp.RpcError` e construtores (Tarefa 3); `amqp.GroupActionParams` (Tarefa 1).
- Produces:
  - `type GroupClient interface` (subconjunto de `session.WAClient`: `GetGroupInfo`, `CreateGroup`, `GetGroupInviteLink`, `SetGroupAnnounce`, `SetGroupName`, `SetGroupTopic`, `SetGroupPhoto`, `UpdateGroupParticipants`, `GetJoinedGroups`, `PNForLID`).
  - `type Fetch func(ctx, url string) ([]byte, error)`
  - `type CreateRequest struct { Name, Description, PhotoURL string; Announce bool }`
  - `type CreateResult struct { GroupJID, InviteURL string; ParticipantCount int; CreatedAt time.Time }`
  - `type Info struct { GroupJID, Name string; Announce bool; ParticipantCount int; CreatedAt time.Time }`
  - `type ActionResult struct { Removed *int }`
  - `Create(ctx, c GroupClient, fetch Fetch, req CreateRequest, log *slog.Logger) (CreateResult, error)`
  - `InviteLink(ctx, c GroupClient, jid types.JID, reset bool) (string, error)`
  - `Describe(ctx, c GroupClient, jid types.JID) (Info, error)`
  - `Joined(ctx, c GroupClient) ([]Info, error)`
  - `Apply(ctx, c GroupClient, fetch Fetch, action string, jid types.JID, params amqp.GroupActionParams) (ActionResult, error)`
  - `ToJPEG(data []byte) ([]byte, error)`
  - `Classify(err error) error` (whatsmeow → `RpcError`)
  - constantes `ActionLock = "lock"`, `ActionUnlock = "unlock"`, `ActionRemoveParticipants = "remove_participants"`, `ActionSetName = "set_name"`, `ActionSetDescription = "set_description"`, `ActionSetPhoto = "set_photo"`, `Actions = []string{...}`.

- [ ] **Passo 1: teste da foto**

```go
package groups_test

import (
	"bytes"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/groups"
)

func TestToJPEGConvertsPNG(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 4, 4))
	img.Set(1, 1, color.RGBA{R: 255, A: 255})
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}

	out, err := groups.ToJPEG(buf.Bytes())
	if err != nil {
		t.Fatalf("ToJPEG: %v", err)
	}
	if _, err := jpeg.Decode(bytes.NewReader(out)); err != nil {
		t.Fatalf("output is not jpeg: %v", err)
	}
}

func TestToJPEGKeepsJPEG(t *testing.T) {
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, image.NewRGBA(image.Rect(0, 0, 2, 2)), nil); err != nil {
		t.Fatal(err)
	}
	out, err := groups.ToJPEG(buf.Bytes())
	if err != nil {
		t.Fatalf("ToJPEG: %v", err)
	}
	if !bytes.Equal(out, buf.Bytes()) {
		t.Fatal("a jpeg input must pass through untouched")
	}
}

func TestToJPEGRejectsGarbage(t *testing.T) {
	if _, err := groups.ToJPEG([]byte("not an image")); err == nil {
		t.Fatal("expected an error for a non-image")
	}
}
```

- [ ] **Passo 2: teste do serviço (dublê local do `GroupClient`)**

```go
package groups_test

import (
	"context"
	"errors"
	"testing"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/groups"
)

type stubClient struct {
	created   []whatsmeow.ReqCreateGroup
	topics    map[string]string
	photos    map[string][]byte
	announce  map[string]bool
	names     map[string]string
	removed   map[string][]types.JID
	info      map[string]*types.GroupInfo
	inviteErr error
	createErr error
	photoErr  error
}

func newStub() *stubClient {
	return &stubClient{topics: map[string]string{}, photos: map[string][]byte{}, announce: map[string]bool{}, names: map[string]string{}, removed: map[string][]types.JID{}, info: map[string]*types.GroupInfo{}}
}

func (s *stubClient) GetGroupInfo(_ context.Context, jid types.JID) (*types.GroupInfo, error) {
	if info, ok := s.info[jid.String()]; ok {
		return info, nil
	}
	return nil, whatsmeow.ErrGroupNotFound
}

func (s *stubClient) CreateGroup(_ context.Context, req whatsmeow.ReqCreateGroup) (*types.GroupInfo, error) {
	if s.createErr != nil {
		return nil, s.createErr
	}
	s.created = append(s.created, req)
	jid := types.NewJID("120363000000000001", types.GroupServer)
	info := &types.GroupInfo{JID: jid}
	info.Name = req.Name
	info.IsAnnounce = req.IsAnnounce
	info.Participants = []types.GroupParticipant{{JID: types.NewJID("15550000000", types.DefaultUserServer)}}
	s.info[jid.String()] = info
	return info, nil
}

func (s *stubClient) GetGroupInviteLink(_ context.Context, jid types.JID, reset bool) (string, error) {
	if s.inviteErr != nil {
		return "", s.inviteErr
	}
	if reset {
		return whatsmeow.InviteLinkPrefix + "reset-" + jid.User, nil
	}
	return whatsmeow.InviteLinkPrefix + jid.User, nil
}

func (s *stubClient) SetGroupAnnounce(_ context.Context, jid types.JID, announce bool) error {
	s.announce[jid.String()] = announce
	return nil
}

func (s *stubClient) SetGroupName(_ context.Context, jid types.JID, name string) error {
	s.names[jid.String()] = name
	return nil
}

func (s *stubClient) SetGroupTopic(_ context.Context, jid types.JID, topic string) error {
	s.topics[jid.String()] = topic
	return nil
}

func (s *stubClient) SetGroupPhoto(_ context.Context, jid types.JID, jpeg []byte) (string, error) {
	if s.photoErr != nil {
		return "", s.photoErr
	}
	s.photos[jid.String()] = jpeg
	return "pic", nil
}

func (s *stubClient) UpdateGroupParticipants(_ context.Context, jid types.JID, participants []types.JID, change whatsmeow.ParticipantChange) ([]types.GroupParticipant, error) {
	s.removed[jid.String()] = append(s.removed[jid.String()], participants...)
	out := make([]types.GroupParticipant, 0, len(participants))
	for _, p := range participants {
		out = append(out, types.GroupParticipant{JID: p})
	}
	return out, nil
}

func (s *stubClient) GetJoinedGroups(context.Context) ([]*types.GroupInfo, error) {
	out := make([]*types.GroupInfo, 0, len(s.info))
	for _, info := range s.info {
		out = append(out, info)
	}
	return out, nil
}

func (s *stubClient) PNForLID(_ context.Context, lid types.JID) (types.JID, bool, error) {
	if lid.User == "2002125877314" {
		return types.NewJID("5511999887766", types.DefaultUserServer), true, nil
	}
	return types.JID{}, false, nil
}

func pngBytes(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := png.Encode(&buf, image.NewRGBA(image.Rect(0, 0, 2, 2))); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func fetchOf(data []byte, err error) groups.Fetch {
	return func(context.Context, string) ([]byte, error) { return data, err }
}

func TestCreateAppliesEverythingAndReturnsLink(t *testing.T) {
	stub := newStub()
	res, err := groups.Create(context.Background(), stub, fetchOf(pngBytes(t), nil), groups.CreateRequest{
		Name: "GRUPO #1", Description: "Regras", PhotoURL: "https://s3/photo.png", Announce: true,
	}, slog.Default())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if res.GroupJID != "120363000000000001@g.us" || res.InviteURL != whatsmeow.InviteLinkPrefix+"120363000000000001" {
		t.Fatalf("result %+v", res)
	}
	if res.ParticipantCount != 1 {
		t.Fatalf("participantCount %d, want 1", res.ParticipantCount)
	}
	if !stub.created[0].IsAnnounce {
		t.Fatal("announce must be set on CreateGroup itself")
	}
	if stub.topics[res.GroupJID] != "Regras" {
		t.Fatalf("topic %q", stub.topics[res.GroupJID])
	}
	if _, err := jpeg.Decode(bytes.NewReader(stub.photos[res.GroupJID])); err != nil {
		t.Fatalf("photo must be sent as jpeg: %v", err)
	}
}

func TestCreateSurvivesPhotoFailureButNotInviteFailure(t *testing.T) {
	stub := newStub()
	stub.photoErr = whatsmeow.ErrInvalidImageFormat
	res, err := groups.Create(context.Background(), stub, fetchOf(pngBytes(t), nil), groups.CreateRequest{Name: "G", PhotoURL: "https://s3/x"}, slog.Default())
	if err != nil || res.InviteURL == "" {
		t.Fatalf("a photo failure must not fail the creation: %v %+v", err, res)
	}

	stub = newStub()
	stub.inviteErr = whatsmeow.ErrIQNotAuthorized
	_, err = groups.Create(context.Background(), stub, nil, groups.CreateRequest{Name: "G"}, slog.Default())
	var rpcErr *amqp.RpcError
	if !errors.As(err, &rpcErr) || rpcErr.Code != amqp.RpcCodeBadGateway {
		t.Fatalf("invite failure must be bad_gateway, got %v", err)
	}
}

func TestCreateRejectsEmptyName(t *testing.T) {
	_, err := groups.Create(context.Background(), newStub(), nil, groups.CreateRequest{}, slog.Default())
	var rpcErr *amqp.RpcError
	if !errors.As(err, &rpcErr) || rpcErr.Code != amqp.RpcCodeInvalidRequest {
		t.Fatalf("want invalid_request, got %v", err)
	}
}

func TestApplyRemoveParticipantsResolvesPhonesAgainstTheGroup(t *testing.T) {
	stub := newStub()
	jid := types.NewJID("120363000000000009", types.GroupServer)
	stub.info[jid.String()] = &types.GroupInfo{JID: jid, Participants: []types.GroupParticipant{
		{JID: types.NewJID("2002125877314", types.HiddenUserServer), LID: types.NewJID("2002125877314", types.HiddenUserServer), PhoneNumber: types.NewJID("5511999887766", types.DefaultUserServer)},
		{JID: types.NewJID("5511888887777", types.DefaultUserServer), PhoneNumber: types.NewJID("5511888887777", types.DefaultUserServer)},
	}}

	res, err := groups.Apply(context.Background(), stub, nil, groups.ActionRemoveParticipants, jid, amqp.GroupActionParams{Phones: []string{"5511999887766", "5500000000000"}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if res.Removed == nil || *res.Removed != 1 {
		t.Fatalf("removed %v, want 1", res.Removed)
	}
	if got := stub.removed[jid.String()]; len(got) != 1 || got[0].User != "2002125877314" {
		t.Fatalf("must remove by the participant's primary JID (the LID here), got %v", got)
	}
}

func TestApplyRemoveParticipantsWithNoMatchIsOkWithZero(t *testing.T) {
	stub := newStub()
	jid := types.NewJID("120363000000000009", types.GroupServer)
	stub.info[jid.String()] = &types.GroupInfo{JID: jid}
	res, err := groups.Apply(context.Background(), stub, nil, groups.ActionRemoveParticipants, jid, amqp.GroupActionParams{Phones: []string{"5500000000000"}})
	if err != nil || res.Removed == nil || *res.Removed != 0 {
		t.Fatalf("got %v %+v", err, res)
	}
	if len(stub.removed[jid.String()]) != 0 {
		t.Fatal("must not call UpdateGroupParticipants with an empty list")
	}
}

func TestApplyLockUnlockNameDescriptionPhoto(t *testing.T) {
	stub := newStub()
	jid := types.NewJID("120363000000000009", types.GroupServer)
	ctx := context.Background()
	if _, err := groups.Apply(ctx, stub, nil, groups.ActionLock, jid, amqp.GroupActionParams{}); err != nil || !stub.announce[jid.String()] {
		t.Fatalf("lock: %v %v", err, stub.announce)
	}
	if _, err := groups.Apply(ctx, stub, nil, groups.ActionUnlock, jid, amqp.GroupActionParams{}); err != nil || stub.announce[jid.String()] {
		t.Fatalf("unlock: %v %v", err, stub.announce)
	}
	if _, err := groups.Apply(ctx, stub, nil, groups.ActionSetName, jid, amqp.GroupActionParams{Name: "Novo"}); err != nil || stub.names[jid.String()] != "Novo" {
		t.Fatalf("set_name: %v %v", err, stub.names)
	}
	if _, err := groups.Apply(ctx, stub, nil, groups.ActionSetDescription, jid, amqp.GroupActionParams{Description: "Desc"}); err != nil || stub.topics[jid.String()] != "Desc" {
		t.Fatalf("set_description: %v %v", err, stub.topics)
	}
	if _, err := groups.Apply(ctx, stub, fetchOf(pngBytes(t), nil), groups.ActionSetPhoto, jid, amqp.GroupActionParams{PhotoURL: "https://s3/p"}); err != nil || len(stub.photos[jid.String()]) == 0 {
		t.Fatalf("set_photo: %v", err)
	}
	if _, err := groups.Apply(ctx, stub, nil, "explode", jid, amqp.GroupActionParams{}); err == nil {
		t.Fatal("unknown action must fail")
	}
}

func TestClassifyMapsWhatsmeowErrors(t *testing.T) {
	cases := map[error]string{
		whatsmeow.ErrGroupNotFound:     amqp.RpcCodeBadGateway,
		whatsmeow.ErrNotInGroup:        amqp.RpcCodeBadGateway,
		whatsmeow.ErrIQNotAuthorized:   amqp.RpcCodeBadGateway,
		whatsmeow.ErrInvalidImageFormat: amqp.RpcCodeInvalidRequest,
		whatsmeow.ErrIQTimedOut:        amqp.RpcCodeUnavailable,
		whatsmeow.ErrNotConnected:      amqp.RpcCodeUnavailable,
		whatsmeow.ErrNotLoggedIn:       amqp.RpcCodeUnavailable,
		errors.New("anything else"):    amqp.RpcCodeInternal,
	}
	for in, want := range cases {
		var rpcErr *amqp.RpcError
		if !errors.As(groups.Classify(in), &rpcErr) || rpcErr.Code != want {
			t.Fatalf("%v → %v, want %s", in, groups.Classify(in), want)
		}
	}
}
```

Adicionar imports `bytes`, `image`, `image/jpeg`, `image/png`, `log/slog`. Se `whatsmeow.ErrNotConnected`/`ErrNotLoggedIn` não existirem com esse nome no `errors.go` da versão fixada, use os nomes reais (`grep -n "ErrNotConnected\|ErrNotLoggedIn" $(go list -m -f '{{.Dir}}' go.mau.fi/whatsmeow)/errors.go`).

- [ ] **Passo 3: rodar e ver falhar**

Run: `go test ./internal/groups -count=1`
Expected: erro de compilação.

- [ ] **Passo 4: `internal/groups/client.go`**

```go
package groups

import (
	"context"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

type GroupClient interface {
	GetGroupInfo(ctx context.Context, jid types.JID) (*types.GroupInfo, error)
	CreateGroup(ctx context.Context, req whatsmeow.ReqCreateGroup) (*types.GroupInfo, error)
	GetGroupInviteLink(ctx context.Context, jid types.JID, reset bool) (string, error)
	SetGroupAnnounce(ctx context.Context, jid types.JID, announce bool) error
	SetGroupName(ctx context.Context, jid types.JID, name string) error
	SetGroupTopic(ctx context.Context, jid types.JID, topic string) error
	SetGroupPhoto(ctx context.Context, jid types.JID, jpeg []byte) (string, error)
	UpdateGroupParticipants(ctx context.Context, jid types.JID, participants []types.JID, change whatsmeow.ParticipantChange) ([]types.GroupParticipant, error)
	GetJoinedGroups(ctx context.Context) ([]*types.GroupInfo, error)
	PNForLID(ctx context.Context, lid types.JID) (types.JID, bool, error)
}

type Fetch func(ctx context.Context, url string) ([]byte, error)
```

- [ ] **Passo 5: `internal/groups/errors.go`**

```go
package groups

import (
	"errors"

	"go.mau.fi/whatsmeow"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

func Classify(err error) error {
	var rpcErr *amqp.RpcError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &rpcErr):
		return err
	case errors.Is(err, whatsmeow.ErrInvalidImageFormat):
		return amqp.RpcInvalidRequest(err.Error())
	case errors.Is(err, whatsmeow.ErrIQTimedOut), errors.Is(err, whatsmeow.ErrNotConnected), errors.Is(err, whatsmeow.ErrNotLoggedIn):
		return amqp.RpcUnavailable(err.Error())
	case errors.Is(err, whatsmeow.ErrGroupNotFound), errors.Is(err, whatsmeow.ErrNotInGroup), errors.Is(err, whatsmeow.ErrGroupInviteLinkUnauthorized), isIQError(err):
		return amqp.RpcBadGateway(err.Error())
	default:
		return &amqp.RpcError{Code: amqp.RpcCodeInternal, Message: err.Error()}
	}
}

func isIQError(err error) bool {
	var iq *whatsmeow.IQError
	return errors.As(err, &iq)
}
```

- [ ] **Passo 6: `internal/groups/photo.go`**

```go
package groups

import (
	"bytes"
	"fmt"
	"image"
	"image/jpeg"
	_ "image/gif"
	_ "image/png"
)

const jpegQuality = 85

func ToJPEG(data []byte) ([]byte, error) {
	_, format, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("groups: decode photo: %w", err)
	}
	if format == "jpeg" {
		return data, nil
	}
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("groups: decode photo: %w", err)
	}
	var out bytes.Buffer
	if err := jpeg.Encode(&out, img, &jpeg.Options{Quality: jpegQuality}); err != nil {
		return nil, fmt.Errorf("groups: encode photo as jpeg: %w", err)
	}
	return out.Bytes(), nil
}
```

Os imports em branco (`_ "image/png"`) registram os decodificadores; são imports, não comentários.

- [ ] **Passo 7: `internal/groups/service.go`**

```go
package groups

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

const (
	ActionLock               = "lock"
	ActionUnlock             = "unlock"
	ActionRemoveParticipants = "remove_participants"
	ActionSetName            = "set_name"
	ActionSetDescription     = "set_description"
	ActionSetPhoto           = "set_photo"
)

var Actions = []string{ActionLock, ActionUnlock, ActionRemoveParticipants, ActionSetName, ActionSetDescription, ActionSetPhoto}

type CreateRequest struct {
	Name        string
	Description string
	PhotoURL    string
	Announce    bool
}

type CreateResult struct {
	GroupJID         string
	InviteURL        string
	ParticipantCount int
	CreatedAt        time.Time
}

type Info struct {
	GroupJID         string
	Name             string
	Announce         bool
	ParticipantCount int
	CreatedAt        time.Time
}

type ActionResult struct {
	Removed *int
}

func Create(ctx context.Context, c GroupClient, fetch Fetch, req CreateRequest, log *slog.Logger) (CreateResult, error) {
	if strings.TrimSpace(req.Name) == "" {
		return CreateResult{}, amqp.RpcInvalidRequest("group name is required")
	}
	info, err := c.CreateGroup(ctx, whatsmeow.ReqCreateGroup{Name: req.Name, GroupAnnounce: types.GroupAnnounce{IsAnnounce: req.Announce}})
	if err != nil {
		return CreateResult{}, Classify(err)
	}
	if req.Description != "" {
		if err := c.SetGroupTopic(ctx, info.JID, req.Description); err != nil {
			log.Warn("groups: set description after create", "group_jid", info.JID.String(), "error", err)
		}
	}
	if req.PhotoURL != "" {
		if err := setPhoto(ctx, c, fetch, info.JID, req.PhotoURL); err != nil {
			log.Warn("groups: set photo after create", "group_jid", info.JID.String(), "error", err)
		}
	}
	link, err := c.GetGroupInviteLink(ctx, info.JID, false)
	if err != nil {
		return CreateResult{}, Classify(err)
	}
	return CreateResult{
		GroupJID:         info.JID.String(),
		InviteURL:        link,
		ParticipantCount: participantCount(info),
		CreatedAt:        createdAt(info),
	}, nil
}

func InviteLink(ctx context.Context, c GroupClient, jid types.JID, reset bool) (string, error) {
	link, err := c.GetGroupInviteLink(ctx, jid, reset)
	if err != nil {
		return "", Classify(err)
	}
	return link, nil
}

func Describe(ctx context.Context, c GroupClient, jid types.JID) (Info, error) {
	info, err := c.GetGroupInfo(ctx, jid)
	if err != nil {
		return Info{}, Classify(err)
	}
	return infoOf(info), nil
}

func Joined(ctx context.Context, c GroupClient) ([]Info, error) {
	list, err := c.GetJoinedGroups(ctx)
	if err != nil {
		return nil, Classify(err)
	}
	out := make([]Info, 0, len(list))
	for _, info := range list {
		out = append(out, infoOf(info))
	}
	return out, nil
}

func Apply(ctx context.Context, c GroupClient, fetch Fetch, action string, jid types.JID, params amqp.GroupActionParams) (ActionResult, error) {
	var err error
	switch action {
	case ActionLock:
		err = c.SetGroupAnnounce(ctx, jid, true)
	case ActionUnlock:
		err = c.SetGroupAnnounce(ctx, jid, false)
	case ActionSetName:
		if strings.TrimSpace(params.Name) == "" {
			return ActionResult{}, amqp.RpcInvalidRequest("name is required")
		}
		err = c.SetGroupName(ctx, jid, params.Name)
	case ActionSetDescription:
		err = c.SetGroupTopic(ctx, jid, params.Description)
	case ActionSetPhoto:
		if params.PhotoURL == "" {
			return ActionResult{}, amqp.RpcInvalidRequest("photoUrl is required")
		}
		err = setPhoto(ctx, c, fetch, jid, params.PhotoURL)
	case ActionRemoveParticipants:
		return removeParticipants(ctx, c, jid, params.Phones)
	default:
		return ActionResult{}, amqp.RpcInvalidRequest(fmt.Sprintf("unknown action %q", action))
	}
	if err != nil {
		return ActionResult{}, Classify(err)
	}
	return ActionResult{}, nil
}

func removeParticipants(ctx context.Context, c GroupClient, jid types.JID, phones []string) (ActionResult, error) {
	info, err := c.GetGroupInfo(ctx, jid)
	if err != nil {
		return ActionResult{}, Classify(err)
	}
	wanted := make(map[string]struct{}, len(phones))
	for _, phone := range phones {
		wanted[strings.TrimLeft(phone, "+")] = struct{}{}
	}
	targets := make([]types.JID, 0, len(phones))
	for _, p := range info.Participants {
		if _, ok := wanted[p.PhoneNumber.User]; ok {
			targets = append(targets, p.JID)
			continue
		}
		if p.JID.Server == types.DefaultUserServer {
			if _, ok := wanted[p.JID.User]; ok {
				targets = append(targets, p.JID)
			}
		}
	}
	removed := 0
	if len(targets) > 0 {
		if _, err := c.UpdateGroupParticipants(ctx, jid, targets, whatsmeow.ParticipantChangeRemove); err != nil {
			return ActionResult{}, Classify(err)
		}
		removed = len(targets)
	}
	return ActionResult{Removed: &removed}, nil
}

func setPhoto(ctx context.Context, c GroupClient, fetch Fetch, jid types.JID, url string) error {
	if fetch == nil {
		return amqp.RpcInvalidRequest("photo fetch is not available")
	}
	raw, err := fetch(ctx, url)
	if err != nil {
		return amqp.RpcInvalidRequest(fmt.Sprintf("fetch photo: %v", err))
	}
	jpg, err := ToJPEG(raw)
	if err != nil {
		return amqp.RpcInvalidRequest(err.Error())
	}
	_, err = c.SetGroupPhoto(ctx, jid, jpg)
	return err
}

func infoOf(info *types.GroupInfo) Info {
	return Info{
		GroupJID:         info.JID.String(),
		Name:             info.Name,
		Announce:         info.IsAnnounce,
		ParticipantCount: participantCount(info),
		CreatedAt:        createdAt(info),
	}
}

func participantCount(info *types.GroupInfo) int {
	if info.ParticipantCount > 0 {
		return info.ParticipantCount
	}
	if len(info.Participants) > 0 {
		return len(info.Participants)
	}
	return 1
}

func createdAt(info *types.GroupInfo) time.Time {
	if info.GroupCreated.IsZero() {
		return time.Now().UTC()
	}
	return info.GroupCreated.UTC()
}
```

- [ ] **Passo 8: rodar e ver passar**

Run: `go test ./internal/groups -count=1 && go vet ./internal/groups`
Expected: `ok`.

- [ ] **Passo 9: commit**

```bash
git add internal/groups
git commit -m "feat(grupos): servico de grupo — criar, convite, info, acoes e classificacao de erro"
```

---

### Tarefa 6: Ledger de ações em massa (`dedupe`)

**Files:**
- Modify: `internal/dedupe/store.go`
- Test: `test/dedupe_actions_test.go`

**Interfaces:**
- Produces: `(*Store) BeginAction(ctx, commandID, groupJID string) (alreadyDone bool, err error)` e `(*Store) MarkActionDone(ctx, commandID, groupJID string) error`.

- [ ] **Passo 1: teste**

```go
package test

import (
	"context"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/dedupe"
)

func TestDedupeActionsClaimThenDone(t *testing.T) {
	dsn := startPostgresForGateway(t)
	ctx := context.Background()
	store, err := dedupe.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(store.Close)

	done, err := store.BeginAction(ctx, "cmd-1", "g1@g.us")
	if err != nil || done {
		t.Fatalf("first claim: done=%v err=%v", done, err)
	}
	done, err = store.BeginAction(ctx, "cmd-1", "g1@g.us")
	if err != nil || done {
		t.Fatalf("pending redelivery must re-run the action: done=%v err=%v", done, err)
	}
	if err := store.MarkActionDone(ctx, "cmd-1", "g1@g.us"); err != nil {
		t.Fatalf("MarkActionDone: %v", err)
	}
	done, err = store.BeginAction(ctx, "cmd-1", "g1@g.us")
	if err != nil || !done {
		t.Fatalf("after done the claim must report alreadyDone: done=%v err=%v", done, err)
	}
	done, err = store.BeginAction(ctx, "cmd-1", "g2@g.us")
	if err != nil || done {
		t.Fatalf("another group of the same command is independent: done=%v err=%v", done, err)
	}
}
```

- [ ] **Passo 2: rodar e ver falhar**

Run: `go test ./test -run 'TestDedupeActionsClaimThenDone' -count=1`
Expected: erro de compilação.

- [ ] **Passo 3: implementar**

Em `internal/dedupe/store.go`, acrescentar a constante:

```go
const createActionsTableSQL = `CREATE TABLE IF NOT EXISTS gateway_group_actions (
	command_id text NOT NULL,
	group_jid text NOT NULL,
	status text NOT NULL,
	updated_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (command_id, group_jid)
)`
```

Em `Open`, depois do `Exec(createTableSQL)`, executar `createActionsTableSQL` com a mesma forma de erro (`dedupe: create gateway_group_actions table: %w`). E:

```go
func (s *Store) BeginAction(ctx context.Context, commandID, groupJID string) (bool, error) {
	var status string
	err := s.pool.QueryRow(ctx,
		`INSERT INTO gateway_group_actions (command_id, group_jid, status) VALUES ($1, $2, $3)
		 ON CONFLICT (command_id, group_jid) DO UPDATE SET updated_at = now()
		 RETURNING status`,
		commandID, groupJID, statusPending,
	).Scan(&status)
	if err != nil {
		return false, fmt.Errorf("dedupe: begin action %s/%s: %w", commandID, groupJID, err)
	}
	return status == statusSent, nil
}

func (s *Store) MarkActionDone(ctx context.Context, commandID, groupJID string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE gateway_group_actions SET status = $3, updated_at = now() WHERE command_id = $1 AND group_jid = $2`,
		commandID, groupJID, statusSent,
	)
	if err != nil {
		return fmt.Errorf("dedupe: mark action done %s/%s: %w", commandID, groupJID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("dedupe: mark action done %s/%s: no ledger row found", commandID, groupJID)
	}
	return nil
}
```

O `ON CONFLICT … DO UPDATE … RETURNING` devolve a linha existente sem trocar o `status`, então uma reentrega enquanto está `pending` volta `false` (reaplica) e depois de `sent` volta `true`.

- [ ] **Passo 4: rodar e ver passar**

Run: `go test ./test -run 'TestDedupe' -count=1`
Expected: `ok` (os testes antigos do ledger de envio continuam verdes).

- [ ] **Passo 5: commit**

```bash
git add internal/dedupe/store.go test/dedupe_actions_test.go
git commit -m "feat(grupos): ledger gateway_group_actions para idempotencia das acoes em massa"
```

---

### Tarefa 7: Mapeador de participantes (`events.GroupInfo` → evento)

**Files:**
- Create: `internal/gateway/participants.go`
- Test: `internal/gateway/participants_test.go`

**Interfaces:**
- Consumes: `amqp.GroupParticipantsEvent`, `amqp.GroupParticipant` (Tarefa 1).
- Produces: `type PhoneResolver interface { PNForLID(ctx, lid types.JID) (types.JID, bool, error) }`; `BuildGroupParticipants(ctx, resolver PhoneResolver, tenantID, channelID string, e *events.GroupInfo) []amqp.GroupParticipantsEvent`.

- [ ] **Passo 1: teste**

```go
package gateway_test

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/w3nder/whatsmeow-gateway/internal/gateway"
)

type lidResolver map[string]string

func (r lidResolver) PNForLID(_ context.Context, lid types.JID) (types.JID, bool, error) {
	if pn, ok := r[lid.User]; ok {
		return types.NewJID(pn, types.DefaultUserServer), true, nil
	}
	return types.JID{}, false, nil
}

func groupEvent() *events.GroupInfo {
	sender := types.NewJID("15550000000", types.DefaultUserServer)
	return &events.GroupInfo{
		JID:       types.NewJID("120363422547615282", types.GroupServer),
		Sender:    &sender,
		Timestamp: time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
	}
}

func TestBuildGroupParticipantsJoinResolvesPhoneAndLid(t *testing.T) {
	e := groupEvent()
	e.Join = []types.JID{types.NewJID("2002125877314", types.HiddenUserServer), types.NewJID("5511888887777", types.DefaultUserServer)}

	out := gateway.BuildGroupParticipants(context.Background(), lidResolver{"2002125877314": "5511999887766"}, "tenant-1", "channel-1", e)
	if len(out) != 1 || out[0].Type != "join" || out[0].TenantID != "tenant-1" || out[0].GroupJID != "120363422547615282@g.us" {
		t.Fatalf("events %+v", out)
	}
	if out[0].OccurredAt != "2026-09-24T12:00:00Z" {
		t.Fatalf("occurredAt %q", out[0].OccurredAt)
	}
	p := out[0].Participants
	if p[0].JID != "2002125877314@lid" || p[0].LID != "2002125877314@lid" || p[0].Phone != "5511999887766" {
		t.Fatalf("lid participant %+v", p[0])
	}
	if p[1].JID != "5511888887777@s.whatsapp.net" || p[1].LID != "" || p[1].Phone != "5511888887777" {
		t.Fatalf("pn participant %+v", p[1])
	}
	if len(out[0].EventID) != 32 {
		t.Fatalf("eventId %q", out[0].EventID)
	}
}

func TestBuildGroupParticipantsEventIDIsDeterministicAndOrderInsensitive(t *testing.T) {
	a := groupEvent()
	a.Join = []types.JID{types.NewJID("1", types.DefaultUserServer), types.NewJID("2", types.DefaultUserServer)}
	b := groupEvent()
	b.Join = []types.JID{types.NewJID("2", types.DefaultUserServer), types.NewJID("1", types.DefaultUserServer)}
	ia := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, "t", "c", a)[0].EventID
	ib := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, "t", "c", b)[0].EventID
	if ia != ib {
		t.Fatalf("same event in another order must dedupe: %s != %s", ia, ib)
	}
	c := groupEvent()
	c.Leave = a.Join
	if ic := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, "t", "c", c)[0].EventID; ic == ia {
		t.Fatal("a leave must not collide with a join")
	}
}

func TestBuildGroupParticipantsLeaveVersusRemoved(t *testing.T) {
	left := groupEvent()
	self := types.NewJID("5511888887777", types.DefaultUserServer)
	left.Sender = &self
	left.Leave = []types.JID{self}
	if out := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, "t", "c", left); out[0].Type != "leave" {
		t.Fatalf("self leave → %q", out[0].Type)
	}
	kicked := groupEvent()
	kicked.Leave = []types.JID{self}
	if out := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, "t", "c", kicked); out[0].Type != "removed" {
		t.Fatalf("admin removal → %q", out[0].Type)
	}
}

func TestBuildGroupParticipantsSplitsJoinLeavePromoteDemote(t *testing.T) {
	e := groupEvent()
	e.Join = []types.JID{types.NewJID("1", types.DefaultUserServer)}
	e.Leave = []types.JID{types.NewJID("2", types.DefaultUserServer)}
	e.Promote = []types.JID{types.NewJID("3", types.DefaultUserServer)}
	e.Demote = []types.JID{types.NewJID("4", types.DefaultUserServer)}
	out := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, "t", "c", e)
	kinds := []string{}
	for _, evt := range out {
		kinds = append(kinds, evt.Type)
	}
	if len(out) != 4 || kinds[0] != "join" || kinds[1] != "removed" || kinds[2] != "promoted" || kinds[3] != "demoted" {
		t.Fatalf("kinds %v", kinds)
	}
}

func TestBuildGroupParticipantsIgnoresNameOnlyChanges(t *testing.T) {
	e := groupEvent()
	e.Name = &types.GroupName{Name: "x"}
	if out := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, "t", "c", e); len(out) != 0 {
		t.Fatalf("expected no events, got %+v", out)
	}
}
```

- [ ] **Passo 2: rodar e ver falhar**

Run: `go test ./internal/gateway -run 'TestBuildGroupParticipants' -count=1`
Expected: erro de compilação.

- [ ] **Passo 3: implementar `internal/gateway/participants.go`**

```go
package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

type PhoneResolver interface {
	PNForLID(ctx context.Context, lid types.JID) (types.JID, bool, error)
}

func BuildGroupParticipants(ctx context.Context, resolver PhoneResolver, tenantID, channelID string, e *events.GroupInfo) []amqp.GroupParticipantsEvent {
	var out []amqp.GroupParticipantsEvent
	add := func(kind string, jids []types.JID) {
		if len(jids) == 0 {
			return
		}
		out = append(out, amqp.GroupParticipantsEvent{
			TenantID:     tenantID,
			ChannelID:    channelID,
			GroupJID:     e.JID.String(),
			Type:         kind,
			Participants: participantsOf(ctx, resolver, jids),
			EventID:      participantsEventID(e.JID, kind, e.Timestamp, jids),
			OccurredAt:   e.Timestamp.UTC().Format(time.RFC3339),
		})
	}
	add("join", e.Join)
	add(leaveKind(e), e.Leave)
	add("promoted", e.Promote)
	add("demoted", e.Demote)
	return out
}

func leaveKind(e *events.GroupInfo) string {
	if e.Sender == nil {
		return "removed"
	}
	for _, jid := range e.Leave {
		if jid.User == e.Sender.User {
			return "leave"
		}
	}
	return "removed"
}

func participantsOf(ctx context.Context, resolver PhoneResolver, jids []types.JID) []amqp.GroupParticipant {
	out := make([]amqp.GroupParticipant, 0, len(jids))
	for _, jid := range jids {
		p := amqp.GroupParticipant{JID: jid.String()}
		switch jid.Server {
		case types.HiddenUserServer:
			p.LID = jid.String()
			if pn, ok, err := resolver.PNForLID(ctx, jid); err == nil && ok {
				p.Phone = pn.User
			}
		case types.DefaultUserServer:
			p.Phone = jid.User
		}
		out = append(out, p)
	}
	return out
}

func participantsEventID(group types.JID, kind string, at time.Time, jids []types.JID) string {
	users := make([]string, 0, len(jids))
	for _, jid := range jids {
		users = append(users, jid.String())
	}
	slices.Sort(users)
	sum := sha256.Sum256([]byte(group.String() + "|" + kind + "|" + strconv.FormatInt(at.Unix(), 10) + "|" + strings.Join(users, ",")))
	return hex.EncodeToString(sum[:16])
}
```

- [ ] **Passo 4: rodar e ver passar**

Run: `go test ./internal/gateway -run 'TestBuildGroupParticipants' -count=1`
Expected: `ok` (5 testes).

- [ ] **Passo 5: commit**

```bash
git add internal/gateway/participants.go internal/gateway/participants_test.go
git commit -m "feat(grupos): mapeia events.GroupInfo em whatsapp.group.participants.v1"
```

---

### Tarefa 8: Handlers RPC de grupo no gateway

**Files:**
- Create: `internal/gateway/groups.go`
- Modify: `internal/gateway/gateway.go` (`Deps`, `gateway`, `Run`, `run`)
- Modify: `cmd/gateway/main.go`
- Test: `test/gateway_group_rpc_test.go`

**Interfaces:**
- Consumes: `amqp.RpcServer` (Tarefa 3), `groups.*` (Tarefa 5), dublê estendido (Tarefa 4), `rpcProbe` (Tarefa 3, pacote `test`).
- Produces: `Deps.Rpc *amqp.RpcServer`; operações `group.create`, `group.invite_link`, `group.info`, `group.joined`; tipos de pedido/resposta em `internal/gateway/groups.go` (`groupCreateRequest`, `groupCreateResponse`, `groupInviteLinkRequest`, `groupInviteLinkResponse`, `groupInfoRequest`, `groupInfoResponse`, `groupJoinedRequest`, `groupJoinedResponse`), com os campos JSON exatamente como na seção 3 do spec.

- [ ] **Passo 1: teste de integração**

Em `test/gateway_group_rpc_test.go`, um `setupGroupGateway(t, fake, channelID) (conn, cancel, runErrCh)` que copia `setupStatusRoundtripGateway` (Tarefa existente em `test/gateway_status_test.go`) trocando o nome do bucket/instância, criando `rpc := gatewayamqp.NewRpcServer(conn, 4)` e passando `Rpc: rpc` em `gateway.Deps`, e devolvendo a `conn` para o `rpcProbe`. Extraia o que for igual para uma função `bootGatewayDeps(t, fake, channelID, name string) (conn, deps)` reutilizada pelos dois `setup*` — não copie o bloco inteiro duas vezes.

```go
package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

func TestGroupCreateRpcCreatesAnnouncesAndReturnsLink(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)

	probe := newRpcProbe(t, conn)
	reply := probe.call(t, "group.create", "c1", `{"tenantId":"tenant-status-roundtrip","channelId":"channel-groups","name":"GRUPO #1","description":"Regras","announce":true}`, 10*time.Second)
	if reply["ok"] != true {
		t.Fatalf("reply %v", reply)
	}
	result := reply["result"].(map[string]any)
	groupJID := result["groupJid"].(string)
	if groupJID == "" || result["inviteUrl"].(string) == "" || result["participantCount"].(float64) != 1 || result["createdAt"].(string) == "" {
		t.Fatalf("result %v", result)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.createdGroups) != 1 || !fake.createdGroups[0].IsAnnounce || fake.createdGroups[0].Name != "GRUPO #1" {
		t.Fatalf("created %+v", fake.createdGroups)
	}
	if fake.topicCalls[groupJID] != "Regras" {
		t.Fatalf("topic %v", fake.topicCalls)
	}
}

func TestGroupInviteLinkResetAndInfoAndJoined(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)

	probe := newRpcProbe(t, conn)
	created := probe.call(t, "group.create", "c1", `{"tenantId":"t","channelId":"channel-groups","name":"G","announce":false}`, 10*time.Second)
	groupJID := created["result"].(map[string]any)["groupJid"].(string)
	first := created["result"].(map[string]any)["inviteUrl"].(string)

	req, _ := json.Marshal(map[string]any{"tenantId": "t", "channelId": "channel-groups", "groupJid": groupJID, "reset": true})
	reset := probe.call(t, "group.invite_link", "c2", string(req), 10*time.Second)
	if reset["ok"] != true || reset["result"].(map[string]any)["inviteUrl"] == first {
		t.Fatalf("reset must return a new link: %v", reset)
	}

	info := probe.call(t, "group.info", "c3", string(req), 10*time.Second)
	r := info["result"].(map[string]any)
	if r["name"] != "G" || r["announce"] != false || r["participantCount"].(float64) != 1 {
		t.Fatalf("info %v", r)
	}

	joined := probe.call(t, "group.joined", "c4", `{"tenantId":"t","channelId":"channel-groups"}`, 10*time.Second)
	list := joined["result"].(map[string]any)["groups"].([]any)
	if len(list) != 1 || list[0].(map[string]any)["groupJid"] != groupJID {
		t.Fatalf("joined %v", joined)
	}
}

func TestGroupRpcDistinguishesNotPairedFromOffline(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	fake.staysDown = true
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)

	probe := newRpcProbe(t, conn)
	notPaired := probe.call(t, "group.create", "n1", `{"tenantId":"t","channelId":"channel-unknown","name":"G"}`, 15*time.Second)
	if notPaired["ok"] != false || notPaired["error"].(map[string]any)["code"] != "not_found" {
		t.Fatalf("unknown channel → %v", notPaired)
	}
	offline := probe.call(t, "group.create", "o1", `{"tenantId":"t","channelId":"channel-groups","name":"G"}`, 30*time.Second)
	if offline["ok"] != false || offline["error"].(map[string]any)["code"] != "unavailable" {
		t.Fatalf("paired but down → %v", offline)
	}
}

func TestGroupRpcRejectsInvalidPayload(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)

	probe := newRpcProbe(t, conn)
	reply := probe.call(t, "group.info", "i1", `{"tenantId":"t","channelId":"channel-groups","groupJid":"not a jid"}`, 10*time.Second)
	if reply["ok"] != false || reply["error"].(map[string]any)["code"] != "invalid_request" {
		t.Fatalf("bad jid → %v", reply)
	}
}
```

O teste de `staysDown` depende de `ensureUp` falhar depois de `connectWait` (10 s) — por isso o timeout maior. O canal `channel-groups` precisa estar salvo no `registry` pelo `setup` (o `setupStatusRoundtripGateway` já salva o `channelID` recebido com tenant `tenant-status-roundtrip`; mantenha isso em `bootGatewayDeps`).

- [ ] **Passo 2: rodar e ver falhar**

Run: `go test ./test -run 'TestGroup' -count=1`
Expected: erro de compilação (`Deps.Rpc` não existe).

- [ ] **Passo 3: `internal/gateway/groups.go`**

```go
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/groups"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

const (
	rpcGroupCreate     = "group.create"
	rpcGroupInviteLink = "group.invite_link"
	rpcGroupInfo       = "group.info"
	rpcGroupJoined     = "group.joined"
)

type groupCreateRequest struct {
	TenantID    string `json:"tenantId"`
	ChannelID   string `json:"channelId"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	PhotoURL    string `json:"photoUrl,omitempty"`
	Announce    bool   `json:"announce"`
}

type groupCreateResponse struct {
	GroupJID         string `json:"groupJid"`
	InviteURL        string `json:"inviteUrl"`
	ParticipantCount int    `json:"participantCount"`
	CreatedAt        string `json:"createdAt"`
}

type groupInviteLinkRequest struct {
	TenantID  string `json:"tenantId"`
	ChannelID string `json:"channelId"`
	GroupJID  string `json:"groupJid"`
	Reset     bool   `json:"reset"`
}

type groupInviteLinkResponse struct {
	InviteURL string `json:"inviteUrl"`
}

type groupInfoRequest struct {
	TenantID  string `json:"tenantId"`
	ChannelID string `json:"channelId"`
	GroupJID  string `json:"groupJid"`
}

type groupInfoResponse struct {
	GroupJID         string `json:"groupJid"`
	Name             string `json:"name"`
	Announce         bool   `json:"announce"`
	ParticipantCount int    `json:"participantCount"`
	CreatedAt        string `json:"createdAt"`
}

type groupJoinedRequest struct {
	TenantID  string `json:"tenantId"`
	ChannelID string `json:"channelId"`
}

type groupJoinedResponse struct {
	Groups []groupInfoResponse `json:"groups"`
}

func (g *gateway) registerGroupRpc(ctx context.Context) error {
	handlers := map[string]amqp.RpcHandler{
		rpcGroupCreate:     g.rpcGroupCreate,
		rpcGroupInviteLink: g.rpcGroupInviteLink,
		rpcGroupInfo:       g.rpcGroupInfo,
		rpcGroupJoined:     g.rpcGroupJoined,
	}
	for operation, handler := range handlers {
		if err := g.rpc.Handle(ctx, operation, handler); err != nil {
			return fmt.Errorf("gateway: register rpc %s: %w", operation, err)
		}
	}
	return nil
}

func decodeRpc[T any](payload json.RawMessage) (T, error) {
	var req T
	if err := json.Unmarshal(payload, &req); err != nil {
		return req, amqp.RpcInvalidRequest(err.Error())
	}
	return req, nil
}

func (g *gateway) groupClient(ctx context.Context, tenantID, channelID string) (session.WAClient, error) {
	if channelID == "" {
		return nil, amqp.RpcInvalidRequest("channelId is required")
	}
	g.setTenant(channelID, tenantID)
	if err := g.ensureChannelConnected(ctx, channelID); err != nil {
		if errors.Is(err, session.ErrNoSession) {
			return nil, amqp.RpcNotFound(err.Error())
		}
		return nil, amqp.RpcUnavailable(err.Error())
	}
	client, err := g.waClientFor(channelID)
	if err != nil {
		return nil, amqp.RpcUnavailable(err.Error())
	}
	return client, nil
}

func parseGroupJID(raw string) (types.JID, error) {
	jid, err := types.ParseJID(raw)
	if err != nil || jid.Server != types.GroupServer {
		return types.JID{}, amqp.RpcInvalidRequest(fmt.Sprintf("groupJid %q is not a group jid", raw))
	}
	return jid, nil
}

func (g *gateway) rpcGroupCreate(ctx context.Context, payload json.RawMessage) (any, error) {
	req, err := decodeRpc[groupCreateRequest](payload)
	if err != nil {
		return nil, err
	}
	client, err := g.groupClient(ctx, req.TenantID, req.ChannelID)
	if err != nil {
		return nil, err
	}
	res, err := groups.Create(ctx, client, fetchMediaURL, groups.CreateRequest{
		Name: req.Name, Description: req.Description, PhotoURL: req.PhotoURL, Announce: req.Announce,
	}, g.logger)
	if err != nil {
		return nil, err
	}
	g.logger.Info("gateway: group created", "channel_id", req.ChannelID, "group_jid", res.GroupJID)
	return groupCreateResponse{
		GroupJID:         res.GroupJID,
		InviteURL:        res.InviteURL,
		ParticipantCount: res.ParticipantCount,
		CreatedAt:        res.CreatedAt.Format(time.RFC3339),
	}, nil
}

func (g *gateway) rpcGroupInviteLink(ctx context.Context, payload json.RawMessage) (any, error) {
	req, err := decodeRpc[groupInviteLinkRequest](payload)
	if err != nil {
		return nil, err
	}
	jid, err := parseGroupJID(req.GroupJID)
	if err != nil {
		return nil, err
	}
	client, err := g.groupClient(ctx, req.TenantID, req.ChannelID)
	if err != nil {
		return nil, err
	}
	link, err := groups.InviteLink(ctx, client, jid, req.Reset)
	if err != nil {
		return nil, err
	}
	return groupInviteLinkResponse{InviteURL: link}, nil
}

func (g *gateway) rpcGroupInfo(ctx context.Context, payload json.RawMessage) (any, error) {
	req, err := decodeRpc[groupInfoRequest](payload)
	if err != nil {
		return nil, err
	}
	jid, err := parseGroupJID(req.GroupJID)
	if err != nil {
		return nil, err
	}
	client, err := g.groupClient(ctx, req.TenantID, req.ChannelID)
	if err != nil {
		return nil, err
	}
	info, err := groups.Describe(ctx, client, jid)
	if err != nil {
		return nil, err
	}
	return infoResponse(info), nil
}

func (g *gateway) rpcGroupJoined(ctx context.Context, payload json.RawMessage) (any, error) {
	req, err := decodeRpc[groupJoinedRequest](payload)
	if err != nil {
		return nil, err
	}
	client, err := g.groupClient(ctx, req.TenantID, req.ChannelID)
	if err != nil {
		return nil, err
	}
	list, err := groups.Joined(ctx, client)
	if err != nil {
		return nil, err
	}
	out := groupJoinedResponse{Groups: make([]groupInfoResponse, 0, len(list))}
	for _, info := range list {
		out.Groups = append(out.Groups, infoResponse(info))
	}
	return out, nil
}

func infoResponse(info groups.Info) groupInfoResponse {
	return groupInfoResponse{
		GroupJID:         info.GroupJID,
		Name:             info.Name,
		Announce:         info.Announce,
		ParticipantCount: info.ParticipantCount,
		CreatedAt:        info.CreatedAt.Format(time.RFC3339),
	}
}
```

- [ ] **Passo 4: ligar em `gateway.go` e `main.go`**

Em `internal/gateway/gateway.go`:
- `Deps` ganha `Rpc *amqp.RpcServer`; `gateway` ganha `rpc *amqp.RpcServer`; `Run` copia `rpc: deps.Rpc`.
- Em `run()`, depois de `StartCall`:

```go
	if err := g.registerGroupRpc(g.workCtx); err != nil {
		g.closeConsumerForFailedBoot()
		_ = g.ownership.ReleaseAll(g.workCtx, g.instanceID)
		return fmt.Errorf("gateway: start group rpc: %w", err)
	}
```

- No `select` de espera, acrescentar um caso `case rpcErr := <-g.rpc.Failed():` com o mesmo tratamento do consumidor (`fatal = fmt.Errorf("gateway: rpc server died: %w", rpcErr)` e log).
- Em `closeConsumerWithDrainDeadline`, fechar também o RPC: dentro da goroutine, `done <- errors.Join(g.consumer.Close(), g.rpc.Close())`.
- `closeConsumerForFailedBoot` também fecha `g.rpc`.

Em `cmd/gateway/main.go`, depois de criar o `consumer`:

```go
	rpc := amqp.NewRpcServer(conn, cfg.Prefetch)
```

e `Rpc: rpc,` em `gateway.Deps`.

Todos os `gateway.Deps{...}` dos testes existentes precisam de `Rpc: gatewayamqp.NewRpcServer(conn, 4)` — `grep -rn "gateway.Deps{" test/` e ajuste cada um (a variável `conn` já existe em todos). Sem isso o `run()` faz `nil pointer` em `g.rpc.Handle`.

- [ ] **Passo 5: rodar e ver passar**

Run: `go build ./... && go vet ./... && go test ./test -run 'TestGroup' -count=1`
Expected: `ok` (4 testes). Depois um teste antigo com `gateway.Deps`: `go test ./test -run 'TestGatewaySendHandlerPublishesOpaqueMessageIdOnSent|TestGatewayPartialBootFailureClosesConsumerAndReleasesShards' -count=1` → `ok`.

- [ ] **Passo 6: commit**

```bash
git add internal/gateway/groups.go internal/gateway/gateway.go cmd/gateway/main.go test/
git commit -m "feat(grupos): RPC group.create, group.invite_link, group.info e group.joined"
```

---

### Tarefa 9: Consumidor de ações em massa

**Files:**
- Modify: `internal/gateway/groups.go`
- Modify: `internal/gateway/gateway.go` (`run`)
- Test: `test/gateway_group_command_test.go`

**Interfaces:**
- Consumes: `Consumer.StartGroup` (Tarefa 2), `dedupe.BeginAction/MarkActionDone` (Tarefa 6), `groups.Apply` (Tarefa 5).
- Produces: `(*gateway) GroupHandler(ctx, amqp.GatewayGroupCommand) error`.

- [ ] **Passo 1: teste**

```go
package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

func publishGroupCommand(t *testing.T, conn *rabbitmq.Connection, cmd gatewayamqp.GatewayGroupCommand) {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	defer func() { _ = ch.Close() }()
	body, _ := json.Marshal(cmd)
	if err := ch.PublishWithContext(context.Background(), gatewayamqp.GatewayGroupExchange, "1", false, false, rabbitmq.Publishing{
		ContentType: "application/json", DeliveryMode: rabbitmq.Persistent, Body: body,
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func probeEvents(t *testing.T, conn *rabbitmq.Connection, routingKey string) <-chan rabbitmq.Delivery {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open probe: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close() })
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("declare: %v", err)
	}
	if err := ch.QueueBind(q.Name, routingKey, gatewayamqp.EventsExchange, false, nil); err != nil {
		t.Fatalf("bind: %v", err)
	}
	deliveries, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	return deliveries
}

func TestGroupCommandLockPublishesOneResultPerGroup(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	events := probeEvents(t, conn, gatewayamqp.GroupActionRoutingKey)

	probe := newRpcProbe(t, conn)
	g1 := probe.call(t, "group.create", "a", `{"tenantId":"t","channelId":"channel-groups","name":"G1"}`, 10*time.Second)["result"].(map[string]any)["groupJid"].(string)
	g2 := probe.call(t, "group.create", "b", `{"tenantId":"t","channelId":"channel-groups","name":"G2"}`, 10*time.Second)["result"].(map[string]any)["groupJid"].(string)

	publishGroupCommand(t, conn, gatewayamqp.GatewayGroupCommand{CommandID: "cmd-lock", TenantID: "t", ChannelID: "channel-groups", Action: "lock", GroupJIDs: []string{g1, g2, "120363999999999999@g.us"}})

	got := map[string]gatewayamqp.GroupActionEvent{}
	for len(got) < 3 {
		d := waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 10*time.Second)
		var evt gatewayamqp.GroupActionEvent
		if err := json.Unmarshal(d.Body, &evt); err != nil {
			t.Fatal(err)
		}
		got[evt.GroupJID] = evt
	}
	if !got[g1].OK || !got[g2].OK || got[g1].CommandID != "cmd-lock" || got[g1].Action != "lock" || got[g1].TenantID != "t" {
		t.Fatalf("results %+v", got)
	}
	fake.mu.Lock()
	locked := fake.announceCalls[g1] && fake.announceCalls[g2]
	fake.mu.Unlock()
	if !locked {
		t.Fatalf("announce calls %v", fake.announceCalls)
	}
}

func TestGroupCommandRedeliveryReplaysDoneAndReappliesPending(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	events := probeEvents(t, conn, gatewayamqp.GroupActionRoutingKey)

	probe := newRpcProbe(t, conn)
	g1 := probe.call(t, "group.create", "a", `{"tenantId":"t","channelId":"channel-groups","name":"G1"}`, 10*time.Second)["result"].(map[string]any)["groupJid"].(string)

	cmd := gatewayamqp.GatewayGroupCommand{CommandID: "cmd-twice", TenantID: "t", ChannelID: "channel-groups", Action: "set_name", GroupJIDs: []string{g1}, Params: gatewayamqp.GroupActionParams{Name: "Renomeado"}}
	publishGroupCommand(t, conn, cmd)
	waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 10*time.Second)
	publishGroupCommand(t, conn, cmd)
	replay := waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 10*time.Second)

	var evt gatewayamqp.GroupActionEvent
	_ = json.Unmarshal(replay.Body, &evt)
	if !evt.OK || evt.GroupJID != g1 {
		t.Fatalf("replay %+v", evt)
	}
	fake.mu.Lock()
	calls := 0
	for range fake.nameCalls {
		calls++
	}
	fake.mu.Unlock()
	if calls != 1 {
		t.Fatalf("a done item must not hit WhatsApp again on redelivery, nameCalls=%v", fake.nameCalls)
	}
}

func TestGroupCommandRemoveParticipantsReportsRemovedCount(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	events := probeEvents(t, conn, gatewayamqp.GroupActionRoutingKey)

	probe := newRpcProbe(t, conn)
	g1 := probe.call(t, "group.create", "a", `{"tenantId":"t","channelId":"channel-groups","name":"G1"}`, 10*time.Second)["result"].(map[string]any)["groupJid"].(string)

	publishGroupCommand(t, conn, gatewayamqp.GatewayGroupCommand{CommandID: "cmd-rm", TenantID: "t", ChannelID: "channel-groups", Action: "remove_participants", GroupJIDs: []string{g1}, Params: gatewayamqp.GroupActionParams{Phones: []string{"15550000000"}}})
	d := waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 10*time.Second)
	var evt gatewayamqp.GroupActionEvent
	_ = json.Unmarshal(d.Body, &evt)
	if !evt.OK || evt.Removed == nil || *evt.Removed != 1 {
		t.Fatalf("remove result %+v", evt)
	}
}
```

O dublê cria o grupo com um participante `15550000000@s.whatsapp.net` (Tarefa 4), por isso a remoção acha 1. Note que `fake.nameCalls` é um mapa por grupo: para contar chamadas de verdade, troque no dublê `nameCalls map[string]string` por `nameCalls map[string][]string` (append) e ajuste a asserção para `len(fake.nameCalls[g1]) != 1`. Faça essa troca no dublê dentro desta tarefa.

- [ ] **Passo 2: rodar e ver falhar**

Run: `go test ./test -run 'TestGroupCommand' -count=1`
Expected: falha por timeout esperando o evento (o consumidor ainda não existe) ou erro de compilação.

- [ ] **Passo 3: handler em `internal/gateway/groups.go`**

```go
func (g *gateway) GroupHandler(ctx context.Context, cmd amqp.GatewayGroupCommand) error {
	g.setTenant(cmd.ChannelID, cmd.TenantID)
	g.logger.Info("gateway: group command received", "command_id", cmd.CommandID, "channel_id", cmd.ChannelID, "action", cmd.Action, "groups", len(cmd.GroupJIDs))

	client, clientErr := g.groupClient(ctx, cmd.TenantID, cmd.ChannelID)
	for _, raw := range cmd.GroupJIDs {
		alreadyDone, err := g.dedupe.BeginAction(ctx, cmd.CommandID, raw)
		if err != nil {
			return fmt.Errorf("gateway: begin action %s/%s: %w", cmd.CommandID, raw, err)
		}
		result := amqp.GroupActionEvent{TenantID: cmd.TenantID, ChannelID: cmd.ChannelID, CommandID: cmd.CommandID, GroupJID: raw, Action: cmd.Action, OK: true}
		if !alreadyDone {
			result = g.applyGroupAction(ctx, client, clientErr, cmd, raw, result)
		}
		if err := g.publisher.PublishGroupAction(ctx, result); err != nil {
			return fmt.Errorf("gateway: publish group action %s/%s: %w", cmd.CommandID, raw, err)
		}
	}
	return nil
}

func (g *gateway) applyGroupAction(ctx context.Context, client session.WAClient, clientErr error, cmd amqp.GatewayGroupCommand, raw string, result amqp.GroupActionEvent) amqp.GroupActionEvent {
	fail := func(err error) amqp.GroupActionEvent {
		result.OK = false
		result.Error = err.Error()
		return result
	}
	if clientErr != nil {
		return fail(clientErr)
	}
	jid, err := parseGroupJID(raw)
	if err != nil {
		return fail(err)
	}
	applied, err := groups.Apply(ctx, client, fetchMediaURL, cmd.Action, jid, cmd.Params)
	if err != nil {
		return fail(err)
	}
	result.Removed = applied.Removed
	if err := g.dedupe.MarkActionDone(ctx, cmd.CommandID, raw); err != nil {
		g.logger.Error("gateway: mark group action done", "command_id", cmd.CommandID, "group_jid", raw, "error", err)
	}
	return result
}
```

Um erro de WhatsApp em um grupo vira `ok: false` naquele evento e **não** derruba o comando (o handler só devolve erro quando o broker ou o ledger falham, e aí a mensagem vai para a DLQ). Um grupo que falhou fica `pending` no ledger, então uma reentrega o tenta de novo. Um grupo que deu erro permanente (JID inválido) fica `pending` para sempre mas nunca é reentregue, porque o handler devolveu `nil`.

Em `run()`, depois de `StartCall`:

```go
	if err := g.consumer.StartGroup(g.workCtx, g.GroupHandler); err != nil {
		g.closeConsumerForFailedBoot()
		_ = g.ownership.ReleaseAll(g.workCtx, g.instanceID)
		return fmt.Errorf("gateway: start group consumer: %w", err)
	}
```

- [ ] **Passo 4: rodar e ver passar**

Run: `go test ./test -run 'TestGroupCommand' -count=1`
Expected: `ok` (3 testes).

- [ ] **Passo 5: commit**

```bash
git add internal/gateway/groups.go internal/gateway/gateway.go test/gateway_group_command_test.go test/fake_test.go
git commit -m "feat(grupos): acoes em massa pela fila gateway.group com resultado por grupo"
```

---

### Tarefa 10: Publicar entrada e saída de participantes

**Files:**
- Modify: `internal/gateway/gateway.go` (`handleSessionEvent`)
- Test: `test/gateway_group_participants_test.go`

**Interfaces:**
- Consumes: `BuildGroupParticipants` (Tarefa 7), `PublishGroupParticipants` (Tarefa 2), `fake.emit` (dublê).

- [ ] **Passo 1: teste**

```go
package test

import (
	"encoding/json"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

func TestGroupInfoJoinIsPublishedAsParticipantsEvent(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	deliveries := probeEvents(t, conn, gatewayamqp.GroupParticipantsRoutingKey)

	probe := newRpcProbe(t, conn)
	probe.call(t, "group.joined", "warm", `{"tenantId":"tenant-status-roundtrip","channelId":"channel-groups"}`, 10*time.Second)

	fake.emit(&events.GroupInfo{
		JID:       types.NewJID("120363422547615282", types.GroupServer),
		Timestamp: time.Now(),
		Join:      []types.JID{types.NewJID("5511888887777", types.DefaultUserServer)},
	})

	d := waitForDelivery(t, deliveries, gatewayamqp.GroupParticipantsRoutingKey, 10*time.Second)
	var evt gatewayamqp.GroupParticipantsEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		t.Fatal(err)
	}
	if evt.Type != "join" || evt.TenantID != "tenant-status-roundtrip" || evt.ChannelID != "channel-groups" || evt.Participants[0].Phone != "5511888887777" || evt.EventID == "" {
		t.Fatalf("event %+v", evt)
	}
}

func TestGroupInfoNameChangeStillInvalidatesCacheAndPublishesNothing(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	deliveries := probeEvents(t, conn, gatewayamqp.GroupParticipantsRoutingKey)

	probe := newRpcProbe(t, conn)
	probe.call(t, "group.joined", "warm", `{"tenantId":"t","channelId":"channel-groups"}`, 10*time.Second)

	fake.emit(&events.GroupInfo{JID: types.NewJID("120363422547615282", types.GroupServer), Timestamp: time.Now(), Name: &types.GroupName{Name: "Novo"}})

	select {
	case d := <-deliveries:
		t.Fatalf("a name-only change must not publish participants: %s", d.Body)
	case <-time.After(2 * time.Second):
	}
}
```

A chamada `group.joined` de aquecimento garante que a sessão do canal está registrada no `Manager` (o `emit` do dublê só chega ao gateway depois de `register`).

- [ ] **Passo 2: rodar e ver falhar**

Run: `go test ./test -run 'TestGroupInfo' -count=1`
Expected: o primeiro falha por timeout.

- [ ] **Passo 3: implementar**

Em `handleSessionEvent`, trocar o caso `*events.GroupInfo` por:

```go
	case *events.GroupInfo:
		g.handleGroupInfo(channelID, e)
```

e acrescentar em `internal/gateway/groups.go`:

```go
func (g *gateway) handleGroupInfo(channelID string, e *events.GroupInfo) {
	if e.Name != nil {
		g.groups.Invalidate(channelID, e.JID)
	}
	client, err := g.waClientFor(channelID)
	if err != nil {
		g.logger.Error("gateway: resolve client for group info", "channel_id", channelID, "error", err)
		return
	}
	for _, evt := range BuildGroupParticipants(g.workCtx, client, g.tenantFor(channelID), channelID, e) {
		if err := g.publisher.PublishGroupParticipants(g.workCtx, evt); err != nil {
			g.logger.Error("gateway: publish group participants", "channel_id", channelID, "group_jid", evt.GroupJID, "type", evt.Type, "error", err)
		}
	}
}
```

(`"go.mau.fi/whatsmeow/types/events"` entra nos imports de `groups.go`.)

- [ ] **Passo 4: rodar e ver passar**

Run: `go test ./test -run 'TestGroupInfo' -count=1`
Expected: `ok`.

- [ ] **Passo 5: commit**

```bash
git add internal/gateway/gateway.go internal/gateway/groups.go test/gateway_group_participants_test.go
git commit -m "feat(grupos): publica entrada, saida, promocao e rebaixamento de participantes"
```

---

### Tarefa 11: Contrato documentado e suíte inteira

**Files:**
- Create: `docs/group-contract.md`

- [ ] **Passo 1: escrever `docs/group-contract.md`** com, na ordem: as quatro operações RPC (fila, pedido, resposta, códigos de erro e timeout sugerido), o comando `whatsapp.gateway.group.v1` (exchange, fila, ações e `params` de cada uma), os dois eventos (`whatsapp.group.action.v1`, `whatsapp.group.participants.v1`) — cada um com o **mesmo JSON literal** dos testes de contrato da Tarefa 1 e das requisições da Tarefa 8, para que o backend copie dali as fixtures. Siga o formato de `docs/call-contract.md`.

- [ ] **Passo 2: rodar a suíte inteira, um pacote por vez**

Run, em sequência, cada um redirecionado para arquivo:
```bash
go test ./internal/... -count=1 > /tmp/gw-internal.log 2>&1; tail -20 /tmp/gw-internal.log
go test ./test -count=1 -p 1 > /tmp/gw-test.log 2>&1; tail -30 /tmp/gw-test.log
go vet ./... && golangci-lint run 2>/dev/null || true
```
Expected: `ok` em todos; nenhum `FAIL`. Se `golangci-lint` não estiver instalado, `go vet` limpo basta.

- [ ] **Passo 3: commit e conferência final**

```bash
git add docs/group-contract.md
git commit -m "docs(grupos): contrato de RPC, comando e eventos de grupo"
git log --oneline origin/main..HEAD
```

Sem push: a branch fica local até o usuário testar (regra do projeto).
