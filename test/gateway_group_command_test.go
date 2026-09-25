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
	calls := len(fake.nameCalls[g1])
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
