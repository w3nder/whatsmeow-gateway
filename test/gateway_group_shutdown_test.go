package test

import (
	"encoding/json"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

func waitUntil(t *testing.T, timeout time.Duration, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func createGroups(t *testing.T, conn *rabbitmq.Connection, channelID string, n int) []string {
	t.Helper()
	probe := newRpcProbe(t, conn)
	jids := make([]string, 0, n)
	for i := range n {
		payload, _ := json.Marshal(map[string]any{"tenantId": "t", "channelId": channelID, "name": "G"})
		res := probe.call(t, "group.create", channelID+"-create-"+string(rune('a'+i)), string(payload), 10*time.Second)
		jids = append(jids, res["result"].(map[string]any)["groupJid"].(string))
	}
	return jids
}

func TestGroupCommandUnavailableAfterShutdownBeganIsRequeuedNotDeadLettered(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	infra := startGatewayInfra(t, "channel-groups")
	deps := gatewayDepsOn(t, infra, fake, "gateway-groups-a")
	deps.ShutdownDrainTimeout = 300 * time.Millisecond
	cancel, runErrCh := startGroupGatewayWith(t, infra, deps)
	events := probeEvents(t, infra.conn, gatewayamqp.GroupActionRoutingKey)

	groupJIDs := createGroups(t, infra.conn, "channel-groups", 2)
	fake.mu.Lock()
	fake.announceDelay = 1500 * time.Millisecond
	fake.mu.Unlock()

	publishGroupCommand(t, infra.conn, gatewayamqp.GatewayGroupCommand{CommandID: "cmd-cross", TenantID: "t", ChannelID: "channel-groups", Action: "lock", GroupJIDs: groupJIDs})
	waitUntil(t, 10*time.Second, "the first lock to reach whatsapp", func() bool { return fake.announceEnteredCount() == 1 })

	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	connectsAtShutdown := fake.connectCallCount()

	waitUntil(t, 10*time.Second, "the interrupted command back in gateway.group", func() bool {
		ready, dead := groupQueueDepths(t, infra.conn)
		if dead != 0 {
			t.Fatalf("an unavailable caused by the shutdown must never go to the dlq, gateway.group.dlq=%d", dead)
		}
		return ready == 1
	})
	drainEvents(events, func(d rabbitmq.Delivery) {
		t.Fatalf("a shutdown must not publish a result for the interrupted group, got %s", d.Body)
	})
	if fake.connectCallCount() != connectsAtShutdown {
		t.Fatalf("nothing may reopen the channel after the shutdown began, connects %d → %d", connectsAtShutdown, fake.connectCallCount())
	}

	fake.mu.Lock()
	fake.announceDelay = 0
	fake.mu.Unlock()
	cancelB, runErrChB := startGroupGateway(t, infra, fake, "gateway-groups-b")
	defer shutdownStatusRoundtripGateway(t, cancelB, runErrChB)
	okGroups := map[string]bool{}
	for len(okGroups) < len(groupJIDs) {
		d := waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 15*time.Second)
		var evt gatewayamqp.GroupActionEvent
		if err := json.Unmarshal(d.Body, &evt); err != nil {
			t.Fatal(err)
		}
		if !evt.OK {
			t.Fatalf("the next gateway must finish the requeued command, got %+v", evt)
		}
		okGroups[evt.GroupJID] = true
	}
}

func TestGroupCommandChannelDroppingMidCommandDeadLettersWithoutFailures(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	infra := startGatewayInfra(t, "channel-groups")
	cancel, runErrCh := startGroupGateway(t, infra, fake, "gateway-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	events := probeEvents(t, infra.conn, gatewayamqp.GroupActionRoutingKey)

	groupJIDs := createGroups(t, infra.conn, "channel-groups", 2)
	fake.mu.Lock()
	fake.dropAfterLocks = 1
	fake.mu.Unlock()

	publishGroupCommand(t, infra.conn, gatewayamqp.GatewayGroupCommand{CommandID: "cmd-drop", TenantID: "t", ChannelID: "channel-groups", Action: "lock", GroupJIDs: groupJIDs})

	d := waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 10*time.Second)
	var first gatewayamqp.GroupActionEvent
	if err := json.Unmarshal(d.Body, &first); err != nil || !first.OK || first.GroupJID != groupJIDs[0] {
		t.Fatalf("the group locked before the drop must report ok, got %+v (%v)", first, err)
	}
	waitUntil(t, 15*time.Second, "the command in gateway.group.dlq", func() bool {
		_, dead := groupQueueDepths(t, infra.conn)
		return dead == 1
	})
	drainEvents(events, func(d rabbitmq.Delivery) {
		t.Fatalf("a channel that drops mid-command is transient: no ok:false may be published, got %s", d.Body)
	})
	alreadyDone, _, err := infra.dedupe.BeginAction(t.Context(), "cmd-drop", groupJIDs[0])
	if err != nil || !alreadyDone {
		t.Fatalf("the ledger must keep the locked group done for the replay: done=%v err=%v", alreadyDone, err)
	}
}

func TestRpcDrainDeadlineIsIndependentOfTheConsumerDeadline(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	infra := startGatewayInfra(t, "channel-groups")
	deps := gatewayDepsOn(t, infra, fake, "gateway-groups")
	deps.ShutdownDrainTimeout = 10 * time.Second
	deps.RpcDrainTimeout = 300 * time.Millisecond
	cancel, runErrCh := startGroupGatewayWith(t, infra, deps)

	fake.mu.Lock()
	fake.createDelay = 5 * time.Second
	fake.mu.Unlock()
	probe := newRpcProbe(t, infra.conn)
	if err := probe.ch.PublishWithContext(t.Context(), "", gatewayamqp.RpcQueueName("group.create"), false, false, rabbitmq.Publishing{
		ContentType: "application/json", ReplyTo: probe.replyQueue, CorrelationId: "slow-create", Expiration: "30000",
		Body: []byte(`{"tenantId":"t","channelId":"channel-groups","name":"G"}`),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitUntil(t, 10*time.Second, "the create to reach whatsapp", func() bool { return fake.createEnteredCount() == 1 })

	started := time.Now()
	cancel()
	select {
	case err := <-runErrCh:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("a slow rpc must be cut at its own drain deadline, not held for the consumer's 10 s nor for the whole create")
	}
	if took := time.Since(started); took > 3*time.Second {
		t.Fatalf("shutdown took %s with a 300 ms rpc drain deadline", took)
	}
}
