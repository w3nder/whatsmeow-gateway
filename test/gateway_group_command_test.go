package test

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	"image/png"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"

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
	if unknown := got["120363999999999999@g.us"]; unknown.OK || unknown.Error == "" {
		t.Fatalf("a group whatsapp does not know must fail on its own, got %+v", unknown)
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
	infra := startGatewayInfra(t, "channel-groups")
	cancel, runErrCh := startGroupGateway(t, infra, fake, "gateway-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	events := probeEvents(t, infra.conn, gatewayamqp.GroupActionRoutingKey)

	probe := newRpcProbe(t, infra.conn)
	g1 := probe.call(t, "group.create", "a", `{"tenantId":"t","channelId":"channel-groups","name":"G1"}`, 10*time.Second)["result"].(map[string]any)["groupJid"].(string)
	g2 := probe.call(t, "group.create", "b", `{"tenantId":"t","channelId":"channel-groups","name":"G2"}`, 10*time.Second)["result"].(map[string]any)["groupJid"].(string)

	cmd := gatewayamqp.GatewayGroupCommand{CommandID: "cmd-twice", TenantID: "t", ChannelID: "channel-groups", Action: "set_name", GroupJIDs: []string{g1}, Params: gatewayamqp.GroupActionParams{Name: "Renomeado"}}
	publishGroupCommand(t, infra.conn, cmd)
	waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 10*time.Second)
	if _, _, err := infra.dedupe.BeginAction(context.Background(), cmd.CommandID, g2); err != nil {
		t.Fatalf("leave g2 pending in the ledger: %v", err)
	}

	cmd.GroupJIDs = []string{g1, g2}
	publishGroupCommand(t, infra.conn, cmd)
	got := map[string]gatewayamqp.GroupActionEvent{}
	for len(got) < 2 {
		d := waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 10*time.Second)
		var evt gatewayamqp.GroupActionEvent
		if err := json.Unmarshal(d.Body, &evt); err != nil {
			t.Fatal(err)
		}
		got[evt.GroupJID] = evt
	}
	if !got[g1].OK || !got[g2].OK {
		t.Fatalf("both the replayed and the reapplied group must report ok, got %+v", got)
	}
	fake.mu.Lock()
	g1Calls, g2Calls := len(fake.nameCalls[g1]), len(fake.nameCalls[g2])
	fake.mu.Unlock()
	if g1Calls != 1 || g2Calls != 1 {
		t.Fatalf("the done group must not hit WhatsApp again and the pending one exactly once, nameCalls=%v", fake.nameCalls)
	}
}

func TestGroupCommandWithChannelOfflineFailsEveryGroupAndAcks(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	fake.staysDown = true
	infra := startGatewayInfra(t, "channel-groups")
	cancel, runErrCh := startGroupGateway(t, infra, fake, "gateway-groups")
	events := probeEvents(t, infra.conn, gatewayamqp.GroupActionRoutingKey)

	groupJIDs := []string{"120363000000000001@g.us", "120363000000000002@g.us"}
	publishGroupCommand(t, infra.conn, gatewayamqp.GatewayGroupCommand{CommandID: "cmd-offline", TenantID: "t", ChannelID: "channel-groups", Action: "lock", GroupJIDs: groupJIDs})

	got := map[string]gatewayamqp.GroupActionEvent{}
	for len(got) < len(groupJIDs) {
		d := waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 30*time.Second)
		var evt gatewayamqp.GroupActionEvent
		if err := json.Unmarshal(d.Body, &evt); err != nil {
			t.Fatal(err)
		}
		got[evt.GroupJID] = evt
	}
	for _, jid := range groupJIDs {
		if got[jid].OK || !strings.HasPrefix(got[jid].Error, gatewayamqp.RpcCodeUnavailable+":") {
			t.Fatalf("offline channel must fail %s as unavailable, got %+v", jid, got[jid])
		}
	}

	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	if ready, dead := groupQueueDepths(t, infra.conn); ready != 0 || dead != 0 {
		t.Fatalf("the command must be acked, gateway.group=%d gateway.group.dlq=%d", ready, dead)
	}
	if fake.announceCallCount() != 0 {
		t.Fatalf("an offline channel must not reach WhatsApp, announce calls %d", fake.announceCallCount())
	}
}

func TestGroupCommandInterruptedByShutdownIsRequeuedAndFinishedByTheNextGateway(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	infra := startGatewayInfra(t, "channel-groups")
	cancel, runErrCh := startGroupGateway(t, infra, fake, "gateway-groups-a")
	events := probeEvents(t, infra.conn, gatewayamqp.GroupActionRoutingKey)

	probe := newRpcProbe(t, infra.conn)
	var groupJIDs []string
	for _, id := range []string{"a", "b", "c"} {
		res := probe.call(t, "group.create", id, `{"tenantId":"t","channelId":"channel-groups","name":"G"}`, 10*time.Second)
		groupJIDs = append(groupJIDs, res["result"].(map[string]any)["groupJid"].(string))
	}
	fake.mu.Lock()
	fake.announceDelay = 400 * time.Millisecond
	fake.mu.Unlock()

	publishGroupCommand(t, infra.conn, gatewayamqp.GatewayGroupCommand{CommandID: "cmd-shutdown", TenantID: "t", ChannelID: "channel-groups", Action: "lock", GroupJIDs: groupJIDs})

	okGroups := map[string]bool{}
	record := func(d rabbitmq.Delivery) {
		t.Helper()
		var evt gatewayamqp.GroupActionEvent
		if err := json.Unmarshal(d.Body, &evt); err != nil {
			t.Fatal(err)
		}
		if !evt.OK {
			t.Fatalf("a shutdown must never be published as a failure, got %+v", evt)
		}
		okGroups[evt.GroupJID] = true
	}
	record(waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 10*time.Second))
	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	drainEvents(events, record)
	if len(okGroups) == len(groupJIDs) {
		t.Fatalf("the shutdown must interrupt the command before the last group, got %v", okGroups)
	}

	cancelB, runErrChB := startGroupGateway(t, infra, fake, "gateway-groups-b")
	defer shutdownStatusRoundtripGateway(t, cancelB, runErrChB)
	for len(okGroups) < len(groupJIDs) {
		record(waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 15*time.Second))
	}
	if calls := fake.announceCallCount(); calls != len(groupJIDs) {
		t.Fatalf("each group must hit WhatsApp exactly once across both gateways, got %d calls", calls)
	}
}

func TestGroupCommandHaltsOnRateLimitAndDeadLetters(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	infra := startGatewayInfra(t, "channel-groups")
	cancel, runErrCh := startGroupGateway(t, infra, fake, "gateway-groups")
	events := probeEvents(t, infra.conn, gatewayamqp.GroupActionRoutingKey)

	probe := newRpcProbe(t, infra.conn)
	g1 := probe.call(t, "group.create", "a", `{"tenantId":"t","channelId":"channel-groups","name":"G1"}`, 10*time.Second)["result"].(map[string]any)["groupJid"].(string)
	g2 := probe.call(t, "group.create", "b", `{"tenantId":"t","channelId":"channel-groups","name":"G2"}`, 10*time.Second)["result"].(map[string]any)["groupJid"].(string)
	fake.mu.Lock()
	fake.groupErr = whatsmeow.ErrIQRateOverLimit
	fake.mu.Unlock()

	publishGroupCommand(t, infra.conn, gatewayamqp.GatewayGroupCommand{CommandID: "cmd-rate", TenantID: "t", ChannelID: "channel-groups", Action: "lock", GroupJIDs: []string{g1, g2}})

	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, dead := groupQueueDepths(t, infra.conn); dead == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a rate-limited command must go to the dlq for a later replay")
		}
		time.Sleep(100 * time.Millisecond)
	}
	drainEvents(events, func(d rabbitmq.Delivery) {
		t.Fatalf("a rate limit must not be published as a result, got %s", d.Body)
	})
	if calls := fake.announceCallCount(); calls != 1 {
		t.Fatalf("the command must stop at the first rate limit, announce calls %d", calls)
	}
	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
}

func TestGroupCommandSetPhotoFetchesOnceAndPacesTheGroups(t *testing.T) {
	var fetches atomic.Int32
	photo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		var buf bytes.Buffer
		_ = png.Encode(&buf, image.NewRGBA(image.Rect(0, 0, 2, 2)))
		_, _ = w.Write(buf.Bytes())
	}))
	defer photo.Close()

	fake := newFakeWAClient()
	fake.markPaired()
	infra := startGatewayInfra(t, "channel-groups")
	cancel, runErrCh := startGroupGateway(t, infra, fake, "gateway-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	events := probeEvents(t, infra.conn, gatewayamqp.GroupActionRoutingKey)

	probe := newRpcProbe(t, infra.conn)
	g1 := probe.call(t, "group.create", "a", `{"tenantId":"t","channelId":"channel-groups","name":"G1"}`, 10*time.Second)["result"].(map[string]any)["groupJid"].(string)
	g2 := probe.call(t, "group.create", "b", `{"tenantId":"t","channelId":"channel-groups","name":"G2"}`, 10*time.Second)["result"].(map[string]any)["groupJid"].(string)

	publishGroupCommand(t, infra.conn, gatewayamqp.GatewayGroupCommand{CommandID: "cmd-photo", TenantID: "t", ChannelID: "channel-groups", Action: "set_photo", GroupJIDs: []string{g1, g2}, Params: gatewayamqp.GroupActionParams{PhotoURL: photo.URL + "/p.png"}})

	var arrivals []time.Time
	for len(arrivals) < 2 {
		d := waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 10*time.Second)
		var evt gatewayamqp.GroupActionEvent
		if err := json.Unmarshal(d.Body, &evt); err != nil {
			t.Fatal(err)
		}
		if !evt.OK {
			t.Fatalf("set_photo %+v", evt)
		}
		arrivals = append(arrivals, time.Now())
	}
	if gap := arrivals[1].Sub(arrivals[0]); gap < 250*time.Millisecond {
		t.Fatalf("groups of one command must be paced, second result came %s after the first", gap)
	}
	if n := fetches.Load(); n != 1 {
		t.Fatalf("the photo must be fetched once per command, got %d fetches", n)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.photoCalls[g1]) == 0 || len(fake.photoCalls[g2]) == 0 {
		t.Fatalf("photo calls %v", fake.photoCalls)
	}
}

func drainEvents(events <-chan rabbitmq.Delivery, record func(rabbitmq.Delivery)) {
	for {
		select {
		case d := <-events:
			if d.RoutingKey == gatewayamqp.GroupActionRoutingKey {
				record(d)
			}
		case <-time.After(500 * time.Millisecond):
			return
		}
	}
}

func groupQueueDepths(t *testing.T, conn *rabbitmq.Connection) (ready, dead int) {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	defer func() { _ = ch.Close() }()
	queue, err := ch.QueueDeclarePassive(gatewayamqp.GatewayGroupQueue, true, false, false, false, rabbitmq.Table{"x-queue-type": "quorum", "x-dead-letter-exchange": gatewayamqp.GatewayGroupDLX})
	if err != nil {
		t.Fatalf("inspect %s: %v", gatewayamqp.GatewayGroupQueue, err)
	}
	dlq, err := ch.QueueDeclarePassive(gatewayamqp.GatewayGroupDLQ, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("inspect %s: %v", gatewayamqp.GatewayGroupDLQ, err)
	}
	return queue.Messages, dlq.Messages
}

func TestGroupCommandRemoveParticipantsReportsRemovedCount(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	events := probeEvents(t, conn, gatewayamqp.GroupActionRoutingKey)

	probe := newRpcProbe(t, conn)
	g1 := probe.call(t, "group.create", "a", `{"tenantId":"t","channelId":"channel-groups","name":"G1"}`, 10*time.Second)["result"].(map[string]any)["groupJid"].(string)
	fake.addGroupParticipant(g1, types.GroupParticipant{JID: types.NewJID("5511988887777", types.DefaultUserServer)})

	publishGroupCommand(t, conn, gatewayamqp.GatewayGroupCommand{CommandID: "cmd-rm", TenantID: "t", ChannelID: "channel-groups", Action: "remove_participants", GroupJIDs: []string{g1}, Params: gatewayamqp.GroupActionParams{Phones: []string{"551188887777"}}})
	d := waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 10*time.Second)
	var evt gatewayamqp.GroupActionEvent
	_ = json.Unmarshal(d.Body, &evt)
	if !evt.OK || evt.Removed == nil || *evt.Removed != 1 {
		t.Fatalf("remove result %+v", evt)
	}
}

func TestGroupCommandRemoveParticipantsRedeliveryReplaysStoredRemovedCount(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	events := probeEvents(t, conn, gatewayamqp.GroupActionRoutingKey)

	probe := newRpcProbe(t, conn)
	g1 := probe.call(t, "group.create", "a", `{"tenantId":"t","channelId":"channel-groups","name":"G1"}`, 10*time.Second)["result"].(map[string]any)["groupJid"].(string)

	fake.addGroupParticipant(g1, types.GroupParticipant{JID: types.NewJID("5511988887777", types.DefaultUserServer)})

	cmd := gatewayamqp.GatewayGroupCommand{CommandID: "cmd-rm-twice", TenantID: "t", ChannelID: "channel-groups", Action: "remove_participants", GroupJIDs: []string{g1}, Params: gatewayamqp.GroupActionParams{Phones: []string{"551188887777"}}}
	publishGroupCommand(t, conn, cmd)
	waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 10*time.Second)
	publishGroupCommand(t, conn, cmd)
	replay := waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 10*time.Second)

	var evt gatewayamqp.GroupActionEvent
	if err := json.Unmarshal(replay.Body, &evt); err != nil {
		t.Fatal(err)
	}
	if !evt.OK || evt.Removed == nil || *evt.Removed != 1 {
		t.Fatalf("replay must still carry the stored removed count: %+v", evt)
	}
	fake.mu.Lock()
	calls := len(fake.participantCalls)
	fake.mu.Unlock()
	if calls != 1 {
		t.Fatalf("a done item must not hit WhatsApp again on redelivery, participantCalls=%v", fake.participantCalls)
	}
}
