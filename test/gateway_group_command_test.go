package test

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	_ "image/jpeg"
	"image/png"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/ownership"
)

func publishGroupCommand(t *testing.T, conn *rabbitmq.Connection, cmd gatewayamqp.GatewayGroupCommand) {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	defer func() { _ = ch.Close() }()
	body, _ := json.Marshal(cmd)
	if err := ch.PublishWithContext(context.Background(), gatewayamqp.GatewayGroupExchange, commandRoutingKey(cmd.ChannelID), false, false, rabbitmq.Publishing{
		ContentType: "application/json", DeliveryMode: rabbitmq.Persistent, Body: body,
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func commandRoutingKey(channelID string) string {
	return strconv.Itoa(ownership.Shard(channelID, ownership.DefaultShardCount))
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
	if _, err := infra.dedupe.BeginAction(context.Background(), cmd.CommandID, g2); err != nil {
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

func TestGroupCommandSetDescriptionChainsTheTopicAndSkipsAnUnchangedOne(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	events := probeEvents(t, conn, gatewayamqp.GroupActionRoutingKey)

	probe := newRpcProbe(t, conn)
	g1 := probe.call(t, "group.create", "a", `{"tenantId":"t","channelId":"channel-groups","name":"G1","description":"Regras"}`, 10*time.Second)["result"].(map[string]any)["groupJid"].(string)

	for _, commandID := range []string{"cmd-desc-1", "cmd-desc-2"} {
		publishGroupCommand(t, conn, gatewayamqp.GatewayGroupCommand{CommandID: commandID, TenantID: "t", ChannelID: "channel-groups", Action: "set_description", GroupJIDs: []string{g1}, Params: gatewayamqp.GroupActionParams{Description: "Novas regras"}})
		d := waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 10*time.Second)
		var evt gatewayamqp.GroupActionEvent
		if err := json.Unmarshal(d.Body, &evt); err != nil {
			t.Fatal(err)
		}
		if !evt.OK {
			t.Fatalf("%s: the description must replace the one set on create through its topic id, got %+v", commandID, evt)
		}
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if info := fake.groups[g1]; info.Topic != "Novas regras" || fake.topicSeq != 2 {
		t.Fatalf("want the topic set once on create and once by the first command, topic %q after %d changes", info.Topic, fake.topicSeq)
	}
}

func TestGroupCommandSetPhotoCropsALargeImageToWhatsAppsSquare(t *testing.T) {
	photo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		_ = png.Encode(&buf, image.NewRGBA(image.Rect(0, 0, 1920, 1080)))
		_, _ = w.Write(buf.Bytes())
	}))
	defer photo.Close()

	fake := newFakeWAClient()
	fake.markPaired()
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	events := probeEvents(t, conn, gatewayamqp.GroupActionRoutingKey)

	probe := newRpcProbe(t, conn)
	g1 := probe.call(t, "group.create", "a", `{"tenantId":"t","channelId":"channel-groups","name":"G1"}`, 10*time.Second)["result"].(map[string]any)["groupJid"].(string)

	publishGroupCommand(t, conn, gatewayamqp.GatewayGroupCommand{CommandID: "cmd-big-photo", TenantID: "t", ChannelID: "channel-groups", Action: "set_photo", GroupJIDs: []string{g1}, Params: gatewayamqp.GroupActionParams{PhotoURL: photo.URL + "/big.png"}})
	d := waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 10*time.Second)
	var evt gatewayamqp.GroupActionEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil || !evt.OK {
		t.Fatalf("set_photo %+v (%v)", evt, err)
	}
	fake.mu.Lock()
	sent := fake.photoCalls[g1]
	fake.mu.Unlock()
	cfg, format, err := image.DecodeConfig(bytes.NewReader(sent))
	if err != nil || format != "jpeg" {
		t.Fatalf("whatsapp must receive a jpeg, got %q (%v)", format, err)
	}
	if cfg.Width != 640 || cfg.Height != 640 {
		t.Fatalf("a 1920x1080 photo must reach whatsapp as a 640x640 square, got %dx%d", cfg.Width, cfg.Height)
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

func collectGroupResults(t *testing.T, events <-chan rabbitmq.Delivery, n int) map[string]gatewayamqp.GroupActionEvent {
	t.Helper()
	got := map[string]gatewayamqp.GroupActionEvent{}
	for len(got) < n {
		d := waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 15*time.Second)
		var evt gatewayamqp.GroupActionEvent
		if err := json.Unmarshal(d.Body, &evt); err != nil {
			t.Fatal(err)
		}
		got[evt.GroupJID] = evt
	}
	return got
}

const lockedGroupError = gatewayamqp.RpcCodeLocked + ": group is locked (423)"

func TestGroupCommandLockedGroupFailsAloneAndTheCommandCompletes(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	infra := startGatewayInfra(t, "channel-groups")
	cancel, runErrCh := startGroupGateway(t, infra, fake, "gateway-groups")
	events := probeEvents(t, infra.conn, gatewayamqp.GroupActionRoutingKey)

	probe := newRpcProbe(t, infra.conn)
	var groupJIDs []string
	for _, id := range []string{"a", "b", "c"} {
		res := probe.call(t, "group.create", id, `{"tenantId":"t","channelId":"channel-groups","name":"G","announce":true}`, 10*time.Second)
		groupJIDs = append(groupJIDs, res["result"].(map[string]any)["groupJid"].(string))
	}
	locked := groupJIDs[1]
	fake.mu.Lock()
	fake.groupErrs = map[string]error{locked: whatsmeow.ErrIQLocked}
	fake.mu.Unlock()

	publishGroupCommand(t, infra.conn, gatewayamqp.GatewayGroupCommand{CommandID: "cmd-unlock-locked", TenantID: "t", ChannelID: "channel-groups", Action: "unlock", GroupJIDs: groupJIDs})

	got := collectGroupResults(t, events, len(groupJIDs))
	if !got[groupJIDs[0]].OK || !got[groupJIDs[2]].OK {
		t.Fatalf("the groups around the locked one must be unlocked, got %+v", got)
	}
	if res := got[locked]; res.OK || res.Error != lockedGroupError {
		t.Fatalf("the locked group must fail on its own with %q, got %+v", lockedGroupError, res)
	}

	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	if ready, dead := groupQueueDepths(t, infra.conn); ready != 0 || dead != 0 {
		t.Fatalf("a locked group must not hold the command back, gateway.group=%d gateway.group.dlq=%d", ready, dead)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if unlocked, ok := fake.announceCalls[groupJIDs[2]]; !ok || unlocked {
		t.Fatalf("the group after the locked one must reach WhatsApp, announce calls %v", fake.announceCalls)
	}
}

func TestGroupCommandSetPhotoOnAGroupThatRefusesTheChannelFailsAloneAsForbidden(t *testing.T) {
	photo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		_ = png.Encode(&buf, image.NewRGBA(image.Rect(0, 0, 2, 2)))
		_, _ = w.Write(buf.Bytes())
	}))
	defer photo.Close()

	fake := newFakeWAClient()
	fake.markPaired()
	infra := startGatewayInfra(t, "channel-groups")
	cancel, runErrCh := startGroupGateway(t, infra, fake, "gateway-groups")
	events := probeEvents(t, infra.conn, gatewayamqp.GroupActionRoutingKey)

	probe := newRpcProbe(t, infra.conn)
	var groupJIDs []string
	for _, id := range []string{"a", "b", "c"} {
		res := probe.call(t, "group.create", id, `{"tenantId":"t","channelId":"channel-groups","name":"G"}`, 10*time.Second)
		groupJIDs = append(groupJIDs, res["result"].(map[string]any)["groupJid"].(string))
	}
	refusing := groupJIDs[1]
	fake.mu.Lock()
	fake.groupErrs = map[string]error{refusing: &whatsmeow.IQError{Code: 401, Text: "not-authorized"}}
	fake.mu.Unlock()

	publishGroupCommand(t, infra.conn, gatewayamqp.GatewayGroupCommand{CommandID: "cmd-photo-forbidden", TenantID: "t", ChannelID: "channel-groups", Action: "set_photo", GroupJIDs: groupJIDs, Params: gatewayamqp.GroupActionParams{PhotoURL: photo.URL + "/p.png"}})

	got := collectGroupResults(t, events, len(groupJIDs))
	if !got[groupJIDs[0]].OK || !got[groupJIDs[2]].OK {
		t.Fatalf("the groups around the refusing one must get the photo, got %+v", got)
	}
	const forbidden = gatewayamqp.RpcCodeForbidden + ": not an admin of the group (401)"
	if res := got[refusing]; res.OK || res.Error != forbidden {
		t.Fatalf("the refusing group must fail on its own with %q, got %+v", forbidden, res)
	}

	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	if ready, dead := groupQueueDepths(t, infra.conn); ready != 0 || dead != 0 {
		t.Fatalf("a group refusing the channel must not hold the command back, gateway.group=%d gateway.group.dlq=%d", ready, dead)
	}
}

func TestGroupCommandReinjectedFromTheDLQSkipsFinishedGroupsAndFailsTheLockedOne(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	infra := startGatewayInfra(t, "channel-groups")
	cancel, runErrCh := startGroupGateway(t, infra, fake, "gateway-groups")
	events := probeEvents(t, infra.conn, gatewayamqp.GroupActionRoutingKey)

	probe := newRpcProbe(t, infra.conn)
	var groupJIDs []string
	for _, id := range []string{"a", "b", "c"} {
		res := probe.call(t, "group.create", id, `{"tenantId":"t","channelId":"channel-groups","name":"G","announce":true}`, 10*time.Second)
		groupJIDs = append(groupJIDs, res["result"].(map[string]any)["groupJid"].(string))
	}
	done, locked, pending := groupJIDs[0], groupJIDs[1], groupJIDs[2]
	fake.mu.Lock()
	fake.groupErrs = map[string]error{locked: whatsmeow.ErrIQLocked}
	fake.mu.Unlock()

	cmd := gatewayamqp.GatewayGroupCommand{CommandID: "cmd-from-dlq", TenantID: "t", ChannelID: "channel-groups", Action: "unlock", GroupJIDs: groupJIDs}
	ctx := context.Background()
	if _, err := infra.dedupe.BeginAction(ctx, cmd.CommandID, done); err != nil {
		t.Fatalf("seed the finished group: %v", err)
	}
	if err := infra.dedupe.MarkActionDone(ctx, cmd.CommandID, done, nil); err != nil {
		t.Fatalf("seed the finished group: %v", err)
	}
	if _, err := infra.dedupe.BeginAction(ctx, cmd.CommandID, locked); err != nil {
		t.Fatalf("leave the locked group pending, as the command halted on it before the fix: %v", err)
	}

	publishGroupCommand(t, infra.conn, cmd)
	got := collectGroupResults(t, events, len(groupJIDs))
	if !got[done].OK || !got[pending].OK {
		t.Fatalf("the finished group must replay ok and the pending one must run, got %+v", got)
	}
	if res := got[locked]; res.OK || res.Error != lockedGroupError {
		t.Fatalf("the locked group must fail on its own with %q, got %+v", lockedGroupError, res)
	}
	callsAfterFirstRun := fake.announceCallCount()
	if callsAfterFirstRun != 2 {
		t.Fatalf("only the locked and the pending group may reach WhatsApp, announce calls %d", callsAfterFirstRun)
	}

	publishGroupCommand(t, infra.conn, cmd)
	replayed := collectGroupResults(t, events, len(groupJIDs))
	if !replayed[done].OK || !replayed[pending].OK || replayed[locked].OK || replayed[locked].Error != lockedGroupError {
		t.Fatalf("a second reinjection must replay the same results from the ledger, got %+v", replayed)
	}
	if calls := fake.announceCallCount(); calls != callsAfterFirstRun {
		t.Fatalf("a second reinjection must not reach WhatsApp again, announce calls %d", calls)
	}

	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	if ready, dead := groupQueueDepths(t, infra.conn); ready != 0 || dead != 0 {
		t.Fatalf("the reinjected command must be acked, gateway.group=%d gateway.group.dlq=%d", ready, dead)
	}
}

func TestGroupCommandGroupFailingWithServerErrorOnEveryReplayFailsAloneOnTheThirdHalt(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	infra := startGatewayInfra(t, "channel-groups")
	cancel, runErrCh := startGroupGateway(t, infra, fake, "gateway-groups")
	events := probeEvents(t, infra.conn, gatewayamqp.GroupActionRoutingKey)

	probe := newRpcProbe(t, infra.conn)
	broken := probe.call(t, "group.create", "a", `{"tenantId":"t","channelId":"channel-groups","name":"G1"}`, 10*time.Second)["result"].(map[string]any)["groupJid"].(string)
	healthy := probe.call(t, "group.create", "b", `{"tenantId":"t","channelId":"channel-groups","name":"G2"}`, 10*time.Second)["result"].(map[string]any)["groupJid"].(string)
	fake.mu.Lock()
	fake.groupErrs = map[string]error{broken: whatsmeow.ErrIQServiceUnavailable}
	fake.mu.Unlock()

	cmd := gatewayamqp.GatewayGroupCommand{CommandID: "cmd-always-503", TenantID: "t", ChannelID: "channel-groups", Action: "lock", GroupJIDs: []string{broken, healthy}}
	for replay := 1; replay < 3; replay++ {
		publishGroupCommand(t, infra.conn, cmd)
		waitUntil(t, 15*time.Second, "the halted command in gateway.group.dlq", func() bool {
			_, dead := groupQueueDepths(t, infra.conn)
			return dead == replay
		})
		drainEvents(events, func(d rabbitmq.Delivery) {
			t.Fatalf("halt %d of the limit must not publish a result, got %s", replay, d.Body)
		})
	}

	publishGroupCommand(t, infra.conn, cmd)
	got := collectGroupResults(t, events, 2)
	if res := got[broken]; res.OK || !strings.HasPrefix(res.Error, gatewayamqp.RpcCodeUnavailable+": ") {
		t.Fatalf("on the third halt the group must fail on its own as unavailable, got %+v", res)
	}
	if !got[healthy].OK {
		t.Fatalf("the group after the broken one must finally run, got %+v", got[healthy])
	}

	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	if ready, dead := groupQueueDepths(t, infra.conn); ready != 0 || dead != 2 {
		t.Fatalf("only the two earlier halts may sit in the dlq, gateway.group=%d gateway.group.dlq=%d", ready, dead)
	}
	if calls := fake.announceCallCount(); calls != 4 {
		t.Fatalf("the broken group must be tried three times and the healthy one once, announce calls %d", calls)
	}
}
