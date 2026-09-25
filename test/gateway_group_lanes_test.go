package test

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

const minGroupGap = 300 * time.Millisecond

func TestGroupCommandsOfDifferentChannelsRunInParallel(t *testing.T) {
	slow, fast := newFakeWAClient(), newFakeWAClient()
	slow.markPaired()
	fast.markPaired()
	fakes := map[string]*fakeWAClient{"channel-slow": slow, "channel-fast": fast}

	infra := startGatewayInfra(t, "channel-slow")
	if err := infra.registry.Save(t.Context(), "channel-fast", types.NewJID("15550008888", types.DefaultUserServer).String(), "t"); err != nil {
		t.Fatalf("registry.Save: %v", err)
	}
	deps := gatewayDepsOn(t, infra, slow, "gateway-groups")
	deps.Manager = session.NewManager(func(channelID string, _ *types.JID) (session.WAClient, error) {
		return fakes[channelID], nil
	})
	cancel, runErrCh := startGroupGatewayWith(t, infra, deps)
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	events := probeEvents(t, infra.conn, gatewayamqp.GroupActionRoutingKey)

	slowGroups := createGroups(t, infra.conn, "channel-slow", 2)
	fastGroups := createGroups(t, infra.conn, "channel-fast", 1)
	slow.mu.Lock()
	slow.announceDelay = 3 * time.Second
	slow.mu.Unlock()

	publishGroupCommand(t, infra.conn, gatewayamqp.GatewayGroupCommand{CommandID: "cmd-slow", TenantID: "t", ChannelID: "channel-slow", Action: "lock", GroupJIDs: slowGroups})
	waitUntil(t, 10*time.Second, "the slow channel to be busy in whatsapp", func() bool { return slow.announceEnteredCount() == 1 })
	publishedFast := time.Now()
	publishGroupCommand(t, infra.conn, gatewayamqp.GatewayGroupCommand{CommandID: "cmd-fast", TenantID: "t", ChannelID: "channel-fast", Action: "lock", GroupJIDs: fastGroups})

	d := waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 10*time.Second)
	var first gatewayamqp.GroupActionEvent
	if err := json.Unmarshal(d.Body, &first); err != nil {
		t.Fatal(err)
	}
	if first.ChannelID != "channel-fast" || !first.OK {
		t.Fatalf("the idle channel must not wait for the busy one, first result %+v", first)
	}
	if took := time.Since(publishedFast); took > 2500*time.Millisecond {
		t.Fatalf("the idle channel waited %s behind the busy one", took)
	}

	var slowArrivals []time.Time
	for len(slowArrivals) < len(slowGroups) {
		d := waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 15*time.Second)
		var evt gatewayamqp.GroupActionEvent
		if err := json.Unmarshal(d.Body, &evt); err != nil {
			t.Fatal(err)
		}
		if evt.ChannelID != "channel-slow" || !evt.OK {
			t.Fatalf("slow channel result %+v", evt)
		}
		slowArrivals = append(slowArrivals, time.Now())
	}
	if gap := slowArrivals[1].Sub(slowArrivals[0]); gap < 3*time.Second {
		t.Fatalf("groups of the same channel must still run one after the other, second came %s after the first", gap)
	}
}

func TestGroupCommandsOfTheSameChannelArePacedAcrossCommands(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	events := probeEvents(t, conn, gatewayamqp.GroupActionRoutingKey)

	groupJIDs := createGroups(t, conn, "channel-groups", 2)
	publishGroupCommand(t, conn, gatewayamqp.GatewayGroupCommand{CommandID: "cmd-pace-1", TenantID: "t", ChannelID: "channel-groups", Action: "lock", GroupJIDs: groupJIDs[:1]})
	publishGroupCommand(t, conn, gatewayamqp.GatewayGroupCommand{CommandID: "cmd-pace-2", TenantID: "t", ChannelID: "channel-groups", Action: "lock", GroupJIDs: groupJIDs[1:]})

	for range 2 {
		d := waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 10*time.Second)
		var evt gatewayamqp.GroupActionEvent
		if err := json.Unmarshal(d.Body, &evt); err != nil || !evt.OK {
			t.Fatalf("result %+v (%v)", evt, err)
		}
	}
	calls := fake.announceStarts()
	if len(calls) != 2 {
		t.Fatalf("want two whatsapp calls, got %d", len(calls))
	}
	if gap := calls[1].Sub(calls[0]); gap < minGroupGap {
		t.Fatalf("two commands on one channel must keep the gap between groups, the second call started %s after the first", gap)
	}
}

func TestABusyChannelNeverHoldsEveryGroupPrefetchSlot(t *testing.T) {
	busy, idle := newFakeWAClient(), newFakeWAClient()
	busy.markPaired()
	idle.markPaired()
	fakes := map[string]*fakeWAClient{"channel-busy": busy, "channel-idle": idle}

	infra := startGatewayInfra(t, "channel-busy")
	if err := infra.registry.Save(t.Context(), "channel-idle", types.NewJID("15550007777", types.DefaultUserServer).String(), "t"); err != nil {
		t.Fatalf("registry.Save: %v", err)
	}
	deps := gatewayDepsOn(t, infra, busy, "gateway-groups")
	if err := deps.Consumer.Close(); err != nil {
		t.Fatalf("close default consumer: %v", err)
	}
	consumer, err := gatewayamqp.NewConsumer(infra.conn, gatewayamqp.ConsumerConfig{Prefetch: 10, GroupPrefetch: 4, GroupLaneBacklog: 1})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	deps.Consumer = consumer
	deps.Manager = session.NewManager(func(channelID string, _ *types.JID) (session.WAClient, error) {
		return fakes[channelID], nil
	})
	cancel, runErrCh := startGroupGatewayWith(t, infra, deps)
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	events := probeEvents(t, infra.conn, gatewayamqp.GroupActionRoutingKey)

	busyGroup := createGroups(t, infra.conn, "channel-busy", 1)
	idleGroup := createGroups(t, infra.conn, "channel-idle", 1)
	busy.mu.Lock()
	busy.announceDelay = time.Second
	busy.mu.Unlock()

	const busyCommands = 6
	for i := range busyCommands {
		publishGroupCommand(t, infra.conn, gatewayamqp.GatewayGroupCommand{CommandID: "cmd-busy-" + strconv.Itoa(i), TenantID: "t", ChannelID: "channel-busy", Action: "lock", GroupJIDs: busyGroup})
	}
	publishGroupCommand(t, infra.conn, gatewayamqp.GatewayGroupCommand{CommandID: "cmd-idle", TenantID: "t", ChannelID: "channel-idle", Action: "lock", GroupJIDs: idleGroup})

	var busyOrder []string
	idleSeenAfter := -1
	for len(busyOrder) < busyCommands || idleSeenAfter < 0 {
		d := waitForDelivery(t, events, gatewayamqp.GroupActionRoutingKey, 30*time.Second)
		var evt gatewayamqp.GroupActionEvent
		if err := json.Unmarshal(d.Body, &evt); err != nil || !evt.OK {
			t.Fatalf("result %+v (%v)", evt, err)
		}
		if evt.ChannelID == "channel-idle" {
			idleSeenAfter = len(busyOrder)
			continue
		}
		busyOrder = append(busyOrder, evt.CommandID)
	}
	if idleSeenAfter >= busyCommands {
		t.Fatalf("the idle channel waited for every command of the busy one, busy order %v", busyOrder)
	}
	for i, commandID := range busyOrder {
		if commandID != "cmd-busy-"+strconv.Itoa(i) {
			t.Fatalf("the commands of one channel must keep their order through the spill, got %v", busyOrder)
		}
	}
}
