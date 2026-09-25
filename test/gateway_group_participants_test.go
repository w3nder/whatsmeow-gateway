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
