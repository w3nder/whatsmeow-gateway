package mapper_test

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
)

var testGroupStatusDeps = mapper.GroupStatusDeps{ChannelID: "channel-1", TenantID: "tenant-1"}

func TestBuildGroupStatusCarriesTheWholeBatchInOneEvent(t *testing.T) {
	evt := &events.Receipt{
		MessageSource: types.MessageSource{
			Chat:   types.NewJID("120363000000000000", types.GroupServer),
			Sender: types.NewJID("5511888887777", types.DefaultUserServer),
		},
		MessageIDs: []string{"A", "B", "C"},
		Type:       types.ReceiptTypeRead,
		Timestamp:  time.Unix(1700000000, 0),
	}

	out := mapper.BuildGroupStatus(testGroupStatusDeps, evt)
	if out == nil {
		t.Fatal("expected an event")
	}
	if len(out.MessageIDs) != 3 {
		t.Fatalf("expected 3 ids in one event, got %d", len(out.MessageIDs))
	}
	if out.Status != "read" {
		t.Fatalf("status = %q", out.Status)
	}
	if out.GroupJID != "120363000000000000@g.us" {
		t.Fatalf("group jid = %q", out.GroupJID)
	}
	if out.TenantID != "tenant-1" {
		t.Fatalf("tenant id = %q", out.TenantID)
	}
	if out.ChannelID != "channel-1" {
		t.Fatalf("channel id = %q", out.ChannelID)
	}
}

func TestBuildGroupStatusCollapsesTheDeviceSuffix(t *testing.T) {
	phone := types.NewJID("5511888887777", types.DefaultUserServer)
	web := phone
	web.Device = 12

	fromPhone := mapper.BuildGroupStatus(testGroupStatusDeps, &events.Receipt{
		MessageSource: types.MessageSource{Chat: types.NewJID("120363000000000000", types.GroupServer), Sender: phone},
		MessageIDs:    []string{"A"}, Type: types.ReceiptTypeRead, Timestamp: time.Unix(1700000000, 0),
	})
	fromWeb := mapper.BuildGroupStatus(testGroupStatusDeps, &events.Receipt{
		MessageSource: types.MessageSource{Chat: types.NewJID("120363000000000000", types.GroupServer), Sender: web},
		MessageIDs:    []string{"A"}, Type: types.ReceiptTypeRead, Timestamp: time.Unix(1700000000, 0),
	})

	if fromPhone.ParticipantJID != fromWeb.ParticipantJID {
		t.Fatalf("two devices of one person gave %q and %q", fromPhone.ParticipantJID, fromWeb.ParticipantJID)
	}
}

func TestBuildGroupStatusIgnoresAPrivateReceiptAndAnUnknownType(t *testing.T) {
	private := mapper.BuildGroupStatus(testGroupStatusDeps, &events.Receipt{
		MessageSource: types.MessageSource{Chat: types.NewJID("5511999998888", types.DefaultUserServer)},
		MessageIDs:    []string{"A"}, Type: types.ReceiptTypeRead,
	})
	if private != nil {
		t.Fatal("a private receipt must not become a group event")
	}

	unknown := mapper.BuildGroupStatus(testGroupStatusDeps, &events.Receipt{
		MessageSource: types.MessageSource{Chat: types.NewJID("120363000000000000", types.GroupServer)},
		MessageIDs:    []string{"A"}, Type: types.ReceiptTypeSender,
	})
	if unknown != nil {
		t.Fatal("a receipt with no delivered/read meaning must be dropped")
	}
}

func TestBuildGroupStatusJSONIsTheWireContractIngestionParses(t *testing.T) {
	deps := mapper.GroupStatusDeps{ChannelID: "22222222-2222-2222-2222-222222222222", TenantID: "11111111-1111-1111-1111-111111111111"}
	evt := &events.Receipt{
		MessageSource: types.MessageSource{
			Chat:   types.NewJID("120363000000000000", types.GroupServer),
			Sender: types.NewJID("5511888887777", types.DefaultUserServer),
		},
		MessageIDs: []string{"3EB0", "3EB1"},
		Type:       types.ReceiptTypeRead,
		Timestamp:  time.Unix(1700000000, 0),
	}

	out := mapper.BuildGroupStatus(deps, evt)
	if out == nil {
		t.Fatal("expected an event")
	}

	got, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	const wantJSON = `{
		"tenantId": "11111111-1111-1111-1111-111111111111",
		"channelId": "22222222-2222-2222-2222-222222222222",
		"groupJid": "120363000000000000@g.us",
		"participantJid": "5511888887777@s.whatsapp.net",
		"messageIds": ["3EB0", "3EB1"],
		"status": "read",
		"timestamp": "1700000000"
	}`

	var gotObj, wantObj map[string]interface{}
	if err := json.Unmarshal(got, &gotObj); err != nil {
		t.Fatalf("unmarshal got: %v", err)
	}
	if err := json.Unmarshal([]byte(wantJSON), &wantObj); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}

	if !reflect.DeepEqual(gotObj, wantObj) {
		t.Fatalf("whatsapp.group.status.v1 wire contract mismatch:\n got  %s\n want %s", got, wantJSON)
	}
}
