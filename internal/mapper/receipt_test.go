package mapper_test

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
)

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

	out := mapper.BuildGroupStatus(evt)
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
}

func TestBuildGroupStatusCollapsesTheDeviceSuffix(t *testing.T) {
	phone := types.NewJID("5511888887777", types.DefaultUserServer)
	web := phone
	web.Device = 12

	fromPhone := mapper.BuildGroupStatus(&events.Receipt{
		MessageSource: types.MessageSource{Chat: types.NewJID("120363000000000000", types.GroupServer), Sender: phone},
		MessageIDs:    []string{"A"}, Type: types.ReceiptTypeRead, Timestamp: time.Unix(1700000000, 0),
	})
	fromWeb := mapper.BuildGroupStatus(&events.Receipt{
		MessageSource: types.MessageSource{Chat: types.NewJID("120363000000000000", types.GroupServer), Sender: web},
		MessageIDs:    []string{"A"}, Type: types.ReceiptTypeRead, Timestamp: time.Unix(1700000000, 0),
	})

	if fromPhone.ParticipantJID != fromWeb.ParticipantJID {
		t.Fatalf("two devices of one person gave %q and %q", fromPhone.ParticipantJID, fromWeb.ParticipantJID)
	}
}

func TestBuildGroupStatusIgnoresAPrivateReceiptAndAnUnknownType(t *testing.T) {
	private := mapper.BuildGroupStatus(&events.Receipt{
		MessageSource: types.MessageSource{Chat: types.NewJID("5511999998888", types.DefaultUserServer)},
		MessageIDs:    []string{"A"}, Type: types.ReceiptTypeRead,
	})
	if private != nil {
		t.Fatal("a private receipt must not become a group event")
	}

	unknown := mapper.BuildGroupStatus(&events.Receipt{
		MessageSource: types.MessageSource{Chat: types.NewJID("120363000000000000", types.GroupServer)},
		MessageIDs:    []string{"A"}, Type: types.ReceiptTypeSender,
	})
	if unknown != nil {
		t.Fatal("a receipt with no delivered/read meaning must be dropped")
	}
}
