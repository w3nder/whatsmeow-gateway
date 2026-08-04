package call_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

func TestInboundCallEventShape(t *testing.T) {
	evt := call.NewInboundCallEvent(call.Identity{PhoneNumberID: "5511999999999", TenantID: "t1"},
		"chan-a", "CALL1", "", "5511888888888", call.DirectionInbound, true, "1754300000")

	body, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out["type"] != "call" {
		t.Errorf("type = %v, want call", out["type"])
	}
	if out["providerMessageId"] != "CALL1" {
		t.Errorf("providerMessageId = %v, want the call id", out["providerMessageId"])
	}
	rich, ok := out["richContent"].(map[string]any)
	if !ok {
		t.Fatalf("richContent = %v, want an object", out["richContent"])
	}
	if rich["kind"] != "call" || rich["state"] != "ringing" || rich["direction"] != "inbound" {
		t.Errorf("richContent = %v, want a ringing inbound call", rich)
	}
	if rich["isVideo"] != true {
		t.Errorf("isVideo = %v, want true", rich["isVideo"])
	}
}

// The chat message and the call event must carry the same id, or the backend
// cannot correlate the state updates with the message it created.
func TestInboundCallEventUsesTheCallIDAsProviderMessageID(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	caller.fireIncoming(&fakeCall{id: "CALL1", peer: "5511888888888@s.whatsapp.net"})

	inbound := pub.inboundEvents()
	if len(inbound) != 1 {
		t.Fatalf("got %d inbound events, want 1", len(inbound))
	}
	if inbound[0].ProviderMessageID != "CALL1" {
		t.Errorf("providerMessageId = %q, want CALL1", inbound[0].ProviderMessageID)
	}
}
