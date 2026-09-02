package call_test

import (
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

func TestRegistryIsolatesChannels(t *testing.T) {
	r := call.NewRegistry()
	a := &call.Tracked{CallID: "C1", ChannelID: "chan-a"}
	b := &call.Tracked{CallID: "C1", ChannelID: "chan-b"}

	r.Insert(a)
	r.Insert(b)

	if got, ok := r.Get("chan-a", "C1"); !ok || got != a {
		t.Errorf("Get(chan-a, C1) = %v,%v, want the chan-a call", got, ok)
	}
	if got, ok := r.Get("chan-b", "C1"); !ok || got != b {
		t.Errorf("Get(chan-b, C1) = %v,%v, want the chan-b call", got, ok)
	}
}

func TestRegistryTakeChannelEmptiesIt(t *testing.T) {
	r := call.NewRegistry()
	r.Insert(&call.Tracked{CallID: "C1", ChannelID: "chan-a"})
	r.Insert(&call.Tracked{CallID: "C2", ChannelID: "chan-a"})
	r.Insert(&call.Tracked{CallID: "C3", ChannelID: "chan-b"})

	taken := r.TakeChannel("chan-a")
	if len(taken) != 2 {
		t.Fatalf("TakeChannel returned %d calls, want 2", len(taken))
	}
	if _, ok := r.Get("chan-a", "C1"); ok {
		t.Error("chan-a still has C1 after TakeChannel")
	}
	if _, ok := r.Get("chan-b", "C3"); !ok {
		t.Error("TakeChannel(chan-a) removed a call from chan-b")
	}
}

func TestRegistryRemoveIsIdempotent(t *testing.T) {
	r := call.NewRegistry()
	r.Insert(&call.Tracked{CallID: "C1", ChannelID: "chan-a"})

	if !r.Remove("chan-a", "C1") {
		t.Error("first Remove = false, want true")
	}
	if r.Remove("chan-a", "C1") {
		t.Error("second Remove = true, want false")
	}
}

func TestRegistryHoldsSeveralCallsPerChannel(t *testing.T) {
	r := call.NewRegistry()
	r.Insert(&call.Tracked{CallID: "C1", ChannelID: "chan-a"})
	r.Insert(&call.Tracked{CallID: "C2", ChannelID: "chan-a"})

	if _, ok := r.Get("chan-a", "C1"); !ok {
		t.Error("C1 is gone after inserting C2")
	}
	if _, ok := r.Get("chan-a", "C2"); !ok {
		t.Error("C2 was not registered")
	}
}
