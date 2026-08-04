package call_test

import (
	"encoding/json"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

func TestPhaseString(t *testing.T) {
	cases := map[call.Phase]string{
		call.PhaseIdle:        "idle",
		call.PhaseCalling:     "calling",
		call.PhaseRinging:     "ringing",
		call.PhaseConnecting:  "connecting",
		call.PhaseActive:      "active",
		call.PhaseEnded:       "ended",
		call.PhaseWaitingRoom: "waiting_room",
	}
	for phase, want := range cases {
		if got := phase.String(); got != want {
			t.Errorf("Phase(%d).String() = %q, want %q", phase, got, want)
		}
	}
}

// A phase the library gains later must not crash a channel.
func TestPhaseStringUnknown(t *testing.T) {
	if got := call.Phase(99).String(); got != "unknown" {
		t.Errorf("Phase(99).String() = %q, want unknown", got)
	}
}

// The event JSON is the backend's contract. Absent fields must stay absent so a
// consumer can tell "no recording" from "empty recording".
func TestEventOmitsAbsentFields(t *testing.T) {
	body, err := json.Marshal(call.Event{
		PhoneNumberID: "5511999999999",
		TenantID:      "t1",
		ChannelID:     "c1",
		CallID:        "ABCDEF",
		Direction:     call.DirectionInbound,
		Type:          call.EventIncoming,
		Timestamp:     "1754300000",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(body)
	want := `{"phoneNumberId":"5511999999999","tenantId":"t1","channelId":"c1",` +
		`"callId":"ABCDEF","direction":"inbound","type":"incoming","timestamp":"1754300000"}`
	if got != want {
		t.Errorf("marshal =\n%s\nwant\n%s", got, want)
	}
}

func TestEventEndedCarriesBothRecordings(t *testing.T) {
	body, err := json.Marshal(call.Event{
		CallID:     "ABCDEF",
		Direction:  call.DirectionInbound,
		Type:       call.EventEnded,
		Timestamp:  "1754300100",
		Duration:   100,
		Media:      &call.Media{Key: "calls/c1/ABCDEF.wav", MimeType: "audio/wav"},
		VideoMedia: &call.Media{Key: "calls/c1/ABCDEF.h264", MimeType: "video/h264"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	media, ok := out["media"].(map[string]any)
	if !ok || media["key"] != "calls/c1/ABCDEF.wav" {
		t.Errorf("media = %v, want the wav key", out["media"])
	}
	videoMedia, ok := out["videoMedia"].(map[string]any)
	if !ok || videoMedia["key"] != "calls/c1/ABCDEF.h264" {
		t.Errorf("videoMedia = %v, want the h264 key", out["videoMedia"])
	}
	if _, exists := out["url"]; exists {
		t.Error("event must not carry a resolved URL, only the S3 key")
	}
}
