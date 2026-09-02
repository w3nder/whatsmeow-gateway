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

func TestPhaseStringUnknown(t *testing.T) {
	if got := call.Phase(99).String(); got != "unknown" {
		t.Errorf("Phase(99).String() = %q, want unknown", got)
	}
}

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

func TestEventRecordingCarriesEveryTrack(t *testing.T) {
	body, err := json.Marshal(call.Event{
		CallID:        "ABCDEF",
		Direction:     call.DirectionInbound,
		Type:          call.EventRecording,
		Timestamp:     "1754300100",
		Media:         &call.Media{Key: "calls/c1/ABCDEF.wav", MimeType: "audio/wav"},
		PeerMedia:     &call.Media{Key: "calls/c1/ABCDEF-peer.wav", MimeType: "audio/wav"},
		OperatorMedia: &call.Media{Key: "calls/c1/ABCDEF-operator.wav", MimeType: "audio/wav"},
		VideoMedia:    &call.Media{Key: "calls/c1/ABCDEF.h264", MimeType: "video/h264"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for field, wantKey := range map[string]string{
		"media":         "calls/c1/ABCDEF.wav",
		"peerMedia":     "calls/c1/ABCDEF-peer.wav",
		"operatorMedia": "calls/c1/ABCDEF-operator.wav",
		"videoMedia":    "calls/c1/ABCDEF.h264",
	} {
		got, ok := out[field].(map[string]any)
		if !ok || got["key"] != wantKey {
			t.Errorf("%s = %v, want key %s", field, out[field], wantKey)
		}
	}
	if _, exists := out["url"]; exists {
		t.Error("event must not carry a resolved URL, only the S3 key")
	}
}

func TestEventOmitsAbsentPerSideRecordings(t *testing.T) {
	body, err := json.Marshal(call.Event{
		CallID:    "ABCDEF",
		Direction: call.DirectionInbound,
		Type:      call.EventRecording,
		Timestamp: "1754300100",
		Media:     &call.Media{Key: "calls/c1/ABCDEF.wav", MimeType: "audio/wav"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, field := range []string{"peerMedia", "operatorMedia", "videoMedia"} {
		if _, exists := out[field]; exists {
			t.Errorf("%s is present but was nil, want it omitted", field)
		}
	}
}
