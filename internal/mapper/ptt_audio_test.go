package mapper

import (
	"bytes"
	"context"
	"encoding/base64"
	"testing"

	"go.mau.fi/whatsmeow"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

type pttStubUploader struct{}

func (pttStubUploader) Upload(ctx context.Context, data []byte, mt whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	return whatsmeow.UploadResponse{
		URL:           "https://mmg.whatsapp.net/stub",
		DirectPath:    "/v/stub",
		MediaKey:      []byte("media-key"),
		FileEncSHA256: []byte("enc-sha"),
		FileSHA256:    []byte("sha"),
		FileLength:    1234,
	}, nil
}

func pttStubFetch(ctx context.Context, url string) ([]byte, error) {
	return []byte("audio-bytes"), nil
}

func TestAudioBecomesPushToTalkWhenVoiceIsSet(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		Type: "audio",
		Media: &amqp.MediaPayload{
			URL:             "https://example.test/a.ogg",
			Mime:            "audio/ogg; codecs=opus",
			Voice:           true,
			Waveform:        base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{50}, 64)),
			DurationSeconds: 7,
		},
	}

	msg, err := buildMedia(context.Background(), pttStubUploader{}, pttStubFetch, cmd, whatsmeow.MediaAudio)
	if err != nil {
		t.Fatalf("buildMedia: %v", err)
	}
	audio := msg.GetAudioMessage()
	if !audio.GetPTT() {
		t.Fatal("expected PTT to be set")
	}
	if got := len(audio.GetWaveform()); got != 64 {
		t.Fatalf("waveform length = %d, want 64", got)
	}
	if audio.GetSeconds() != 7 {
		t.Fatalf("seconds = %d, want 7", audio.GetSeconds())
	}
}

func TestAudioStaysAPlainFileWhenVoiceIsAbsent(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		Type:  "audio",
		Media: &amqp.MediaPayload{URL: "https://example.test/a.ogg", Mime: "audio/ogg"},
	}

	msg, err := buildMedia(context.Background(), pttStubUploader{}, pttStubFetch, cmd, whatsmeow.MediaAudio)
	if err != nil {
		t.Fatalf("buildMedia: %v", err)
	}
	if msg.GetAudioMessage().GetPTT() {
		t.Fatal("expected a plain audio message")
	}
}

func TestMalformedWaveformDoesNotBreakTheSend(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		Type:  "audio",
		Media: &amqp.MediaPayload{URL: "https://example.test/a.ogg", Mime: "audio/ogg", Voice: true, Waveform: "nao-e-base64!!"},
	}

	msg, err := buildMedia(context.Background(), pttStubUploader{}, pttStubFetch, cmd, whatsmeow.MediaAudio)
	if err != nil {
		t.Fatalf("buildMedia: %v", err)
	}
	if !msg.GetAudioMessage().GetPTT() {
		t.Fatal("expected PTT even without a usable waveform")
	}
	if msg.GetAudioMessage().GetWaveform() != nil {
		t.Fatal("expected the bad waveform to be dropped")
	}
}
