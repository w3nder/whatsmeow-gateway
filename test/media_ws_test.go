package test

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/golang-jwt/jwt/v5"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
	"github.com/w3nder/whatsmeow-gateway/internal/media"
)

const mediaTestSecret = "media-test-secret"

type memCallPublisher struct{}

func (memCallPublisher) PublishCall(context.Context, call.Event) error { return nil }

func (memCallPublisher) PublishInbound(context.Context, call.InboundCallEvent) error { return nil }

type memCallStore struct{}

func (memCallStore) PutStream(context.Context, string, string, io.Reader) error { return nil }

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func startMediaServer(t *testing.T) (*httptest.Server, *fakeLiveCall) {
	t.Helper()
	return startMediaServerWithOrigins(t, nil)
}

func startMediaServerWithOrigins(t *testing.T, origins []string) (*httptest.Server, *fakeLiveCall) {
	t.Helper()

	m := call.NewManager(
		memCallPublisher{},
		memCallStore{},
		func(string) call.Identity { return call.Identity{TenantID: "tenant-1"} },
		nil,
		nil,
		call.Options{TmpDir: t.TempDir()},
		discardLogger(),
	)

	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeLiveCall{callID: "CALL1", peer: "5511888888888"}
	caller.fireIncoming(lc)

	srv := media.NewServer(m, media.ServerConfig{Secret: mediaTestSecret, AllowedOrigins: origins}, discardLogger())
	ts := httptest.NewServer(srv.Handler)
	t.Cleanup(ts.Close)

	return ts, lc
}

func signMediaToken(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(mediaTestSecret))
	if err != nil {
		t.Fatalf("sign media token: %v", err)
	}
	return token
}

func mediaClaims(callID string) jwt.MapClaims {
	return jwt.MapClaims{
		"tenantId":  "tenant-1",
		"channelId": "chan-a",
		"callId":    callID,
		"userId":    "user-1",
		"exp":       time.Now().Add(5 * time.Minute).Unix(),
	}
}

func dialMedia(t *testing.T, ts *httptest.Server, callID, token string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	return dialMediaWithOrigin(t, ts, callID, token, "")
}

func dialMediaWithOrigin(t *testing.T, ts *httptest.Server, callID, token, origin string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	url := "ws" + ts.URL[len("http"):] + "/calls/" + callID + "/media?token=" + token
	var opts *websocket.DialOptions
	if origin != "" {
		opts = &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{origin}}}
	}
	return websocket.Dial(context.Background(), url, opts)
}

func readFrameOfKind(t *testing.T, conn *websocket.Conn, kind byte, timeout time.Duration) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		_, raw, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		gotKind, payload, err := media.DecodeFrame(raw)
		if err != nil {
			t.Fatalf("DecodeFrame: %v", err)
		}
		if gotKind == kind {
			return payload
		}
	}
}

func TestMediaSocketRejectsATokenForAnotherCall(t *testing.T) {
	ts, _ := startMediaServer(t)

	token := signMediaToken(t, mediaClaims("SOME_OTHER_CALL"))
	conn, resp, err := dialMedia(t, ts, "CALL1", token)
	if err == nil {
		_ = conn.CloseNow()
		t.Fatal("Dial succeeded, want the handshake refused for a mismatched call")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		status := "<nil response>"
		if resp != nil {
			status = resp.Status
		}
		t.Fatalf("status = %s, want %d", status, http.StatusForbidden)
	}
}

func TestMediaSocketRejectsAnExpiredToken(t *testing.T) {
	ts, _ := startMediaServer(t)

	claims := mediaClaims("CALL1")
	claims["exp"] = time.Now().Add(-time.Minute).Unix()
	token := signMediaToken(t, claims)

	conn, resp, err := dialMedia(t, ts, "CALL1", token)
	if err == nil {
		_ = conn.CloseNow()
		t.Fatal("Dial succeeded, want the handshake refused for an expired token")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		status := "<nil response>"
		if resp != nil {
			status = resp.Status
		}
		t.Fatalf("status = %s, want %d", status, http.StatusUnauthorized)
	}
}

func TestMediaSocketDeliversPeerAudioToTheOperator(t *testing.T) {
	ts, lc := startMediaServer(t)

	conn, resp, err := dialMedia(t, ts, "CALL1", signMediaToken(t, mediaClaims("CALL1")))
	if err != nil {
		t.Fatalf("Dial: %v (status %v)", err, resp)
	}
	defer func() { _ = conn.CloseNow() }()

	readFrameOfKind(t, conn, media.FrameKeyframe, 5*time.Second)

	lc.feedAudio([]float32{0.5, -0.5})

	payload := readFrameOfKind(t, conn, media.FrameAudio, 5*time.Second)
	if len(payload) != 4 {
		t.Fatalf("payload len = %d, want 4 bytes for two s16le samples", len(payload))
	}

	got0 := int16(binary.LittleEndian.Uint16(payload[0:2]))
	got1 := int16(binary.LittleEndian.Uint16(payload[2:4]))
	if abs16(got0-16383) > 1 || abs16(got1+16383) > 1 {
		t.Errorf("samples = [%d %d], want approximately [16383 -16383]", got0, got1)
	}
}

func abs16(v int16) int16 {
	if v < 0 {
		return -v
	}
	return v
}

func TestMediaSocketFeedsOperatorAudioIntoTheCall(t *testing.T) {
	ts, lc := startMediaServer(t)

	conn, resp, err := dialMedia(t, ts, "CALL1", signMediaToken(t, mediaClaims("CALL1")))
	if err != nil {
		t.Fatalf("Dial: %v (status %v)", err, resp)
	}
	defer func() { _ = conn.CloseNow() }()

	sent := []byte{0x00, 0x40, 0x00, 0xC0}
	if err := conn.Write(context.Background(), websocket.MessageBinary, media.EncodeFrame(media.FrameAudio, sent)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	src := lc.playedSrc()
	if src == nil {
		t.Fatal("the call has no outbound audio source")
	}

	var got []byte
	waitFor(t, 5*time.Second, "the operator's audio to reach the call's outbound source", func() bool {
		frame := make([]byte, len(sent))
		if _, err := io.ReadFull(src, frame); err != nil {
			t.Fatalf("read back operator audio: %v", err)
		}
		if bytes.Equal(frame, sent) {
			got = frame
			return true
		}
		return false
	})

	if !bytes.Equal(got, sent) {
		t.Errorf("the call's outbound audio = %x, want %x", got, sent)
	}
}

func TestMediaSocketForwardsAKeyframeRequest(t *testing.T) {
	ts, _ := startMediaServer(t)

	conn, resp, err := dialMedia(t, ts, "CALL1", signMediaToken(t, mediaClaims("CALL1")))
	if err != nil {
		t.Fatalf("Dial: %v (status %v)", err, resp)
	}
	defer func() { _ = conn.CloseNow() }()

	payload := readFrameOfKind(t, conn, media.FrameKeyframe, 5*time.Second)
	if len(payload) != 0 {
		t.Errorf("keyframe payload = % x, want none", payload)
	}
}

func TestMediaSocketForwardsAPeerKeyframeRequestMidCall(t *testing.T) {
	ts, lc := startMediaServer(t)

	conn, resp, err := dialMedia(t, ts, "CALL1", signMediaToken(t, mediaClaims("CALL1")))
	if err != nil {
		t.Fatalf("Dial: %v (status %v)", err, resp)
	}
	defer func() { _ = conn.CloseNow() }()

	readFrameOfKind(t, conn, media.FrameKeyframe, 5*time.Second)

	lc.fireVideoKeyframeRequest()

	payload := readFrameOfKind(t, conn, media.FrameKeyframe, 5*time.Second)
	if len(payload) != 0 {
		t.Errorf("keyframe payload = % x, want none", payload)
	}
}

func TestMediaSocketAcceptsALargeVideoFrameFromTheOperator(t *testing.T) {
	ts, lc := startMediaServer(t)

	conn, resp, err := dialMedia(t, ts, "CALL1", signMediaToken(t, mediaClaims("CALL1")))
	if err != nil {
		t.Fatalf("Dial: %v (status %v)", err, resp)
	}
	defer func() { _ = conn.CloseNow() }()

	big := make([]byte, 64*1024)
	for i := range big {
		big[i] = byte(i)
	}
	if err := conn.Write(context.Background(), websocket.MessageBinary, media.EncodeFrame(media.FrameVideo, big)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	waitFor(t, 5*time.Second, "the large video frame to reach SendVideo", func() bool {
		for _, action := range lc.recordedActions() {
			if action == "video.send" {
				return true
			}
		}
		return false
	})
}

func TestMediaSocketRejectsADisallowedOrigin(t *testing.T) {
	ts, _ := startMediaServer(t)

	token := signMediaToken(t, mediaClaims("CALL1"))
	conn, resp, err := dialMediaWithOrigin(t, ts, "CALL1", token, "http://evil.example")
	if err == nil {
		_ = conn.CloseNow()
		t.Fatal("Dial succeeded, want the handshake refused for a disallowed Origin")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		status := "<nil response>"
		if resp != nil {
			status = resp.Status
		}
		t.Fatalf("status = %s, want %d", status, http.StatusForbidden)
	}
}

func TestMediaSocketAllowsAConfiguredCrossOrigin(t *testing.T) {
	ts, _ := startMediaServerWithOrigins(t, []string{"operator.example.com"})

	token := signMediaToken(t, mediaClaims("CALL1"))
	conn, resp, err := dialMediaWithOrigin(t, ts, "CALL1", token, "http://operator.example.com")
	if err != nil {
		t.Fatalf("Dial: %v (status %v)", err, resp)
	}
	defer func() { _ = conn.CloseNow() }()

	readFrameOfKind(t, conn, media.FrameKeyframe, 5*time.Second)
}

func TestMediaSocketRejectedOriginDoesNotDisturbTheIncumbentOperator(t *testing.T) {
	ts, lc := startMediaServer(t)

	incumbent, resp, err := dialMedia(t, ts, "CALL1", signMediaToken(t, mediaClaims("CALL1")))
	if err != nil {
		t.Fatalf("Dial (incumbent): %v (status %v)", err, resp)
	}
	defer func() { _ = incumbent.CloseNow() }()

	readFrameOfKind(t, incumbent, media.FrameKeyframe, 5*time.Second)

	rejected, rejResp, err := dialMediaWithOrigin(t, ts, "CALL1", signMediaToken(t, mediaClaims("CALL1")), "http://evil.example")
	if err == nil {
		_ = rejected.CloseNow()
		t.Fatal("Dial (rejected) succeeded, want the handshake refused for a disallowed Origin")
	}
	if rejResp == nil || rejResp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %v, want %d", rejResp, http.StatusForbidden)
	}

	lc.feedAudio([]float32{0.5, -0.5})

	payload := readFrameOfKind(t, incumbent, media.FrameAudio, 5*time.Second)
	if len(payload) != 4 {
		t.Fatalf("incumbent got no audio after the rejected connection, want it undisturbed (payload len = %d)", len(payload))
	}
}
