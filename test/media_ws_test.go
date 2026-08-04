package test

import (
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

// memCallPublisher and memCallStore let the test build a real call.Manager
// without a broker or an S3 bucket: the media server only cares that the
// manager has a live call registered on it, which neither dependency
// contributes to.
type memCallPublisher struct{}

func (memCallPublisher) PublishCall(context.Context, call.Event) error { return nil }

type memCallStore struct{}

func (memCallStore) PutStream(context.Context, string, string, io.Reader) error { return nil }

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startMediaServer wires a real call.Manager, with one fake call already live
// on it, behind an httptest server driven by media.NewServer's own handler --
// the same construction cmd/gateway/main.go uses, minus ListenAndServe.
func startMediaServer(t *testing.T) (*httptest.Server, *fakeLiveCall) {
	t.Helper()

	m := call.NewManager(
		memCallPublisher{},
		memCallStore{},
		func(string) call.Identity { return call.Identity{TenantID: "tenant-1"} },
		call.Options{TmpDir: t.TempDir()},
		discardLogger(),
	)

	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeLiveCall{callID: "CALL1", peer: "5511888888888"}
	caller.fireIncoming(lc)

	srv := media.NewServer(m, media.ServerConfig{Secret: mediaTestSecret}, discardLogger())
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

// dialMedia hits the media route with the given call ID and token. The
// response is returned alongside the error because a refused handshake
// carries its status there -- Dial itself only ever returns a generic error.
func dialMedia(t *testing.T, ts *httptest.Server, callID, token string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	url := "ws" + ts.URL[len("http"):] + "/calls/" + callID + "/media?token=" + token
	return websocket.Dial(context.Background(), url, nil)
}

// readFrameOfKind reads frames off conn until one of the wanted kind arrives,
// skipping others (e.g. the keyframe request every attach sends up front).
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

// TestMediaSocketRejectsATokenForAnotherCall is the security-critical case:
// a signed, unexpired token minted for a different call must be refused with
// 403 before any upgrade happens -- an accepted-then-closed handshake would
// already have told the caller CALL1 exists.
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

	waitFor(t, 5*time.Second, "the operator's audio to reach Play", func() bool {
		return lc.playedSrc() != nil
	})

	got := make([]byte, len(sent))
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(lc.playedSrc(), got)
		readDone <- err
	}()

	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("read back operator audio: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out reading back the operator's audio from Play's source")
	}

	if string(got) != string(sent) {
		t.Errorf("Play read %x, want %x", got, sent)
	}
}

// TestMediaSocketForwardsAKeyframeRequest exercises the signal an operator
// needs to decode video mid-call: attaching mid-stream has missed every prior
// keyframe, so the stream requests one immediately, and the server has to
// carry that request across the socket with no payload of its own.
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
