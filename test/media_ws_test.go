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

// memCallPublisher and memCallStore let the test build a real call.Manager
// without a broker or an S3 bucket: the media server only cares that the
// manager has a live call registered on it, which neither dependency
// contributes to.
type memCallPublisher struct{}

func (memCallPublisher) PublishCall(context.Context, call.Event) error { return nil }

func (memCallPublisher) PublishInbound(context.Context, call.InboundCallEvent) error { return nil }

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
	return startMediaServerWithOrigins(t, nil)
}

// startMediaServerWithOrigins is startMediaServer with control over
// ServerConfig.AllowedOrigins, for the cross-origin cases.
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

// dialMedia hits the media route with the given call ID and token. The
// response is returned alongside the error because a refused handshake
// carries its status there -- Dial itself only ever returns a generic error.
func dialMedia(t *testing.T, ts *httptest.Server, callID, token string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	return dialMediaWithOrigin(t, ts, callID, token, "")
}

// dialMediaWithOrigin is dialMedia with an explicit Origin header, for the
// cross-origin cases. websocket.Dial itself never sets one (it is a Go
// client, not a browser), so a mismatched-origin request has to be built by
// hand.
func dialMediaWithOrigin(t *testing.T, ts *httptest.Server, callID, token, origin string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	url := "ws" + ts.URL[len("http"):] + "/calls/" + callID + "/media?token=" + token
	var opts *websocket.DialOptions
	if origin != "" {
		opts = &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{origin}}}
	}
	return websocket.Dial(context.Background(), url, opts)
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

	// Reading the attach-time keyframe frame first proves AttachStream has
	// already run server-side -- it is what queues that frame. Feeding audio
	// before that would race AttachStream: Track's sink only forwards a
	// frame once a stream is registered, so a frame fed in that gap is
	// dropped outright rather than merely delayed (see the ordering comment
	// on handleMedia in internal/media/server.go).
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

	// The source never blocks -- that is the whole point of it, since the
	// calling library's send loop reads it synchronously -- so a read that
	// beats the websocket message simply comes back as silence. Poll for the
	// operator's bytes rather than treating the first read as authoritative.
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

// TestMediaSocketForwardsAPeerKeyframeRequestMidCall is the mid-call twin of
// the attach-time test above: after the one-shot request that comes with
// attaching is drained, WhatsApp's own PLI/FIR feedback (wired in
// Manager.subscribe via OnVideoKeyframeRequest) must still reach the
// operator as a second FrameKeyframe.
func TestMediaSocketForwardsAPeerKeyframeRequestMidCall(t *testing.T) {
	ts, lc := startMediaServer(t)

	conn, resp, err := dialMedia(t, ts, "CALL1", signMediaToken(t, mediaClaims("CALL1")))
	if err != nil {
		t.Fatalf("Dial: %v (status %v)", err, resp)
	}
	defer func() { _ = conn.CloseNow() }()

	// Drain the attach-time request first so the read below can only pass
	// because of fireVideoKeyframeRequest.
	readFrameOfKind(t, conn, media.FrameKeyframe, 5*time.Second)

	lc.fireVideoKeyframeRequest()

	payload := readFrameOfKind(t, conn, media.FrameKeyframe, 5*time.Second)
	if len(payload) != 0 {
		t.Errorf("keyframe payload = % x, want none", payload)
	}
}

// TestMediaSocketAcceptsALargeVideoFrameFromTheOperator guards against
// coder/websocket's default 32 KiB read limit, which is well under a
// realistic H.264 IDR from a webcam (30-100 KB): without an explicit
// SetReadLimit, the operator's very first keyframe would exceed it and tear
// the whole connection down instead of just being read.
func TestMediaSocketAcceptsALargeVideoFrameFromTheOperator(t *testing.T) {
	ts, lc := startMediaServer(t)

	conn, resp, err := dialMedia(t, ts, "CALL1", signMediaToken(t, mediaClaims("CALL1")))
	if err != nil {
		t.Fatalf("Dial: %v (status %v)", err, resp)
	}
	defer func() { _ = conn.CloseNow() }()

	big := make([]byte, 64*1024) // above the library's 32 KiB default
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

// TestMediaSocketRejectsADisallowedOrigin is the default-closed side of I3:
// with no AllowedOrigins configured, a cross-origin request is refused before
// the upgrade, same as any other rejection here.
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

// TestMediaSocketAllowsAConfiguredCrossOrigin proves the fix side of I3: the
// operator is a browser tab served from the frontend's own origin, not this
// listener's, so a real deployment has to be able to allow it explicitly.
func TestMediaSocketAllowsAConfiguredCrossOrigin(t *testing.T) {
	ts, _ := startMediaServerWithOrigins(t, []string{"operator.example.com"})

	token := signMediaToken(t, mediaClaims("CALL1"))
	conn, resp, err := dialMediaWithOrigin(t, ts, "CALL1", token, "http://operator.example.com")
	if err != nil {
		t.Fatalf("Dial: %v (status %v)", err, resp)
	}
	defer func() { _ = conn.CloseNow() }()

	// A successful read of any frame proves the handshake actually went
	// through, not just that Dial returned without error.
	readFrameOfKind(t, conn, media.FrameKeyframe, 5*time.Second)
}

// TestMediaSocketRejectedOriginDoesNotDisturbTheIncumbentOperator is the
// regression test for the sharpest edge of I3: AttachStream replaces and
// closes whatever stream is already live, so if it ran before the upgrade, a
// second connection that fails the Origin check would still have kicked the
// first operator off the call. It must not: attaching only happens once a
// handshake actually succeeds.
func TestMediaSocketRejectedOriginDoesNotDisturbTheIncumbentOperator(t *testing.T) {
	ts, lc := startMediaServer(t)

	incumbent, resp, err := dialMedia(t, ts, "CALL1", signMediaToken(t, mediaClaims("CALL1")))
	if err != nil {
		t.Fatalf("Dial (incumbent): %v (status %v)", err, resp)
	}
	defer func() { _ = incumbent.CloseNow() }()

	// Drain the attach-time keyframe request so the frame read after the
	// rejected connection below can only be the audio fed at that point.
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
