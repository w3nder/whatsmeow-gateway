package media

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// setTestTimeouts shrinks the write/ping windows for a test and restores the
// production values afterward. The real windows (10s / 15s) are far too long
// for a test to wait out; shrinking the package vars, rather than adding a
// server-config knob nothing in production should ever tune, keeps the
// production behaviour exactly what ops sees while still letting a test
// observe the give-up-on-a-silent-peer path deterministically.
func setTestTimeouts(t *testing.T, write, ping time.Duration) {
	t.Helper()
	oldWrite, oldPing := writeTimeout, pingInterval
	writeTimeout, pingInterval = write, ping
	t.Cleanup(func() { writeTimeout, pingInterval = oldWrite, oldPing })
}

// newTestConnPair returns both ends of one real websocket connection over
// loopback TCP, both left open (no close, no read loop) for the test to drive
// directly. The server handler blocks on doneCh rather than returning, so the
// hijacked connection and its accepted *websocket.Conn stay alive for the
// life of the test.
func newTestConnPair(t *testing.T) (server, client *websocket.Conn) {
	t.Helper()

	accepted := make(chan *websocket.Conn, 1)
	doneCh := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("Accept: %v", err)
			close(accepted)
			return
		}
		accepted <- c
		<-doneCh
	}))
	t.Cleanup(func() {
		close(doneCh)
		ts.Close()
	})

	url := "ws" + ts.URL[len("http"):]
	c, _, err := websocket.Dial(context.Background(), url, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.CloseNow() })

	select {
	case sc, ok := <-accepted:
		if !ok {
			t.Fatal("server never accepted the connection")
		}
		return sc, c
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the server to accept")
		return nil, nil
	}
}

// TestPumpPingClosesASilentConnection is the direct regression test for I2: a
// client that holds the TCP connection open but never drives its own read
// loop (the "vanished without a FIN" case -- coder/websocket's Ping requires
// a concurrent Reader on the peer to ever see a pong) must eventually get its
// connection force-closed by the ping mechanism, not wait forever. It also
// proves that force-close is what unwinds a Read blocked on the very same
// conn -- the exact deadlock pumpOperator/pumpPeer would otherwise hit,
// since prior to this fix neither pump's context was ever cancelled except
// by the other.
func TestPumpPingClosesASilentConnection(t *testing.T) {
	setTestTimeouts(t, 30*time.Millisecond, 30*time.Millisecond)

	serverConn, _ := newTestConnPair(t)
	// The client end is deliberately left completely idle: no Read, no
	// Write, no Close. That is what makes it silent rather than merely slow.

	ctx, cancel := context.WithCancel(context.Background())
	s := &server{log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	readErrCh := make(chan error, 1)
	go func() {
		_, _, err := serverConn.Read(ctx)
		readErrCh <- err
	}()

	pingDone := make(chan struct{})
	go func() {
		s.pumpPing(ctx, cancel, serverConn)
		close(pingDone)
	}()

	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("pumpPing never gave up on a connection that never answers a ping")
	}
	<-pingDone

	select {
	case err := <-readErrCh:
		if err == nil {
			t.Error("Read returned nil, want the force-close to have unblocked it with an error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a concurrent Read on the same conn was never unblocked by the force-close")
	}

	if err := serverConn.Write(context.Background(), websocket.MessageBinary, []byte("x")); err == nil {
		t.Error("Write succeeded after the ping gave up, want the connection to already be closed")
	}
}

// TestWriteTimesOutOnAnUndrainedConnection is the narrower unit for the other
// half of I2: a write big enough to actually block on TCP flow control (the
// peer never reads, so the kernel send buffer fills) must not hang past
// writeTimeout. A single several-MB write reliably exceeds a loopback
// socket's send buffer, unlike a small frame, which the kernel just
// absorbs -- so this needs no retry loop to become a real blocking write.
func TestWriteTimesOutOnAnUndrainedConnection(t *testing.T) {
	setTestTimeouts(t, 50*time.Millisecond, time.Hour) // ping out of the way

	serverConn, clientConn := newTestConnPair(t)
	_ = clientConn // never read: nothing ever drains the socket buffer

	s := &server{log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	huge := make([]byte, 8<<20) // 8 MiB, well past any default loopback buffer

	errCh := make(chan error, 1)
	go func() { errCh <- s.write(context.Background(), serverConn, FrameVideo, huge) }()

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("write succeeded, want it bounded by writeTimeout on an undrained connection")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write blocked well past writeTimeout instead of being bounded by it")
	}
}
