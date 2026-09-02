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

func setTestTimeouts(t *testing.T, write, ping time.Duration) {
	t.Helper()
	oldWrite, oldPing := writeTimeout, pingInterval
	writeTimeout, pingInterval = write, ping
	t.Cleanup(func() { writeTimeout, pingInterval = oldWrite, oldPing })
}

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

func TestPumpPingClosesASilentConnection(t *testing.T) {
	setTestTimeouts(t, 30*time.Millisecond, 30*time.Millisecond)

	serverConn, _ := newTestConnPair(t)

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

func TestWriteTimesOutOnAnUndrainedConnection(t *testing.T) {
	setTestTimeouts(t, 50*time.Millisecond, time.Hour)

	serverConn, clientConn := newTestConnPair(t)
	_ = clientConn

	s := &server{log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	huge := make([]byte, 8<<20)

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
