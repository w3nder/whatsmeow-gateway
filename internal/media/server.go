package media

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

// maxOperatorFrameBytes bounds one frame from the operator. coder/websocket's
// own default (32 KiB) is well under a realistic H.264 IDR from a webcam
// (30-100 KB): the operator's very first keyframe would exceed it and the
// library tears the whole connection down rather than dropping the message.
// 1 MiB leaves headroom above that without opening the door to a frame large
// enough to matter for memory.
const maxOperatorFrameBytes = 1 << 20

// writeTimeout and pingInterval are vars, not consts, purely so
// server_internal_test.go can shrink them for a test -- production never
// changes them.
//
// writeTimeout bounds every write to the operator, including pings. A
// vanished client with no FIN otherwise blocks a write forever:
// coder/websocket arms a close-the-whole-connection callback off the context
// passed to Write, so giving each write its own short-lived context turns a
// wedged socket into a closed one, which is what unblocks pumpOperator's Read
// on the same conn.
//
// pingInterval keeps traffic flowing on an otherwise-silent connection (a
// quiet call, an operator only listening) so a vanished peer is still caught
// by writeTimeout instead of waiting on TCP to notice on its own.
var (
	writeTimeout = 10 * time.Second
	pingInterval = 15 * time.Second
)

// ServerConfig configures the media websocket listener.
type ServerConfig struct {
	Addr   string
	Secret string
	// AllowedOrigins authorises cross-origin upgrades, passed straight through
	// to websocket.Accept's OriginPatterns. The operator is a browser tab
	// served from the frontend's origin, not this server's, so without this
	// every real upgrade fails coder/websocket's default same-origin check.
	// Nil means only same-origin requests (and requests with no Origin header
	// at all, e.g. non-browser clients) are accepted.
	AllowedOrigins []string
}

// server exposes one call's live media over a websocket, authenticated by a
// short-lived token instead of session cookies: the operator connecting here
// is a browser tab, not a service-to-service caller.
type server struct {
	calls          *call.Manager
	secret         string
	allowedOrigins []string
	log            *slog.Logger
}

// NewServer builds the media HTTP server. It does not start listening --
// callers run it (e.g. via ListenAndServe) and stop it via Shutdown, same as
// any other http.Server.
func NewServer(calls *call.Manager, cfg ServerConfig, log *slog.Logger) *http.Server {
	s := &server{calls: calls, secret: cfg.Secret, allowedOrigins: cfg.AllowedOrigins, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /calls/{callID}/media", s.handleMedia)

	// This is the one listener in the process a browser reaches directly, and
	// the token check only runs once a full request has arrived. Without these
	// timeouts an unauthenticated client can pin a socket indefinitely by
	// dribbling headers, or by connecting and never speaking at all.
	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// handleMedia authenticates and authorises the request, then upgrades. Every
// rejection down to and including the existence check returns a plain HTTP
// error and stops before websocket.Accept: once the handshake completes, the
// caller already knows the call exists, so nothing that must stay secret can
// be checked after it.
//
// The existence check uses Get, not AttachStream: attaching replaces and
// closes whatever stream is already live on the call, and websocket.Accept
// can still fail after this point (a disallowed Origin, a dead client
// aborting mid-handshake). A rejected upgrade must not be able to knock a
// connected operator off the call, so the replace-and-close only happens
// once the handshake has actually succeeded.
func (s *server) handleMedia(w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("callID")

	claims, err := VerifyCallToken(r.URL.Query().Get("token"), s.secret)
	if err != nil {
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}

	// A token is minted for one call. Without this check any operator holding a
	// valid token could open the media of every other call on the instance.
	if claims.CallID != callID {
		http.Error(w, "token does not match this call", http.StatusForbidden)
		return
	}

	if _, ok := s.calls.Get(claims.ChannelID, claims.CallID); !ok {
		http.Error(w, "call not found", http.StatusNotFound)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: s.allowedOrigins})
	if err != nil {
		s.log.Error("media: websocket upgrade", "call_id", callID, "error", err)
		return
	}
	defer func() { _ = conn.CloseNow() }()

	// See the maxOperatorFrameBytes comment: the library's own default is
	// sized for text/JSON traffic, not a video access unit.
	conn.SetReadLimit(maxOperatorFrameBytes)

	// The call may have ended in the gap between the existence check above
	// and the handshake finishing. That is an ordinary race, not a new
	// disclosure -- the caller already knows the call existed a moment ago --
	// so it is reported by simply closing rather than a further HTTP error,
	// which is no longer possible after Accept.
	//
	// Attaching only here, rather than before Accept, is what keeps a
	// rejected upgrade from disturbing an already-attached operator (see the
	// handleMedia doc comment) -- but it also means a peer frame arriving in
	// the gap between Accept returning and this call finishing is dropped
	// rather than queued, since Track's fan-out only forwards to a
	// registered stream. That gap is a handful of in-process operations
	// (SetReadLimit, a map lookup), nanoseconds against a ~20ms audio
	// cadence, so it is accepted as a trade-off rather than engineered away.
	stream, detach, ok := s.calls.AttachStream(claims.ChannelID, claims.CallID)
	if !ok {
		_ = conn.Close(websocket.StatusNormalClosure, "call ended")
		return
	}
	defer detach()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// pumpOperator and pumpPing both cancel ctx when they return, which is
	// what tells pumpPeer below to stop writing to a connection that is going
	// away. pumpPeer itself never cancels ctx directly: its own write
	// failures already imply the conn is dead (see the writeTimeout comment),
	// so returning is enough -- the deferred cancel above covers it once
	// handleMedia unwinds.
	go s.pumpOperator(ctx, cancel, conn, stream)
	go s.pumpPing(ctx, cancel, conn)
	s.pumpPeer(ctx, conn, stream)
}

// pumpOperator reads frames the operator sends -- their microphone and
// camera -- and feeds them into the call. It owns the read side of the
// socket for the life of the connection, so it is also where a closed or
// broken socket is first noticed.
func (s *server) pumpOperator(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn, stream *call.Stream) {
	defer cancel()

	for {
		_, raw, err := conn.Read(ctx)
		if err != nil {
			return
		}

		kind, payload, err := DecodeFrame(raw)
		if err != nil {
			s.log.Warn("media: dropped an unreadable operator frame", "error", err)
			continue
		}

		switch kind {
		case FrameAudio:
			if err := stream.WriteAudio(payload); err != nil {
				s.log.Warn("media: write operator audio", "error", err)
			}
		case FrameVideo:
			if err := stream.WriteVideo(payload); err != nil {
				s.log.Warn("media: write operator video", "error", err)
			}
		default:
			s.log.Warn("media: dropped an operator frame of an unhandled kind", "kind", kind)
		}
	}
}

// pumpPing keeps a silent connection honest. A call with nothing flowing --
// no peer audio, an operator only listening -- never touches the write path
// otherwise, so a vanished client with no FIN would sit undetected until TCP
// eventually gives up, hours later. Ping shares writeTimeout's bounded
// context, so a dead peer gets caught here the same way a stalled media write
// does.
func (s *server) pumpPing(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn) {
	defer cancel()

	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, pingCancel := context.WithTimeout(ctx, writeTimeout)
			err := conn.Ping(pingCtx)
			pingCancel()
			if err != nil {
				return
			}
		}
	}
}

// pumpPeer writes the peer's media -- and keyframe requests -- out to the
// operator until the stream ends or ctx is cancelled. A closed Audio()
// channel is the stream's end-of-call signal, so that branch sends FrameEnd
// and returns; every other exit (ctx.Done, a write failure) leaves the
// operator to notice the socket died on its own.
func (s *server) pumpPeer(ctx context.Context, conn *websocket.Conn, stream *call.Stream) {
	for {
		select {
		case <-ctx.Done():
			return

		case frame, ok := <-stream.Audio():
			if !ok {
				s.sendEnd(ctx, conn)
				return
			}
			if err := s.write(ctx, conn, FrameAudio, encodeS16LE(frame)); err != nil {
				return
			}

		case unit, ok := <-stream.Video():
			if !ok {
				s.sendEnd(ctx, conn)
				return
			}
			if err := s.write(ctx, conn, FrameVideo, unit); err != nil {
				return
			}

		// Keyframe carries two senses of "the operator's encoder needs to
		// emit an IDR now" on one channel: the one-shot request newStream
		// makes on attach (this operator has missed every keyframe so far),
		// and the peer's own repeated PLI/FIR feedback after packet loss
		// (wired in Manager.subscribe). Downstream, both mean exactly the
		// same thing, so there is no need to tell them apart here.
		case _, ok := <-stream.Keyframe():
			if !ok {
				s.sendEnd(ctx, conn)
				return
			}
			if err := s.write(ctx, conn, FrameKeyframe, nil); err != nil {
				return
			}
		}
	}
}

func (s *server) sendEnd(ctx context.Context, conn *websocket.Conn) {
	_ = s.write(ctx, conn, FrameEnd, nil)
}

func (s *server) write(ctx context.Context, conn *websocket.Conn, kind byte, payload []byte) error {
	writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()

	if err := conn.Write(writeCtx, websocket.MessageBinary, EncodeFrame(kind, payload)); err != nil {
		if !errors.Is(err, context.Canceled) {
			s.log.Warn("media: write to operator", "error", err)
		}
		return fmt.Errorf("media: write to operator: %w", err)
	}
	return nil
}

// encodeS16LE converts the library's decoded float32 samples to the s16le
// bytes the wire format carries. The clamp mirrors call.Recorder's: an
// overshooting sample must saturate, not wrap into the opposite sign and
// become a loud click.
func encodeS16LE(frame []float32) []byte {
	out := make([]byte, len(frame)*2)
	for i, sample := range frame {
		switch {
		case sample > 1:
			sample = 1
		case sample < -1:
			sample = -1
		}
		binary.LittleEndian.PutUint16(out[i*2:], uint16(int16(sample*32767)))
	}
	return out
}
