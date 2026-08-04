package media

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/coder/websocket"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

// ServerConfig configures the media websocket listener.
type ServerConfig struct {
	Addr   string
	Secret string
}

// server exposes one call's live media over a websocket, authenticated by a
// short-lived token instead of session cookies: the operator connecting here
// is a browser tab, not a service-to-service caller.
type server struct {
	calls  *call.Manager
	secret string
	log    *slog.Logger
}

// NewServer builds the media HTTP server. It does not start listening --
// callers run it (e.g. via ListenAndServe) and stop it via Shutdown, same as
// any other http.Server.
func NewServer(calls *call.Manager, cfg ServerConfig, log *slog.Logger) *http.Server {
	s := &server{calls: calls, secret: cfg.Secret, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /calls/{callID}/media", s.handleMedia)

	return &http.Server{
		Addr:    cfg.Addr,
		Handler: mux,
	}
}

// handleMedia authenticates and authorises the request, then upgrades. Every
// rejection below returns a plain HTTP error and stops before
// websocket.Accept: once the handshake completes, the caller already knows
// the call exists, so nothing that must stay secret can be checked after it.
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

	stream, detach, ok := s.calls.AttachStream(claims.ChannelID, claims.CallID)
	if !ok {
		http.Error(w, "call not found", http.StatusNotFound)
		return
	}
	defer detach()

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		s.log.Error("media: websocket upgrade", "call_id", callID, "error", err)
		return
	}
	defer func() { _ = conn.CloseNow() }()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// pumpOperator cancels ctx when it returns (socket closed, bad frame, or
	// context done), which is what tells pumpPeer below to stop writing to a
	// connection that is going away.
	go s.pumpOperator(ctx, cancel, conn, stream)
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
	if err := conn.Write(ctx, websocket.MessageBinary, EncodeFrame(kind, payload)); err != nil {
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
