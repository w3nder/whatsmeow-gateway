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

const maxOperatorFrameBytes = 1 << 20

var (
	writeTimeout = 10 * time.Second
	pingInterval = 15 * time.Second
)

type ServerConfig struct {
	Addr           string
	Secret         string
	AllowedOrigins []string
}

type server struct {
	calls          *call.Manager
	secret         string
	allowedOrigins []string
	log            *slog.Logger
}

func NewServer(calls *call.Manager, cfg ServerConfig, log *slog.Logger) *http.Server {
	s := &server{calls: calls, secret: cfg.Secret, allowedOrigins: cfg.AllowedOrigins, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /calls/{callID}/media", s.handleMedia)

	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func (s *server) handleMedia(w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("callID")

	claims, err := VerifyCallToken(r.URL.Query().Get("token"), s.secret)
	if err != nil {
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}

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

	conn.SetReadLimit(maxOperatorFrameBytes)

	stream, detach, ok := s.calls.AttachStream(claims.ChannelID, claims.CallID)
	if !ok {
		_ = conn.Close(websocket.StatusNormalClosure, "call ended")
		return
	}
	defer detach()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	go s.pumpOperator(ctx, cancel, conn, stream)
	go s.pumpPing(ctx, cancel, conn)
	s.pumpPeer(ctx, conn, stream)
}

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
