package gateway

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

func TestGroupClientNeverReopensAChannelOnceTheGatewayIsStopping(t *testing.T) {
	opened := 0
	g := &gateway{
		manager: session.NewManager(func(string, *types.JID) (session.WAClient, error) {
			opened++
			return nil, errors.New("must not be called")
		}),
		logger:          slog.New(slog.DiscardHandler),
		tenantByChannel: map[string]string{},
	}
	g.stopping.Store(true)

	_, err := g.groupClient(context.Background(), "t", "channel-1")
	var rpcErr *amqp.RpcError
	if !errors.As(err, &rpcErr) || rpcErr.Code != amqp.RpcCodeUnavailable {
		t.Fatalf("a stopping gateway must answer unavailable, got %v", err)
	}
	if opened != 0 {
		t.Fatalf("a stopping gateway must not open a session, factory called %d times", opened)
	}
}
