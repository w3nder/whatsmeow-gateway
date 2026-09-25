package gateway

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

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

func TestGroupHandlerRequeuesOnceTheGatewayIsStoppingEvenWithALiveContext(t *testing.T) {
	g := &gateway{logger: slog.New(slog.DiscardHandler)}
	g.stopping.Store(true)

	err := g.GroupHandler(context.Background(), amqp.GatewayGroupCommand{CommandID: "c", ChannelID: "channel-1", Action: "lock", GroupJIDs: []string{"120363000000000001@g.us"}})
	if !errors.Is(err, amqp.ErrRequeue) {
		t.Fatalf("a gateway that is exiting must hand the command back, never fail its groups, got %v", err)
	}
}

func TestChannelPacerForgetsChannelsIdleLongerThanTheGap(t *testing.T) {
	p := newChannelPacer()
	p.touch("old")
	p.last["old"] = time.Now().Add(-time.Minute)
	p.touch("recent")
	if _, kept := p.last["old"]; kept {
		t.Fatal("a channel idle longer than the largest gap must be pruned")
	}
	if _, kept := p.last["recent"]; !kept {
		t.Fatal("the channel just touched must be kept")
	}
}
