package groupinfo

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
)

type countingLookup struct {
	calls int
	name  string
	err   error
}

func (l *countingLookup) GetGroupInfo(_ context.Context, _ types.JID) (*types.GroupInfo, error) {
	l.calls++
	if l.err != nil {
		return nil, l.err
	}
	return &types.GroupInfo{GroupName: types.GroupName{Name: l.name}}, nil
}

func TestNameIsFetchedOnceAndThenServedFromCache(t *testing.T) {
	c := New(Options{TTL: time.Hour}, slog.Default())
	lookup := &countingLookup{name: "Vendas Nordeste"}
	jid := types.NewJID("120363000000000000", types.GroupServer)

	for i := 0; i < 5; i++ {
		if got := c.Name(context.Background(), lookup, "ch1", jid); got != "Vendas Nordeste" {
			t.Fatalf("call %d: got %q", i, got)
		}
	}
	if lookup.calls != 1 {
		t.Fatalf("expected 1 lookup, got %d", lookup.calls)
	}
}

func TestInvalidateForcesTheNextLookup(t *testing.T) {
	c := New(Options{TTL: time.Hour}, slog.Default())
	lookup := &countingLookup{name: "Antigo"}
	jid := types.NewJID("120363000000000000", types.GroupServer)

	c.Name(context.Background(), lookup, "ch1", jid)
	lookup.name = "Novo"
	c.Invalidate("ch1", jid)

	if got := c.Name(context.Background(), lookup, "ch1", jid); got != "Novo" {
		t.Fatalf("got %q, want Novo", got)
	}
	if lookup.calls != 2 {
		t.Fatalf("expected 2 lookups, got %d", lookup.calls)
	}
}

func TestTheSameGroupInTwoChannelsIsTwoEntries(t *testing.T) {
	c := New(Options{TTL: time.Hour}, slog.Default())
	lookup := &countingLookup{name: "Vendas"}
	jid := types.NewJID("120363000000000000", types.GroupServer)

	c.Name(context.Background(), lookup, "ch1", jid)
	c.Name(context.Background(), lookup, "ch2", jid)

	if lookup.calls != 2 {
		t.Fatalf("expected 2 lookups, got %d", lookup.calls)
	}
}

func TestAFailedLookupIsNotRetriedImmediately(t *testing.T) {
	c := New(Options{TTL: time.Hour, RetryAfter: time.Hour}, slog.Default())
	lookup := &countingLookup{err: errors.New("offline")}
	jid := types.NewJID("120363000000000000", types.GroupServer)

	c.Name(context.Background(), lookup, "ch1", jid)
	c.Name(context.Background(), lookup, "ch1", jid)

	if lookup.calls != 1 {
		t.Fatalf("expected 1 lookup, got %d", lookup.calls)
	}
}

func TestANilLookupAnswersEmptyWithoutPanicking(t *testing.T) {
	c := New(Options{TTL: time.Hour}, slog.Default())
	jid := types.NewJID("120363000000000000", types.GroupServer)

	if got := c.Name(context.Background(), nil, "ch1", jid); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}
