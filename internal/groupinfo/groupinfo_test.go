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

func TestALapsedTTLTriggersASecondLookup(t *testing.T) {
	now := time.Now()
	c := New(Options{TTL: time.Hour, Now: func() time.Time { return now }}, slog.Default())
	lookup := &countingLookup{name: "Antigo"}
	jid := types.NewJID("120363000000000000", types.GroupServer)

	if got := c.Name(context.Background(), lookup, "ch1", jid); got != "Antigo" {
		t.Fatalf("got %q, want Antigo", got)
	}

	now = now.Add(2 * time.Hour)
	lookup.name = "Novo"

	if got := c.Name(context.Background(), lookup, "ch1", jid); got != "Novo" {
		t.Fatalf("got %q, want Novo", got)
	}
	if lookup.calls != 2 {
		t.Fatalf("expected 2 lookups, got %d", lookup.calls)
	}
}

type blockingLookup struct {
	started chan struct{}
	release chan struct{}
	name    string
}

func (l *blockingLookup) GetGroupInfo(_ context.Context, _ types.JID) (*types.GroupInfo, error) {
	close(l.started)
	<-l.release
	return &types.GroupInfo{GroupName: types.GroupName{Name: l.name}}, nil
}

func TestEvictionNeverDropsAnEntryMidResolve(t *testing.T) {
	c := New(Options{TTL: time.Hour}, slog.Default())
	jid := types.NewJID("120363000000000000", types.GroupServer)
	key := "ch1|" + jid.String()

	lookup := &blockingLookup{
		started: make(chan struct{}),
		release: make(chan struct{}),
		name:    "Resolved",
	}

	done := make(chan string, 1)
	go func() {
		done <- c.Name(context.Background(), lookup, "ch1", jid)
	}()
	<-lookup.started

	c.mu.Lock()
	c.evictIdle()
	_, stillPresent := c.entries[key]
	c.mu.Unlock()

	if !stillPresent {
		t.Fatalf("evictIdle dropped an entry that was mid-resolve")
	}

	close(lookup.release)
	if got := <-done; got != "Resolved" {
		t.Fatalf("got %q, want Resolved", got)
	}

	if got := c.Name(context.Background(), lookup, "ch1", jid); got != "Resolved" {
		t.Fatalf("got %q, want the resolved value to still be cached", got)
	}
}
