package test

import (
	"context"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/dedupe"
)

func TestDedupeActionsClaimThenDone(t *testing.T) {
	dsn := startPostgresForGateway(t)
	ctx := context.Background()
	store, err := dedupe.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(store.Close)

	done, removed, err := store.BeginAction(ctx, "cmd-1", "g1@g.us")
	if err != nil || done || removed != nil {
		t.Fatalf("first claim: done=%v removed=%v err=%v", done, removed, err)
	}
	done, removed, err = store.BeginAction(ctx, "cmd-1", "g1@g.us")
	if err != nil || done || removed != nil {
		t.Fatalf("pending redelivery must re-run the action: done=%v removed=%v err=%v", done, removed, err)
	}
	two := 2
	if err := store.MarkActionDone(ctx, "cmd-1", "g1@g.us", &two); err != nil {
		t.Fatalf("MarkActionDone: %v", err)
	}
	done, removed, err = store.BeginAction(ctx, "cmd-1", "g1@g.us")
	if err != nil || !done || removed == nil || *removed != 2 {
		t.Fatalf("after done the claim must report alreadyDone with the stored removed count: done=%v removed=%v err=%v", done, removed, err)
	}
	done, removed, err = store.BeginAction(ctx, "cmd-1", "g2@g.us")
	if err != nil || done || removed != nil {
		t.Fatalf("another group of the same command is independent: done=%v removed=%v err=%v", done, removed, err)
	}
}
