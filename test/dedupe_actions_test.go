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

	done, err := store.BeginAction(ctx, "cmd-1", "g1@g.us")
	if err != nil || done {
		t.Fatalf("first claim: done=%v err=%v", done, err)
	}
	done, err = store.BeginAction(ctx, "cmd-1", "g1@g.us")
	if err != nil || done {
		t.Fatalf("pending redelivery must re-run the action: done=%v err=%v", done, err)
	}
	if err := store.MarkActionDone(ctx, "cmd-1", "g1@g.us"); err != nil {
		t.Fatalf("MarkActionDone: %v", err)
	}
	done, err = store.BeginAction(ctx, "cmd-1", "g1@g.us")
	if err != nil || !done {
		t.Fatalf("after done the claim must report alreadyDone: done=%v err=%v", done, err)
	}
	done, err = store.BeginAction(ctx, "cmd-1", "g2@g.us")
	if err != nil || done {
		t.Fatalf("another group of the same command is independent: done=%v err=%v", done, err)
	}
}
