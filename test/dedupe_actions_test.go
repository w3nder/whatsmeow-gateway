package test

import (
	"context"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/dedupe"
)

func openActionLedger(t *testing.T) *dedupe.Store {
	t.Helper()
	store, err := dedupe.Open(context.Background(), startPostgresForGateway(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(store.Close)
	return store
}

func TestDedupeActionsClaimThenDone(t *testing.T) {
	store := openActionLedger(t)
	ctx := context.Background()

	record, err := store.BeginAction(ctx, "cmd-1", "g1@g.us")
	if err != nil || record.Finished || record.Removed != nil {
		t.Fatalf("first claim: record=%+v err=%v", record, err)
	}
	record, err = store.BeginAction(ctx, "cmd-1", "g1@g.us")
	if err != nil || record.Finished || record.Removed != nil {
		t.Fatalf("pending redelivery must re-run the action: record=%+v err=%v", record, err)
	}
	two := 2
	if err := store.MarkActionDone(ctx, "cmd-1", "g1@g.us", &two); err != nil {
		t.Fatalf("MarkActionDone: %v", err)
	}
	record, err = store.BeginAction(ctx, "cmd-1", "g1@g.us")
	if err != nil || !record.Finished || record.Failure != "" || record.Removed == nil || *record.Removed != 2 {
		t.Fatalf("after done the claim must report finished with the stored removed count: record=%+v err=%v", record, err)
	}
	record, err = store.BeginAction(ctx, "cmd-1", "g2@g.us")
	if err != nil || record.Finished || record.Removed != nil {
		t.Fatalf("another group of the same command is independent: record=%+v err=%v", record, err)
	}
}

func TestDedupeActionsKeepAFailedGroupForTheReplay(t *testing.T) {
	store := openActionLedger(t)
	ctx := context.Background()

	if _, err := store.BeginAction(ctx, "cmd-locked", "g1@g.us"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := store.MarkActionFailed(ctx, "cmd-locked", "g1@g.us", "locked: group is locked (423)"); err != nil {
		t.Fatalf("MarkActionFailed: %v", err)
	}
	record, err := store.BeginAction(ctx, "cmd-locked", "g1@g.us")
	if err != nil || !record.Finished || record.Failure != "locked: group is locked (423)" || record.Removed != nil {
		t.Fatalf("a failed group must replay its failure instead of running again: record=%+v err=%v", record, err)
	}
}
