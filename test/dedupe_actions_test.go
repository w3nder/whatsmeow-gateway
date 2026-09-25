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

func TestDedupeActionsRefuseAFailureWithoutReason(t *testing.T) {
	store := openActionLedger(t)
	ctx := context.Background()

	if _, err := store.BeginAction(ctx, "cmd-empty", "g1@g.us"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := store.MarkActionFailed(ctx, "cmd-empty", "g1@g.us", ""); err == nil {
		t.Fatal("a failure without reason would replay as ok:true and must be refused")
	}
	record, err := store.BeginAction(ctx, "cmd-empty", "g1@g.us")
	if err != nil || record.Finished {
		t.Fatalf("the group must stay pending after a refused failure: record=%+v err=%v", record, err)
	}
}

func TestDedupeActionsCountTheHaltsOfAGroup(t *testing.T) {
	store := openActionLedger(t)
	ctx := context.Background()

	if _, err := store.RecordActionHalt(ctx, "cmd-halt", "g1@g.us"); err == nil {
		t.Fatal("a halt on a group never claimed must be refused")
	}
	for want := 1; want <= 3; want++ {
		if _, err := store.BeginAction(ctx, "cmd-halt", "g1@g.us"); err != nil {
			t.Fatalf("claim %d: %v", want, err)
		}
		halts, err := store.RecordActionHalt(ctx, "cmd-halt", "g1@g.us")
		if err != nil || halts != want {
			t.Fatalf("halt %d: got %d err=%v", want, halts, err)
		}
	}
	record, err := store.BeginAction(ctx, "cmd-halt", "g1@g.us")
	if err != nil || record.Finished {
		t.Fatalf("halts alone never finish a group: record=%+v err=%v", record, err)
	}
}
