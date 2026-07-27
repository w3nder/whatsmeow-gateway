package test

import (
	"context"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/dedupe"
)

func TestDedupeStoreBeginInsertsPendingRow(t *testing.T) {
	dsn := startPostgresForGateway(t)
	ctx := context.Background()

	store, err := dedupe.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("dedupe.Open failed: %v", err)
	}
	t.Cleanup(store.Close)

	providerID := dedupe.DeterministicProviderID("msg-1")

	alreadySent, existingProviderID, err := store.Begin(ctx, "msg-1", providerID)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if alreadySent {
		t.Fatal("expected alreadySent=false for a brand new message id")
	}
	if existingProviderID != providerID {
		t.Fatalf("expected the freshly-inserted row to carry providerID %q, got %q", providerID, existingProviderID)
	}
}

func TestDedupeStoreBeginBeforeMarkSentIsNotAlreadySent(t *testing.T) {
	dsn := startPostgresForGateway(t)
	ctx := context.Background()

	store, err := dedupe.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("dedupe.Open failed: %v", err)
	}
	t.Cleanup(store.Close)

	providerID := dedupe.DeterministicProviderID("msg-2")

	if _, _, err := store.Begin(ctx, "msg-2", providerID); err != nil {
		t.Fatalf("first Begin failed: %v", err)
	}

	alreadySent, _, err := store.Begin(ctx, "msg-2", providerID)
	if err != nil {
		t.Fatalf("second Begin failed: %v", err)
	}
	if alreadySent {
		t.Fatal("expected alreadySent=false while the row is still pending (crash before MarkSent), so the caller retries the send")
	}
}

func TestDedupeStoreMarkSentThenBeginReportsAlreadySent(t *testing.T) {
	dsn := startPostgresForGateway(t)
	ctx := context.Background()

	store, err := dedupe.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("dedupe.Open failed: %v", err)
	}
	t.Cleanup(store.Close)

	providerID := dedupe.DeterministicProviderID("msg-3")

	if _, _, err := store.Begin(ctx, "msg-3", providerID); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := store.MarkSent(ctx, "msg-3"); err != nil {
		t.Fatalf("MarkSent failed: %v", err)
	}

	alreadySent, existingProviderID, err := store.Begin(ctx, "msg-3", providerID)
	if err != nil {
		t.Fatalf("Begin after MarkSent failed: %v", err)
	}
	if !alreadySent {
		t.Fatal("expected alreadySent=true after MarkSent")
	}
	if existingProviderID != providerID {
		t.Fatalf("expected existingProviderID=%q, got %q", providerID, existingProviderID)
	}
}

func TestDedupeStoreMarkSentUnknownMessageIDFails(t *testing.T) {
	dsn := startPostgresForGateway(t)
	ctx := context.Background()

	store, err := dedupe.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("dedupe.Open failed: %v", err)
	}
	t.Cleanup(store.Close)

	if err := store.MarkSent(ctx, "never-began"); err == nil {
		t.Fatal("expected MarkSent for an unknown message id to fail")
	}
}

func TestDeterministicProviderIDIsStablePerMessageID(t *testing.T) {
	first := dedupe.DeterministicProviderID("msg-abc")
	second := dedupe.DeterministicProviderID("msg-abc")
	if first != second {
		t.Fatalf("expected DeterministicProviderID to be stable, got %q then %q", first, second)
	}

	other := dedupe.DeterministicProviderID("msg-xyz")
	if first == other {
		t.Fatalf("expected DeterministicProviderID to differ for different message ids, both were %q", first)
	}
}
