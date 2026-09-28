package test

import (
	"context"
	"errors"
	"testing"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/logging"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
	"github.com/w3nder/whatsmeow-gateway/internal/store"
)

func TestSessionClientLIDForPNResolvesFromTheLIDStoreAndAsksTheServerOnAMiss(t *testing.T) {
	ctx := context.Background()
	dsn := startPostgresForGateway(t)
	waLogger, slogger := logging.New()

	container, err := store.Open(ctx, dsn, waLogger)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	fresh, err := store.DeviceFor(ctx, container, nil)
	if err != nil {
		t.Fatalf("store.DeviceFor(nil): %v", err)
	}
	own := types.NewJID("15551234567", types.DefaultUserServer)
	fresh.ID = &own
	fresh.Account = &waAdv.ADVSignedDeviceIdentity{
		Details:             []byte{},
		AccountSignature:    make([]byte, 64),
		AccountSignatureKey: make([]byte, 32),
		DeviceSignature:     make([]byte, 64),
	}
	if err := fresh.Save(ctx); err != nil {
		t.Fatalf("device.Save: %v", err)
	}
	device, err := store.DeviceFor(ctx, container, &own)
	if err != nil {
		t.Fatalf("store.DeviceFor(own): %v", err)
	}

	known := types.NewJID("5511999887766", types.DefaultUserServer)
	lid := types.NewJID("2002125877314", types.HiddenUserServer)
	if err := device.LIDs.PutLIDMapping(ctx, lid, known); err != nil {
		t.Fatalf("PutLIDMapping: %v", err)
	}

	client := session.NewWAClient("channel-lid", device, waLogger, slogger)

	resolved, found, err := client.LIDForPN(ctx, known)
	if err != nil {
		t.Fatalf("LIDForPN(known): %v", err)
	}
	if !found || resolved != lid {
		t.Fatalf("expected %s from the LID store, got %s (found=%v)", lid, resolved, found)
	}

	unknown := types.NewJID("5511888777666", types.DefaultUserServer)
	if _, found, err := client.LIDForPN(ctx, unknown); !errors.Is(err, whatsmeow.ErrNotConnected) || found {
		t.Fatalf("expected a store miss to ask the server (not connected here), got found=%v err=%v", found, err)
	}
}
