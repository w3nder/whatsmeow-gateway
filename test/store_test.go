package test

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/logging"
	"github.com/w3nder/whatsmeow-gateway/internal/store"
)

func TestStoreOpenAndDeviceRoundTrip(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("whatsmeow"),
		tcpostgres.WithUsername("whatsmeow"),
		tcpostgres.WithPassword("whatsmeow"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := pgContainer.Terminate(context.Background()); err != nil {
			t.Errorf("failed to terminate postgres container: %v", err)
		}
	})

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	waLogger, _ := logging.New()

	container1, err := store.Open(ctx, dsn, waLogger)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	device, err := store.DeviceFor(ctx, container1, nil)
	if err != nil {
		t.Fatalf("DeviceFor(nil) failed: %v", err)
	}
	if device == nil {
		t.Fatal("DeviceFor(nil) returned nil device")
	}

	jid := types.NewJID("15551234567", types.DefaultUserServer)
	device.ID = &jid
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details:             []byte{},
		AccountSignature:    make([]byte, 64),
		AccountSignatureKey: make([]byte, 32),
		DeviceSignature:     make([]byte, 64),
	}

	if err := device.Save(ctx); err != nil {
		t.Fatalf("device.Save failed: %v", err)
	}

	container2, err := store.Open(ctx, dsn, waLogger)
	if err != nil {
		t.Fatalf("second Open failed: %v", err)
	}

	devices, err := container2.GetAllDevices(ctx)
	if err != nil {
		t.Fatalf("GetAllDevices failed: %v", err)
	}
	if len(devices) != 1 {
		t.Fatalf("expected 1 persisted device, got %d", len(devices))
	}
	if devices[0].ID == nil || *devices[0].ID != jid {
		t.Fatalf("expected persisted device JID %s, got %v", jid, devices[0].ID)
	}

	fetched, err := store.DeviceFor(ctx, container2, &jid)
	if err != nil {
		t.Fatalf("DeviceFor(jid) failed: %v", err)
	}
	if fetched == nil {
		t.Fatal("DeviceFor(jid) returned nil device")
	}
	if fetched.ID == nil || *fetched.ID != jid {
		t.Fatalf("expected fetched device JID %s, got %v", jid, fetched.ID)
	}
}
