package test

import (
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/util/keys"
	"google.golang.org/protobuf/proto"

	"github.com/w3nder/whatsmeow-gateway/internal/deviceprops"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

func TestConfigureAutoReconnectEnablesInitialAutoReconnect(t *testing.T) {
	client := &whatsmeow.Client{}

	session.ConfigureAutoReconnect(client)

	if !client.EnableAutoReconnect {
		t.Fatal("expected EnableAutoReconnect so a dropped socket is retried by whatsmeow")
	}
	if !client.InitialAutoReconnect {
		t.Fatal("expected InitialAutoReconnect so a retryable network error on the first Connect keeps retrying in background instead of killing the session")
	}
	if client.AutoReconnectHook == nil {
		t.Fatal("expected an AutoReconnectHook so the gateway controls the retry ceiling")
	}
}

func TestConfigureAutoReconnectKeepsRetryingWithCappedBackoff(t *testing.T) {
	client := &whatsmeow.Client{}
	session.ConfigureAutoReconnect(client)

	client.AutoReconnectErrors = 10000

	if !client.AutoReconnectHook(errors.New("dial tcp: network is unreachable")) {
		t.Fatal("expected the hook to keep retrying so an unstable network recovers without re-pairing")
	}

	backoff := time.Duration(client.AutoReconnectErrors) * 2 * time.Second
	if backoff > session.MaxAutoReconnectDelay {
		t.Fatalf("expected reconnect backoff capped at %s, got %s", session.MaxAutoReconnectDelay, backoff)
	}
}

func TestTakeOverHistoryTouchesOnlyTheClientThatImports(t *testing.T) {
	importing := &whatsmeow.Client{}
	idle := &whatsmeow.Client{}
	if importing.ManualHistorySyncDownload || idle.ManualHistorySyncDownload {
		t.Fatal("a client starts with whatsmeow's automatic history download")
	}

	session.TakeOverHistory(importing)

	if !importing.ManualHistorySyncDownload {
		t.Fatal("the importing client must leave the chunk download to the gateway")
	}
	if idle.ManualHistorySyncDownload {
		t.Fatal("a client without an import must keep the automatic download")
	}
}

func unpairedDevice() *store.Device {
	identity := keys.NewKeyPair()
	return &store.Device{RegistrationID: 1, IdentityKey: identity, SignedPreKey: identity.CreateSignedPreKey(1)}
}

func registrationProps(t *testing.T, client *whatsmeow.Client) *waCompanionReg.DeviceProps {
	t.Helper()
	payload := client.Store.GetClientPayload()
	if client.GetClientPayload != nil {
		payload = client.GetClientPayload()
	}
	var props waCompanionReg.DeviceProps
	if err := proto.Unmarshal(payload.GetDevicePairingData().GetDeviceProps(), &props); err != nil {
		t.Fatalf("unmarshal the registration props: %v", err)
	}
	return &props
}

func TestTakeOverHistoryRegistersOnlyTheImportingClientWithTheHistoryProps(t *testing.T) {
	importing := &whatsmeow.Client{Store: unpairedDevice()}
	idle := &whatsmeow.Client{Store: unpairedDevice()}
	shared := proto.Clone(store.DeviceProps)

	session.TakeOverHistory(importing)

	props := registrationProps(t, importing)
	config := props.GetHistorySyncConfig()
	if props.GetPlatformType() != deviceprops.HistoryPlatform || props.GetRequireFullSync() != deviceprops.HistoryRequireFullSync ||
		config.GetFullSyncDaysLimit() != deviceprops.WindowDays || config.GetRecentSyncDaysLimit() != deviceprops.WindowDays ||
		config.GetFullSyncSizeMbLimit() != deviceprops.HistorySizeMbLimit {
		t.Fatalf("the importing client must register with the history props, got %v", props)
	}
	if !proto.Equal(registrationProps(t, idle), shared) {
		t.Fatal("a client without an import must register with today's props")
	}
	if !proto.Equal(store.DeviceProps, shared) {
		t.Fatal("taking over the history must never change the props shared by every channel")
	}
}
