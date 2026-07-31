package test

import (
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"

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

	// whatsmeow sleeps AutoReconnectErrors*2s between attempts (client.go autoReconnect).
	backoff := time.Duration(client.AutoReconnectErrors) * 2 * time.Second
	if backoff > session.MaxAutoReconnectDelay {
		t.Fatalf("expected reconnect backoff capped at %s, got %s", session.MaxAutoReconnectDelay, backoff)
	}
}
