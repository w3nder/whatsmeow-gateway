package gateway

import (
	"context"
	"log/slog"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/channelsettings"
)

func settingsBool(v bool) *bool { return &v }

func settingsString(v string) *string { return &v }

func TestSettingsFromAppliesTheCommandOverTheDefaults(t *testing.T) {
	got := settingsFrom(amqp.SettingsCommand{
		ListenGroups:      settingsBool(false),
		ReceiveCalls:      settingsBool(false),
		CallRejectMessage: settingsString("  Não atendemos ligações.  "),
	})
	want := channelsettings.Settings{ListenGroups: false, ReceiveCalls: false, CallRejectMessage: "Não atendemos ligações."}
	if got != want {
		t.Fatalf("settingsFrom = %+v, want %+v", got, want)
	}
}

func TestSettingsFromTreatsAbsentFieldsAsTheDefaults(t *testing.T) {
	if got := settingsFrom(amqp.SettingsCommand{}); got != channelsettings.Defaults() {
		t.Fatalf("an empty command must mean the defaults, got %+v", got)
	}
}

func TestSettingsFromTreatsANullMessageAsNoMessage(t *testing.T) {
	got := settingsFrom(amqp.SettingsCommand{ReceiveCalls: settingsBool(false), CallRejectMessage: nil})
	if got.CallRejectMessage != "" || got.ReceiveCalls {
		t.Fatalf("settingsFrom = %+v, want calls off and no message", got)
	}
}

func TestSettingsHandlerRejectsACommandWithoutIdentity(t *testing.T) {
	g := &gateway{settings: channelsettings.NewMap(), logger: slog.New(slog.DiscardHandler), tenantByChannel: map[string]string{}}

	if err := g.SettingsHandler(context.Background(), amqp.SettingsCommand{TenantID: "t1"}); err == nil {
		t.Fatal("a command without channelId must fail so it lands in the dead-letter queue")
	}
	if err := g.SettingsHandler(context.Background(), amqp.SettingsCommand{ChannelID: "channel-1"}); err == nil {
		t.Fatal("a command without tenantId must fail so it lands in the dead-letter queue")
	}
}

func TestSettingsHandlerRefusesAnotherTenantsCommandForAKnownChannel(t *testing.T) {
	g := &gateway{settings: channelsettings.NewMap(), logger: slog.New(slog.DiscardHandler), tenantByChannel: map[string]string{"channel-1": "tenant-1"}}

	err := g.SettingsHandler(context.Background(), amqp.SettingsCommand{
		TenantID:     "tenant-other",
		ChannelID:    "channel-1",
		ListenGroups: settingsBool(false),
	})
	if err == nil {
		t.Fatal("a command from another tenant must be refused")
	}
	if got := g.settings.For("channel-1"); got != channelsettings.Defaults() {
		t.Fatalf("a refused command must not change the map, got %+v", got)
	}
}
