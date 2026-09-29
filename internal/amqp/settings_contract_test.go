package amqp_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

const settingsCommandLiteral = `{"tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","listenGroups":true,"receiveCalls":false,"callRejectMessage":"Não atendemos ligações, escreva aqui."}`

const settingsCommandNullMessageLiteral = `{"tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","listenGroups":false,"receiveCalls":true,"callRejectMessage":null}`

const settingsCommandAbsentFieldsLiteral = `{"tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d"}`

func settingsBool(v bool) *bool { return &v }

func settingsString(v string) *string { return &v }

func decodeSettings(t *testing.T, literal string) amqp.SettingsCommand {
	t.Helper()
	var cmd amqp.SettingsCommand
	if err := json.Unmarshal([]byte(literal), &cmd); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return cmd
}

func TestSettingsCommandContractLiteral(t *testing.T) {
	got := decodeSettings(t, settingsCommandLiteral)
	want := amqp.SettingsCommand{
		TenantID:          "1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c",
		ChannelID:         "2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d",
		ListenGroups:      settingsBool(true),
		ReceiveCalls:      settingsBool(false),
		CallRejectMessage: settingsString("Não atendemos ligações, escreva aqui."),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded %+v, want %+v", got, want)
	}
}

func TestSettingsCommandAcceptsANullMessage(t *testing.T) {
	got := decodeSettings(t, settingsCommandNullMessageLiteral)
	if got.CallRejectMessage != nil {
		t.Fatalf("a null callRejectMessage must decode to nil, got %q", *got.CallRejectMessage)
	}
	if got.ListenGroups == nil || *got.ListenGroups || got.ReceiveCalls == nil || !*got.ReceiveCalls {
		t.Fatalf("booleans decoded wrong: %+v", got)
	}
}

func TestSettingsCommandLeavesAbsentFieldsNil(t *testing.T) {
	got := decodeSettings(t, settingsCommandAbsentFieldsLiteral)
	if got.ListenGroups != nil || got.ReceiveCalls != nil || got.CallRejectMessage != nil {
		t.Fatalf("absent fields must stay nil so the gateway applies the defaults, got %+v", got)
	}
}
