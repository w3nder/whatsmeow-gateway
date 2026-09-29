package amqp_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

const pairCommandWithHistoryLiteral = `{"tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","userId":"4d6e8f0a-1b2c-4d3e-9f5a-6b7c8d9e0f1a","importHistory":true,"importId":"3c5d7e9f-1a2b-4c3d-8e4f-5a6b7c8d9e0f"}`

const pairCommandWithoutHistoryLiteral = `{"tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","userId":"4d6e8f0a-1b2c-4d3e-9f5a-6b7c8d9e0f1a","importHistory":false}`

const pairCommandBeforeHistoryLiteral = `{"tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","userId":"4d6e8f0a-1b2c-4d3e-9f5a-6b7c8d9e0f1a"}`

func decodePair(t *testing.T, literal string) amqp.PairCommand {
	t.Helper()
	var cmd amqp.PairCommand
	if err := json.Unmarshal([]byte(literal), &cmd); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return cmd
}

func encodePair(t *testing.T, cmd amqp.PairCommand) string {
	t.Helper()
	encoded, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(encoded)
}

func TestPairCommandWithHistoryLiteral(t *testing.T) {
	want := amqp.PairCommand{
		TenantID:      "1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c",
		ChannelID:     "2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d",
		UserID:        "4d6e8f0a-1b2c-4d3e-9f5a-6b7c8d9e0f1a",
		ImportHistory: true,
		ImportID:      "3c5d7e9f-1a2b-4c3d-8e4f-5a6b7c8d9e0f",
	}
	got := decodePair(t, pairCommandWithHistoryLiteral)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded %+v, want %+v", got, want)
	}
	if !got.ImportsHistory() {
		t.Fatal("a command with importHistory and an importId must import the history")
	}
	if encoded := encodePair(t, want); encoded != pairCommandWithHistoryLiteral {
		t.Fatalf("encoded %s\nwant    %s", encoded, pairCommandWithHistoryLiteral)
	}
}

func TestPairCommandWithoutHistoryLiteral(t *testing.T) {
	got := decodePair(t, pairCommandWithoutHistoryLiteral)
	if got.ImportHistory || got.ImportID != "" || got.ImportsHistory() {
		t.Fatalf("an opted-out command must import nothing, got %+v", got)
	}
	if encoded := encodePair(t, got); encoded != pairCommandWithoutHistoryLiteral {
		t.Fatalf("encoded %s\nwant    %s", encoded, pairCommandWithoutHistoryLiteral)
	}
}

func TestPairCommandFromABackendWithoutHistoryImportsNothing(t *testing.T) {
	if got := decodePair(t, pairCommandBeforeHistoryLiteral); got.ImportsHistory() {
		t.Fatalf("a command without the history fields must import nothing, got %+v", got)
	}
}

func TestPairCommandAskingForHistoryWithoutAnImportIDImportsNothing(t *testing.T) {
	if (amqp.PairCommand{ImportHistory: true}).ImportsHistory() {
		t.Fatal("without an importId there is no import to publish to")
	}
}
