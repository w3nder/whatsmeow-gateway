package amqp_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

const historyDoneLiteral = `{"kind":"done","tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","importId":"3c5d7e9f-1a2b-4c3d-8e4f-5a6b7c8d9e0f","source":"gateway","totalBatches":9,"skippedChats":2}`

func TestHistoryDoneMarshalsToTheContractLiteral(t *testing.T) {
	done := amqp.HistoryDone{
		Kind:         amqp.HistoryDoneKind,
		TenantID:     "1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c",
		ChannelID:    "2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d",
		ImportID:     "3c5d7e9f-1a2b-4c3d-8e4f-5a6b7c8d9e0f",
		Source:       amqp.HistorySourceGateway,
		TotalBatches: 9,
		SkippedChats: 2,
	}

	encoded, err := json.Marshal(done)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(encoded) != historyDoneLiteral {
		t.Fatalf("encoded %s\nwant    %s", encoded, historyDoneLiteral)
	}

	var decoded amqp.HistoryDone
	if err := json.Unmarshal([]byte(historyDoneLiteral), &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(decoded, done) {
		t.Fatalf("decoded %+v, want %+v", decoded, done)
	}
}
