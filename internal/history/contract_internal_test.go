package history

import (
	"encoding/json"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
)

const historyBatchLiteral = `{"kind":"batch","tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","importId":"3c5d7e9f-1a2b-4c3d-8e4f-5a6b7c8d9e0f","source":"gateway","chunkOrder":4,"sourceProgress":55,"batchIndex":2,"batchesInChunk":3,"chats":[{"phone":"5511999998888","lid":null,"profileName":"Maria","messages":[{"providerMessageId":"3EB0A1","text":{"body":"Olá"},"timestamp":"1739230955","type":"text"}]}]}`

func TestHistoryBatchBuiltFromALiveEventMatchesTheContractLiteral(t *testing.T) {
	live := mapper.InboundEvent{
		PhoneNumberID:     "2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d",
		From:              "5511999998888",
		SenderPn:          "5511999998888",
		ProfileName:       "Maria",
		ProviderMessageID: "3EB0A1",
		Timestamp:         "1739230955",
		Type:              "text",
		Text:              &mapper.InboundText{Body: "Olá"},
	}
	chat, err := withMessages(amqp.HistoryChat{Phone: "5511999998888", Lid: optional("")}, []mapper.InboundEvent{live})
	if err != nil {
		t.Fatalf("withMessages: %v", err)
	}
	batch := amqp.HistoryBatch{
		Kind:           amqp.HistoryBatchKind,
		TenantID:       "1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c",
		ChannelID:      "2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d",
		ImportID:       "3c5d7e9f-1a2b-4c3d-8e4f-5a6b7c8d9e0f",
		Source:         amqp.HistorySourceGateway,
		ChunkOrder:     4,
		SourceProgress: 55,
		BatchIndex:     2,
		BatchesInChunk: 3,
		Chats:          []amqp.HistoryChat{chat},
	}

	encoded, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(encoded) != historyBatchLiteral {
		t.Fatalf("the gateway emits\n%s\nthe contract says\n%s", encoded, historyBatchLiteral)
	}
}
