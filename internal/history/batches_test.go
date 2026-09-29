package history_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/history"
)

func chatWith(phone string, count, bodyBytes int) amqp.HistoryChat {
	name := "Maria"
	chat := amqp.HistoryChat{Phone: phone, ProfileName: &name}
	for n := range count {
		chat.Messages = append(chat.Messages, json.RawMessage(fmt.Sprintf(`{"providerMessageId":"%s-%d","text":{"body":"%s"},"timestamp":"1739230955","type":"text"}`, phone, n, strings.Repeat("a", bodyBytes))))
	}
	return chat
}

func messageIDs(t *testing.T, batches [][]amqp.HistoryChat) []string {
	t.Helper()
	var ids []string
	for _, batch := range batches {
		for _, chat := range batch {
			for _, raw := range chat.Messages {
				var head struct {
					ProviderMessageID string `json:"providerMessageId"`
				}
				if err := json.Unmarshal(raw, &head); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}
				ids = append(ids, head.ProviderMessageID)
			}
		}
	}
	return ids
}

func TestSplitCapsEachBatchAtMaxMessagesAndRepeatsTheChatHeader(t *testing.T) {
	chat := chatWith("5511999998888", 1200, 10)

	batches := history.Split([]amqp.HistoryChat{chat}, 500, 1_000_000)

	if len(batches) != 3 {
		t.Fatalf("1200 messages make 3 batches of at most 500, got %d", len(batches))
	}
	for n, want := range []int{500, 500, 200} {
		if len(batches[n]) != 1 || len(batches[n][0].Messages) != want {
			t.Fatalf("batch %d: want one chat with %d messages, got %d chats", n, want, len(batches[n]))
		}
		if batches[n][0].Phone != "5511999998888" || batches[n][0].ProfileName == nil || *batches[n][0].ProfileName != "Maria" {
			t.Fatalf("batch %d must repeat the chat header, got %+v", n, batches[n][0])
		}
	}
	if ids := messageIDs(t, batches); len(ids) != 1200 || ids[0] != "5511999998888-0" || ids[1199] != "5511999998888-1199" {
		t.Fatalf("every message stays once and in order, got %d", len(ids))
	}
}

func TestSplitKeepsEveryBatchUnderMaxBytes(t *testing.T) {
	chats := []amqp.HistoryChat{
		chatWith("5511911111111", 100, 2000),
		chatWith("5511922222222", 100, 2000),
		chatWith("5511933333333", 100, 2000),
	}
	const maxBytes = 64 * 1024

	batches := history.Split(chats, 500, maxBytes)

	for n, chatsInBatch := range batches {
		encoded, err := json.Marshal(amqp.HistoryBatch{
			Kind:           amqp.HistoryBatchKind,
			TenantID:       "1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c",
			ChannelID:      "2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d",
			ImportID:       "3c5d7e9f-1a2b-4c3d-8e4f-5a6b7c8d9e0f",
			Source:         amqp.HistorySourceGateway,
			ChunkOrder:     4294967295,
			SourceProgress: 100,
			BatchIndex:     n + 1,
			BatchesInChunk: len(batches),
			Chats:          chatsInBatch,
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if len(encoded) > maxBytes {
			t.Fatalf("batch %d has %d bytes, above %d", n, len(encoded), maxBytes)
		}
	}
	if ids := messageIDs(t, batches); len(ids) != 300 || ids[0] != "5511911111111-0" || ids[299] != "5511933333333-99" {
		t.Fatalf("every message stays once and in order, got %d", len(ids))
	}
}

func TestSplitOfNoChatsHasNoBatch(t *testing.T) {
	if batches := history.Split(nil, 500, 1_000_000); len(batches) != 0 {
		t.Fatalf("nothing to publish, got %d batches", len(batches))
	}
}
