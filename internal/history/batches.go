package history

import (
	"encoding/json"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

const (
	batchEnvelopeBytes = 512
	chatEnvelopeBytes  = 96
)

func Split(chats []amqp.HistoryChat, maxMessages, maxBytes int) ([][]amqp.HistoryChat, []string) {
	var batches [][]amqp.HistoryChat
	var oversized []string
	var current []amqp.HistoryChat
	count, size := 0, batchEnvelopeBytes
	for _, chat := range chats {
		open := -1
		header := chatHeaderBytes(chat)
		for _, message := range chat.Messages {
			if batchEnvelopeBytes+header+len(message)+1 > maxBytes {
				oversized = append(oversized, providerMessageID(message))
				continue
			}
			cost := len(message) + 1
			if open < 0 {
				cost += header
			}
			if count > 0 && (count == maxMessages || size+cost > maxBytes) {
				batches = append(batches, current)
				current, count, size, open = nil, 0, batchEnvelopeBytes, -1
				cost = len(message) + 1 + header
			}
			if open < 0 {
				current = append(current, amqp.HistoryChat{Phone: chat.Phone, Lid: chat.Lid, ProfileName: chat.ProfileName})
				open = len(current) - 1
			}
			current[open].Messages = append(current[open].Messages, message)
			count++
			size += cost
		}
	}
	if count > 0 {
		batches = append(batches, current)
	}
	return batches, oversized
}

func providerMessageID(message json.RawMessage) string {
	var head struct {
		ProviderMessageID string `json:"providerMessageId"`
	}
	_ = json.Unmarshal(message, &head)
	return head.ProviderMessageID
}

func chatHeaderBytes(chat amqp.HistoryChat) int {
	size := chatEnvelopeBytes + len(chat.Phone)
	if chat.Lid != nil {
		size += len(*chat.Lid)
	}
	if chat.ProfileName != nil {
		size += len(*chat.ProfileName)
	}
	return size
}
