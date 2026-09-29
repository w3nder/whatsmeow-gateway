package history

import "github.com/w3nder/whatsmeow-gateway/internal/amqp"

const (
	batchEnvelopeBytes = 512
	chatEnvelopeBytes  = 96
)

func Split(chats []amqp.HistoryChat, maxMessages, maxBytes int) [][]amqp.HistoryChat {
	var batches [][]amqp.HistoryChat
	var current []amqp.HistoryChat
	count, size := 0, batchEnvelopeBytes
	for _, chat := range chats {
		open := -1
		header := chatHeaderBytes(chat)
		for _, message := range chat.Messages {
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
	return batches
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
