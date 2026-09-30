package history

import (
	"encoding/json"
	"fmt"

	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
)

var chatLevelFields = []string{"phoneNumberId", "from", "senderPn", "senderLid", "profileName"}

func messageOf(evt mapper.InboundEvent) (json.RawMessage, error) {
	encoded, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("history: encode message %s: %w", evt.ProviderMessageID, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return nil, fmt.Errorf("history: decode message %s: %w", evt.ProviderMessageID, err)
	}
	for _, name := range chatLevelFields {
		delete(fields, name)
	}
	return json.Marshal(fields)
}
