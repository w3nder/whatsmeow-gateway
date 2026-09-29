package gateway

import (
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func chatOfEvent(evt any) (types.JID, bool) {
	switch e := evt.(type) {
	case *events.Message:
		return e.Info.Chat, true
	case *events.Receipt:
		return e.Chat, true
	case *events.GroupInfo:
		return e.JID, true
	default:
		return types.JID{}, false
	}
}

func isGroupEvent(evt any) bool {
	chat, ok := chatOfEvent(evt)
	return ok && chat.Server == types.GroupServer
}

func (g *gateway) ignoredByListenGroups(channelID string, evt any) bool {
	return !g.settings.For(channelID).ListenGroups && isGroupEvent(evt)
}
