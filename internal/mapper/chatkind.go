package mapper

import "go.mau.fi/whatsmeow/types"

type ChatKind int

const (
	ChatPrivate ChatKind = iota
	ChatGroup
	ChatIgnored
)

func KindOf(chat types.JID) ChatKind {
	switch chat.Server {
	case types.GroupServer:
		return ChatGroup
	case types.BroadcastServer, types.NewsletterServer:
		return ChatIgnored
	default:
		return ChatPrivate
	}
}
