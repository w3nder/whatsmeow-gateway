package history

import (
	"context"
	"encoding/json"
	"time"

	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
	"github.com/w3nder/whatsmeow-gateway/internal/senderid"
)

type MessageSource interface {
	mapper.Downloader
	mapper.PNResolver
	mapper.MessageSecrets
	ParseWebMessage(chat types.JID, msg *waWeb.WebMessageInfo) (*events.Message, error)
}

type TranslateDeps struct {
	Source    MessageSource
	Media     mapper.MediaStore
	ChannelID string
	TenantID  string
}

type Translation struct {
	Chats             []amqp.HistoryChat
	Messages          int
	OutOfWindow       int
	Skipped           int
	ChatsWithoutPhone int
}

type candidate struct {
	chat int
	evt  *events.Message
}

type builtMessage struct {
	event mapper.InboundEvent
	ok    bool
}

func Translate(ctx context.Context, deps TranslateDeps, data *waHistorySync.HistorySync, limits Limits, now time.Time) (Translation, error) {
	var result Translation
	oldest := now.Add(-limits.Window)
	var headers []amqp.HistoryChat
	var candidates []candidate

	for _, conv := range data.GetConversations() {
		chat, err := types.ParseJID(conv.GetID())
		if err != nil || mapper.KindOf(chat) != mapper.ChatPrivate {
			result.Skipped += len(conv.GetMessages())
			continue
		}
		alt := chatAlt(chat, conv)
		lid, pn := senderid.Resolve(ctx, deps.Source, chat, alt)
		if pn == "" {
			result.ChatsWithoutPhone++
			continue
		}
		index := len(headers)
		headers = append(headers, amqp.HistoryChat{Phone: pn, Lid: optional(lid)})
		for _, item := range conv.GetMessages() {
			evt, err := deps.Source.ParseWebMessage(chat, item.GetMessage())
			switch {
			case err != nil || evt.Message == nil || evt.IsEdit:
				result.Skipped++
			case evt.Info.Timestamp.Before(oldest):
				result.OutOfWindow++
			default:
				withChatAlt(evt, alt)
				candidates = append(candidates, candidate{chat: index, evt: evt})
			}
		}
	}

	built := buildAll(ctx, deps, candidates)
	messages := make([][]mapper.InboundEvent, len(headers))
	for n, c := range candidates {
		if !built[n].ok {
			result.Skipped++
			continue
		}
		messages[c.chat] = append(messages[c.chat], built[n].event)
	}

	for n, header := range headers {
		if len(messages[n]) == 0 {
			continue
		}
		chat, err := withMessages(header, messages[n])
		if err != nil {
			return Translation{}, err
		}
		result.Chats = append(result.Chats, chat)
		result.Messages += len(chat.Messages)
	}
	return result, nil
}

func buildAll(ctx context.Context, deps TranslateDeps, candidates []candidate) []builtMessage {
	built := make([]builtMessage, len(candidates))
	for n, c := range candidates {
		built[n].event, built[n].ok = buildOne(ctx, deps, c.evt)
	}
	return built
}

func buildOne(ctx context.Context, deps TranslateDeps, evt *events.Message) (mapper.InboundEvent, bool) {
	out, err := mapper.BuildInbound(ctx, mapper.InboundDeps{
		Downloader: deps.Source,
		Resolver:   deps.Source,
		Media:      deps.Media,
		Secrets:    deps.Source,
		ChannelID:  deps.ChannelID,
		TenantID:   deps.TenantID,
	}, evt)
	if err != nil || out.ChangesAnotherMessage() {
		return mapper.InboundEvent{}, false
	}
	return out, true
}

func withMessages(header amqp.HistoryChat, built []mapper.InboundEvent) (amqp.HistoryChat, error) {
	chat := header
	chat.Messages = make([]json.RawMessage, 0, len(built))
	for _, evt := range built {
		if chat.ProfileName == nil {
			chat.ProfileName = optional(evt.ProfileName)
		}
		raw, err := messageOf(evt)
		if err != nil {
			return amqp.HistoryChat{}, err
		}
		chat.Messages = append(chat.Messages, raw)
	}
	return chat, nil
}

func chatAlt(chat types.JID, conv *waHistorySync.Conversation) types.JID {
	raw := conv.GetPnJID()
	if chat.Server == types.DefaultUserServer {
		raw = conv.GetLidJID()
	}
	if raw == "" {
		return types.EmptyJID
	}
	alt, err := types.ParseJID(raw)
	if err != nil {
		return types.EmptyJID
	}
	return alt
}

func withChatAlt(evt *events.Message, alt types.JID) {
	if evt.Info.IsFromMe {
		evt.Info.RecipientAlt = alt
		return
	}
	evt.Info.SenderAlt = alt
}

func optional(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
