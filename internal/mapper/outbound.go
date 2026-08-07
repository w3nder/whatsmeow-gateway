package mapper

import (
	"context"
	"fmt"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

type Uploader interface {
	Upload(ctx context.Context, data []byte, mt whatsmeow.MediaType) (whatsmeow.UploadResponse, error)
}

type MessageBuilder interface {
	Uploader
	BuildEdit(chat types.JID, id types.MessageID, newContent *waE2E.Message) *waE2E.Message
	BuildRevoke(chat, sender types.JID, id types.MessageID) *waE2E.Message
	BuildReaction(chat, sender types.JID, id types.MessageID, reaction string) *waE2E.Message
}

type MediaFetcher func(ctx context.Context, url string) ([]byte, error)

func BuildOutbound(ctx context.Context, cli MessageBuilder, cmd amqp.GatewaySendCommand, fetch MediaFetcher) (types.JID, *waE2E.Message, []waBinary.Node, error) {
	to, err := types.ParseJID(cmd.To)
	if err != nil {
		return types.JID{}, nil, nil, fmt.Errorf("mapper: parse recipient jid %q: %w", cmd.To, err)
	}
	if to.User == "" {
		return types.JID{}, nil, nil, fmt.Errorf("mapper: invalid recipient jid %q", cmd.To)
	}

	switch cmd.Kind {
	case "edit":
		if cmd.TargetProviderMessageID == "" {
			return types.JID{}, nil, nil, fmt.Errorf("mapper: edit requires targetProviderMessageId")
		}
		return to, cli.BuildEdit(to, types.MessageID(cmd.TargetProviderMessageID), &waE2E.Message{Conversation: proto.String(cmd.Text)}), nil, nil
	case "revoke":
		if cmd.TargetProviderMessageID == "" {
			return types.JID{}, nil, nil, fmt.Errorf("mapper: revoke requires targetProviderMessageId")
		}
		return to, cli.BuildRevoke(to, types.EmptyJID, types.MessageID(cmd.TargetProviderMessageID)), nil, nil
	case "reaction":
		if cmd.TargetProviderMessageID == "" {
			return types.JID{}, nil, nil, fmt.Errorf("mapper: reaction requires targetProviderMessageId")
		}
		sender := types.EmptyJID
		if !cmd.TargetFromMe {
			sender = to
			if cmd.TargetParticipantJID != "" {
				participant, err := types.ParseJID(cmd.TargetParticipantJID)
				if err != nil {
					return types.JID{}, nil, nil, fmt.Errorf("mapper: parse reaction participant jid %q: %w", cmd.TargetParticipantJID, err)
				}
				sender = participant
			}
		}
		return to, cli.BuildReaction(to, sender, types.MessageID(cmd.TargetProviderMessageID), cmd.Emoji), nil, nil
	}

	msg, nodes, err := buildByType(ctx, cli, cmd, fetch)
	if err != nil {
		return types.JID{}, nil, nil, err
	}
	if cmd.Forwarded {
		msg = markForwarded(msg)
	}
	return to, msg, nodes, nil
}

func buildByType(ctx context.Context, up Uploader, cmd amqp.GatewaySendCommand, fetch MediaFetcher) (*waE2E.Message, []waBinary.Node, error) {
	switch cmd.Type {
	case "text":
		return buildText(cmd), nil, nil
	case "image":
		msg, err := buildMedia(ctx, up, fetch, cmd, whatsmeow.MediaImage)
		return msg, nil, err
	case "video":
		msg, err := buildMedia(ctx, up, fetch, cmd, whatsmeow.MediaVideo)
		return msg, nil, err
	case "audio":
		msg, err := buildMedia(ctx, up, fetch, cmd, whatsmeow.MediaAudio)
		return msg, nil, err
	case "document":
		msg, err := buildMedia(ctx, up, fetch, cmd, whatsmeow.MediaDocument)
		return msg, nil, err
	case "location":
		msg, err := buildLocation(cmd)
		return msg, nil, err
	case "contacts":
		msg, err := buildContacts(cmd)
		return msg, nil, err
	case "buttons":
		if cmd.Interactive == nil {
			return nil, nil, fmt.Errorf("mapper: type %q requires an interactive payload", cmd.Type)
		}
		return BuildButtons(*cmd.Interactive, contextInfoFor(cmd.ReplyTo))
	case "list":
		if cmd.Interactive == nil {
			return nil, nil, fmt.Errorf("mapper: type %q requires an interactive payload", cmd.Type)
		}
		return BuildList(*cmd.Interactive, contextInfoFor(cmd.ReplyTo))
	default:
		return nil, nil, fmt.Errorf("mapper: unsupported message type %q", cmd.Type)
	}
}

func mergeForwarded(ci *waE2E.ContextInfo) *waE2E.ContextInfo {
	if ci == nil {
		ci = &waE2E.ContextInfo{}
	}
	ci.IsForwarded = proto.Bool(true)
	ci.ForwardingScore = proto.Uint32(1)
	return ci
}

func markForwarded(msg *waE2E.Message) *waE2E.Message {
	switch {
	case msg.Conversation != nil:
		return &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: msg.Conversation, ContextInfo: mergeForwarded(nil)}}
	case msg.ExtendedTextMessage != nil:
		msg.ExtendedTextMessage.ContextInfo = mergeForwarded(msg.ExtendedTextMessage.GetContextInfo())
	case msg.ImageMessage != nil:
		msg.ImageMessage.ContextInfo = mergeForwarded(msg.ImageMessage.GetContextInfo())
	case msg.VideoMessage != nil:
		msg.VideoMessage.ContextInfo = mergeForwarded(msg.VideoMessage.GetContextInfo())
	case msg.AudioMessage != nil:
		msg.AudioMessage.ContextInfo = mergeForwarded(msg.AudioMessage.GetContextInfo())
	case msg.DocumentMessage != nil:
		msg.DocumentMessage.ContextInfo = mergeForwarded(msg.DocumentMessage.GetContextInfo())
	}
	return msg
}

func buildText(cmd amqp.GatewaySendCommand) *waE2E.Message {
	if cmd.ReplyTo == nil {
		return &waE2E.Message{Conversation: proto.String(cmd.Text)}
	}
	return &waE2E.Message{
		ExtendedTextMessage: &waE2E.ExtendedTextMessage{
			Text:        proto.String(cmd.Text),
			ContextInfo: contextInfoFor(cmd.ReplyTo),
		},
	}
}

func contextInfoFor(replyTo *amqp.ReplyToPayload) *waE2E.ContextInfo {
	if replyTo == nil {
		return nil
	}
	ctxInfo := &waE2E.ContextInfo{StanzaID: proto.String(replyTo.ProviderMessageID)}
	if replyTo.ParticipantJID != "" {
		ctxInfo.Participant = proto.String(replyTo.ParticipantJID)
	}
	return ctxInfo
}

func buildMedia(ctx context.Context, up Uploader, fetch MediaFetcher, cmd amqp.GatewaySendCommand, mt whatsmeow.MediaType) (*waE2E.Message, error) {
	if cmd.Media == nil {
		return nil, fmt.Errorf("mapper: type %q requires media", cmd.Type)
	}

	data, err := fetch(ctx, cmd.Media.URL)
	if err != nil {
		return nil, fmt.Errorf("mapper: fetch media: %w", err)
	}

	resp, err := up.Upload(ctx, data, mt)
	if err != nil {
		return nil, fmt.Errorf("mapper: upload media: %w", err)
	}

	ctxInfo := contextInfoFor(cmd.ReplyTo)

	switch cmd.Type {
	case "image":
		return &waE2E.Message{
			ImageMessage: &waE2E.ImageMessage{
				URL:           proto.String(resp.URL),
				DirectPath:    proto.String(resp.DirectPath),
				MediaKey:      resp.MediaKey,
				Mimetype:      proto.String(cmd.Media.Mime),
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    proto.Uint64(resp.FileLength),
				Caption:       optionalString(cmd.Text),
				ContextInfo:   ctxInfo,
			},
		}, nil
	case "video":
		return &waE2E.Message{
			VideoMessage: &waE2E.VideoMessage{
				URL:           proto.String(resp.URL),
				DirectPath:    proto.String(resp.DirectPath),
				MediaKey:      resp.MediaKey,
				Mimetype:      proto.String(cmd.Media.Mime),
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    proto.Uint64(resp.FileLength),
				Caption:       optionalString(cmd.Text),
				ContextInfo:   ctxInfo,
			},
		}, nil
	case "audio":
		return &waE2E.Message{
			AudioMessage: &waE2E.AudioMessage{
				URL:           proto.String(resp.URL),
				DirectPath:    proto.String(resp.DirectPath),
				MediaKey:      resp.MediaKey,
				Mimetype:      proto.String(cmd.Media.Mime),
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    proto.Uint64(resp.FileLength),
				ContextInfo:   ctxInfo,
			},
		}, nil
	default:
		return &waE2E.Message{
			DocumentMessage: &waE2E.DocumentMessage{
				URL:           proto.String(resp.URL),
				DirectPath:    proto.String(resp.DirectPath),
				MediaKey:      resp.MediaKey,
				Mimetype:      proto.String(cmd.Media.Mime),
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    proto.Uint64(resp.FileLength),
				FileName:      optionalString(cmd.Media.Filename),
				Caption:       optionalString(cmd.Text),
				ContextInfo:   ctxInfo,
			},
		}, nil
	}
}

func buildLocation(cmd amqp.GatewaySendCommand) (*waE2E.Message, error) {
	if cmd.Location == nil {
		return nil, fmt.Errorf("mapper: type %q requires location", cmd.Type)
	}
	return &waE2E.Message{
		LocationMessage: &waE2E.LocationMessage{
			DegreesLatitude:  proto.Float64(cmd.Location.Lat),
			DegreesLongitude: proto.Float64(cmd.Location.Lng),
			Name:             optionalString(cmd.Location.Name),
			Address:          optionalString(cmd.Location.Address),
			ContextInfo:      contextInfoFor(cmd.ReplyTo),
		},
	}, nil
}

func buildContacts(cmd amqp.GatewaySendCommand) (*waE2E.Message, error) {
	if len(cmd.Contacts) == 0 {
		return nil, fmt.Errorf("mapper: type %q requires at least one contact", cmd.Type)
	}

	if len(cmd.Contacts) == 1 {
		msg := contactMessage(cmd.Contacts[0])
		msg.ContextInfo = contextInfoFor(cmd.ReplyTo)
		return &waE2E.Message{ContactMessage: msg}, nil
	}

	contacts := make([]*waE2E.ContactMessage, len(cmd.Contacts))
	for i, c := range cmd.Contacts {
		contacts[i] = contactMessage(c)
	}
	return &waE2E.Message{
		ContactsArrayMessage: &waE2E.ContactsArrayMessage{
			Contacts:    contacts,
			ContextInfo: contextInfoFor(cmd.ReplyTo),
		},
	}, nil
}

func contactMessage(c amqp.ContactPayload) *waE2E.ContactMessage {
	return &waE2E.ContactMessage{
		DisplayName: proto.String(c.Name),
		Vcard:       proto.String(c.Vcard),
	}
}

func optionalString(s string) *string {
	if s == "" {
		return nil
	}
	return proto.String(s)
}
