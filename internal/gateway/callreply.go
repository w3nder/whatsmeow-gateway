package gateway

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
	"github.com/w3nder/whatsmeow-gateway/internal/dedupe"
	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
	"github.com/w3nder/whatsmeow-gateway/internal/senderid"
)

const callAutoReplyDedupePrefix = "call-auto-reply:"

type textSender interface {
	Send(ctx context.Context, channelID string, to types.JID, msg *waE2E.Message, id string, nodes []waBinary.Node) (string, time.Time, error)
}

type inboundPublisher interface {
	PublishInbound(ctx context.Context, evt any) error
}

type callAutoReplier struct {
	sender    textSender
	publisher inboundPublisher
	timeout   time.Duration
}

func (r callAutoReplier) Reply(ctx context.Context, reply call.AutoReply) error {
	to, err := replyRecipient(reply)
	if err != nil {
		return err
	}

	sendCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	providerID := dedupe.DeterministicProviderID(callAutoReplyDedupePrefix + reply.CallID)
	msg := &waE2E.Message{Conversation: proto.String(reply.Text)}
	id, sentAt, err := r.sender.Send(sendCtx, reply.ChannelID, to, msg, providerID, nil)
	if err != nil {
		return fmt.Errorf("gateway: send call auto reply for call %s: %w", reply.CallID, err)
	}

	evt := mapper.InboundEvent{
		PhoneNumberID:     reply.ChannelID,
		From:              senderid.From(reply.SenderLid, reply.SenderPn),
		SenderLid:         reply.SenderLid,
		SenderPn:          reply.SenderPn,
		FromMe:            true,
		ProviderMessageID: id,
		Timestamp:         strconv.FormatInt(sentAt.Unix(), 10),
		Type:              "text",
		Text:              &mapper.InboundText{Body: reply.Text},
		Origin:            mapper.OriginCallAutoReply,
	}
	if err := r.publisher.PublishInbound(ctx, evt); err != nil {
		return fmt.Errorf("gateway: publish call auto reply for call %s: %w", reply.CallID, err)
	}
	return nil
}

func replyRecipient(reply call.AutoReply) (types.JID, error) {
	switch {
	case reply.SenderPn != "":
		return types.NewJID(reply.SenderPn, types.DefaultUserServer), nil
	case reply.SenderLid != "":
		return types.NewJID(reply.SenderLid, types.HiddenUserServer), nil
	default:
		return types.JID{}, errors.New("gateway: call auto reply has no recipient")
	}
}
