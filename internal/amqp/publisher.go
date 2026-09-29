package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	rabbitmq "github.com/rabbitmq/amqp091-go"

	"github.com/w3nder/whatsmeow-gateway/internal/avatar"
)

type ChannelQREvent struct {
	TenantID  string `json:"tenantId"`
	UserID    string `json:"userId"`
	ChannelID string `json:"channelId"`
	QR        string `json:"qr"`
}

type ChannelStatusEvent struct {
	TenantID       string          `json:"tenantId"`
	UserID         string          `json:"userId,omitempty"`
	ChannelID      string          `json:"channelId"`
	Status         string          `json:"status"`
	Reason         string          `json:"reason,omitempty"`
	PhoneNumber    string          `json:"phoneNumber,omitempty"`
	DisplayName    string          `json:"displayName,omitempty"`
	ProfilePicture *avatar.Picture `json:"profilePicture,omitempty"`
}

type Publisher struct {
	channel   *rabbitmq.Channel
	confirmed *rabbitmq.Channel
}

func NewPublisher(conn *rabbitmq.Connection) (*Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("amqp: open publisher channel: %w", err)
	}
	if err := assertEventsExchange(ch); err != nil {
		_ = ch.Close()
		return nil, err
	}
	confirmed, err := conn.Channel()
	if err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("amqp: open confirmed publisher channel: %w", err)
	}
	if err := confirmed.Confirm(false); err != nil {
		_ = ch.Close()
		_ = confirmed.Close()
		return nil, fmt.Errorf("amqp: enable confirms on the publisher: %w", err)
	}
	return &Publisher{channel: ch, confirmed: confirmed}, nil
}

func (p *Publisher) PublishInbound(ctx context.Context, evt any) error {
	return p.publish(ctx, InboundRoutingKey, evt)
}

func (p *Publisher) PublishStatus(ctx context.Context, evt any) error {
	return p.publish(ctx, StatusRoutingKey, evt)
}

func (p *Publisher) PublishGroupInbound(ctx context.Context, evt any) error {
	return p.publish(ctx, GroupInboundRoutingKey, evt)
}

func (p *Publisher) PublishGroupStatus(ctx context.Context, evt any) error {
	return p.publish(ctx, GroupStatusRoutingKey, evt)
}

func (p *Publisher) PublishCall(ctx context.Context, evt any) error {
	return p.publish(ctx, CallRoutingKey, evt)
}

func (p *Publisher) PublishChannelQR(ctx context.Context, evt ChannelQREvent) error {
	return p.publish(ctx, ChannelQRRoutingKey, evt)
}

func (p *Publisher) PublishChannelStatus(ctx context.Context, evt ChannelStatusEvent) error {
	return p.publish(ctx, ChannelStatusRoutingKey, evt)
}

func (p *Publisher) PublishGroupAction(ctx context.Context, evt GroupActionEvent) error {
	return p.publish(ctx, GroupActionRoutingKey, evt)
}

func (p *Publisher) PublishGroupParticipants(ctx context.Context, evt GroupParticipantsEvent) error {
	if evt.Participants == nil {
		evt.Participants = []GroupParticipant{}
	}
	return p.publish(ctx, GroupParticipantsRoutingKey, evt)
}

func (p *Publisher) PublishHistoryBatch(ctx context.Context, batch HistoryBatch) error {
	batch.Kind = HistoryBatchKind
	batch.Source = HistorySourceGateway
	return p.publishConfirmed(ctx, HistoryRoutingKey, batch)
}

func (p *Publisher) PublishHistoryDone(ctx context.Context, done HistoryDone) error {
	done.Kind = HistoryDoneKind
	done.Source = HistorySourceGateway
	return p.publishConfirmed(ctx, HistoryRoutingKey, done)
}

func (p *Publisher) publishConfirmed(ctx context.Context, routingKey string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("amqp: marshal %s payload: %w", routingKey, err)
	}
	confirm, err := p.confirmed.PublishWithDeferredConfirmWithContext(ctx, EventsExchange, routingKey, false, false, rabbitmq.Publishing{
		ContentType:  "application/json",
		DeliveryMode: rabbitmq.Persistent,
		Body:         body,
	})
	if err != nil {
		return fmt.Errorf("amqp: publish %s: %w", routingKey, err)
	}
	acked, err := confirm.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("amqp: wait %s confirm: %w", routingKey, err)
	}
	if !acked {
		return fmt.Errorf("amqp: broker refused %s", routingKey)
	}
	return nil
}

func (p *Publisher) publish(ctx context.Context, routingKey string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("amqp: marshal %s payload: %w", routingKey, err)
	}
	err = p.channel.PublishWithContext(ctx, EventsExchange, routingKey, false, false, rabbitmq.Publishing{
		ContentType:  "application/json",
		DeliveryMode: rabbitmq.Persistent,
		Body:         body,
	})
	if err != nil {
		return fmt.Errorf("amqp: publish %s: %w", routingKey, err)
	}
	return nil
}

func (p *Publisher) Close() error {
	return errors.Join(p.confirmed.Close(), p.channel.Close())
}
