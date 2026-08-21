package amqp

import (
	"context"
	"encoding/json"
	"fmt"

	rabbitmq "github.com/rabbitmq/amqp091-go"
)

type ChannelQREvent struct {
	TenantID  string `json:"tenantId"`
	UserID    string `json:"userId"`
	ChannelID string `json:"channelId"`
	QR        string `json:"qr"`
}

type ChannelStatusEvent struct {
	TenantID  string `json:"tenantId"`
	UserID    string `json:"userId,omitempty"`
	ChannelID string `json:"channelId"`
	Status    string `json:"status"`
	Reason    string `json:"reason,omitempty"`
	// Numero conectado, so no status "connected". Vazio quando a sessao ainda nao existe.
	PhoneNumber string `json:"phoneNumber,omitempty"`
}

type Publisher struct {
	channel *rabbitmq.Channel
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
	return &Publisher{channel: ch}, nil
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
	return p.channel.Close()
}
