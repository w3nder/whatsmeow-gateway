package amqp

import (
	"fmt"

	rabbitmq "github.com/rabbitmq/amqp091-go"
)

const EventsExchange = "sender.events"

const (
	InboundRoutingKey       = "whatsapp.inbound.v1"
	StatusRoutingKey        = "whatsapp.status.v1"
	CallRoutingKey          = "whatsapp.call.v1"
	ChannelQRRoutingKey     = "channel.qr"
	ChannelStatusRoutingKey = "channel.status"
)

const (
	GatewaySendExchange = "whatsapp.gateway.send.v1"
	GatewaySendQueue    = "gateway.send"
	GatewaySendDLX      = "gateway.send.dlx"
	GatewaySendDLQ      = "gateway.send.dlq"
	GatewaySendConsumer = "whatsmeow-gateway.send"

	// Call commands get their own queue rather than sharing gateway.send: a call
	// is long-lived, and letting one sit in a shared prefetch would stall
	// message delivery behind it.
	GatewayCallExchange = "whatsapp.gateway.call.v1"
	GatewayCallQueue    = "gateway.call"
	GatewayCallDLX      = "gateway.call.dlx"
	GatewayCallDLQ      = "gateway.call.dlq"
	GatewayCallConsumer = "whatsmeow-gateway.call"

	GatewayPairExchange = "whatsapp.gateway.pair.v1"
	GatewayPairQueue    = "gateway.pair"
	GatewayPairDLX      = "gateway.pair.dlx"
	GatewayPairDLQ      = "gateway.pair.dlq"
	GatewayPairConsumer = "whatsmeow-gateway.pair"
)

func assertEventsExchange(ch *rabbitmq.Channel) error {
	if err := ch.ExchangeDeclare(EventsExchange, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("amqp: declare %s exchange: %w", EventsExchange, err)
	}
	return nil
}

func declareCommandTopology(ch *rabbitmq.Channel, exchange, queue, dlx, dlq string) error {
	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("amqp: declare %s exchange: %w", exchange, err)
	}
	if err := ch.ExchangeDeclare(dlx, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("amqp: declare %s exchange: %w", dlx, err)
	}
	if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
		return fmt.Errorf("amqp: declare %s queue: %w", dlq, err)
	}
	if err := ch.QueueBind(dlq, "#", dlx, false, nil); err != nil {
		return fmt.Errorf("amqp: bind %s to %s: %w", dlq, dlx, err)
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, rabbitmq.Table{
		"x-queue-type":           "quorum",
		"x-dead-letter-exchange": dlx,
	}); err != nil {
		return fmt.Errorf("amqp: declare %s queue: %w", queue, err)
	}
	if err := ch.QueueBind(queue, "#", exchange, false, nil); err != nil {
		return fmt.Errorf("amqp: bind %s to %s: %w", queue, exchange, err)
	}
	return nil
}
