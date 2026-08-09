package test

import (
	"context"
	"testing"
	"time"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

// A broker restart closes the delivery channel, which ends the consumer's range loop.
// That used to happen silently and leave the gateway running while consuming nothing,
// so the consumer has to report it.
func TestConsumerReportsFailureWhenBrokerConnectionDies(t *testing.T) {
	conn := startRabbitMQ(t)

	consumer, err := gatewayamqp.NewConsumer(conn, gatewayamqp.ConsumerConfig{Prefetch: 10})
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}

	if err := consumer.StartSend(context.Background(), func(_ context.Context, _ gatewayamqp.GatewaySendCommand) error {
		return nil
	}); err != nil {
		t.Fatalf("StartSend failed: %v", err)
	}
	if err := consumer.StartPair(context.Background(), func(_ context.Context, _ gatewayamqp.PairCommand, accept func()) error {
		accept()
		return nil
	}); err != nil {
		t.Fatalf("StartPair failed: %v", err)
	}

	// The broker going away looks exactly like this from the client's side.
	if err := conn.Close(); err != nil {
		t.Fatalf("failed to close the rabbitmq connection: %v", err)
	}

	select {
	case failure := <-consumer.Failed():
		if failure == nil {
			t.Fatal("expected a non-nil failure describing why the consumer stopped")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("consumer stopped consuming without reporting it: the gateway would keep running as a zombie, acking nothing")
	}
}

func TestConsumerGracefulCloseReportsNoFailure(t *testing.T) {
	conn := startRabbitMQ(t)

	consumer, err := gatewayamqp.NewConsumer(conn, gatewayamqp.ConsumerConfig{Prefetch: 10})
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}

	if err := consumer.StartSend(context.Background(), func(_ context.Context, _ gatewayamqp.GatewaySendCommand) error {
		return nil
	}); err != nil {
		t.Fatalf("StartSend failed: %v", err)
	}

	if err := consumer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	select {
	case failure := <-consumer.Failed():
		t.Fatalf("expected no failure on a graceful shutdown, got %v", failure)
	case <-time.After(500 * time.Millisecond):
	}
}
