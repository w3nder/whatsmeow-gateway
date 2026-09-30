package test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

func TestPublisherHistoryBatchAndDoneAreConfirmedWithTheirKindAndSource(t *testing.T) {
	conn := startRabbitMQ(t)

	publisher, err := gatewayamqp.NewPublisher(conn)
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("publisher.Close failed: %v", err)
		}
	})

	probeCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open probe channel: %v", err)
	}
	t.Cleanup(func() {
		if err := probeCh.Close(); err != nil && !errors.Is(err, rabbitmq.ErrClosed) {
			t.Errorf("failed to close probe channel: %v", err)
		}
	})
	probeQ, err := probeCh.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("failed to declare probe queue: %v", err)
	}
	if err := probeCh.QueueBind(probeQ.Name, gatewayamqp.HistoryRoutingKey, gatewayamqp.EventsExchange, false, nil); err != nil {
		t.Fatalf("failed to bind probe queue: %v", err)
	}
	deliveries, err := probeCh.Consume(probeQ.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume probe queue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := publisher.PublishHistoryBatch(ctx, gatewayamqp.HistoryBatch{
		TenantID:       "tenant-history",
		ChannelID:      "channel-history",
		ImportID:       "import-history",
		ChunkOrder:     1,
		SourceProgress: 30,
		BatchIndex:     1,
		BatchesInChunk: 1,
		Chats:          []gatewayamqp.HistoryChat{},
	}); err != nil {
		t.Fatalf("PublishHistoryBatch failed: %v", err)
	}
	batch := waitForDelivery(t, deliveries, gatewayamqp.HistoryRoutingKey, 10*time.Second)
	if batch.DeliveryMode != rabbitmq.Persistent {
		t.Fatalf("a history batch must be persistent, got delivery mode %d", batch.DeliveryMode)
	}
	var gotBatch map[string]any
	if err := json.Unmarshal(batch.Body, &gotBatch); err != nil {
		t.Fatalf("unmarshal batch: %v", err)
	}
	if gotBatch["kind"] != gatewayamqp.HistoryBatchKind || gotBatch["source"] != gatewayamqp.HistorySourceGateway || gotBatch["importId"] != "import-history" {
		t.Fatalf("batch carries the wrong header: %v", gotBatch)
	}

	if err := publisher.PublishHistoryDone(ctx, gatewayamqp.HistoryDone{
		TenantID:     "tenant-history",
		ChannelID:    "channel-history",
		ImportID:     "import-history",
		TotalBatches: 1,
	}); err != nil {
		t.Fatalf("PublishHistoryDone failed: %v", err)
	}
	done := waitForDelivery(t, deliveries, gatewayamqp.HistoryRoutingKey, 10*time.Second)
	var gotDone map[string]any
	if err := json.Unmarshal(done.Body, &gotDone); err != nil {
		t.Fatalf("unmarshal done: %v", err)
	}
	if gotDone["kind"] != gatewayamqp.HistoryDoneKind || gotDone["source"] != gatewayamqp.HistorySourceGateway || gotDone["totalBatches"] != float64(1) {
		t.Fatalf("done carries the wrong fields: %v", gotDone)
	}
}
