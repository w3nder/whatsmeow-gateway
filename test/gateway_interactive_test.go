package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"
	"go.mau.fi/whatsmeow"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/dedupe"
	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
)

func TestGatewaySendHandlerForwardsInteractiveNodesToWAClient(t *testing.T) {
	const messageID = "msg-interactive-buttons-1"
	expectedProviderID := dedupe.DeterministicProviderID(messageID)

	fake := newFakeWAClient()
	fake.sendResp = whatsmeow.SendResponse{ID: expectedProviderID, Timestamp: time.Now().Truncate(time.Second)}

	probeCh, deliveries, _, cancel, runErrCh := setupStatusRoundtripGateway(t, fake, "channel-interactive-buttons-1")

	sendCmd := gatewayamqp.GatewaySendCommand{
		TenantID:  "tenant-interactive-buttons-1",
		ChannelID: "channel-interactive-buttons-1",
		MessageID: messageID,
		To:        "15551234567@s.whatsapp.net",
		Type:      "buttons",
		Interactive: &gatewayamqp.InteractivePayload{
			Body:    "escolha uma opcao",
			Buttons: []gatewayamqp.InteractiveButton{{Text: "sim"}},
		},
	}
	sendBody, err := json.Marshal(sendCmd)
	if err != nil {
		t.Fatalf("failed to marshal send command: %v", err)
	}

	publishCtx, publishCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer publishCancel()
	if err := probeCh.PublishWithContext(publishCtx, gatewayamqp.GatewaySendExchange, "0", false, false, rabbitmq.Publishing{
		ContentType: "application/json",
		Body:        sendBody,
	}); err != nil {
		t.Fatalf("failed to publish send command: %v", err)
	}

	delivery := waitForDelivery(t, deliveries, gatewayamqp.StatusRoutingKey, 10*time.Second)
	var evt mapper.StatusEvent
	if err := json.Unmarshal(delivery.Body, &evt); err != nil {
		t.Fatalf("failed to unmarshal whatsapp.status.v1 event: %v", err)
	}
	if evt.Status != "sent" {
		t.Fatalf("expected status=sent, got %+v", evt)
	}
	if evt.ProviderMessageID != expectedProviderID {
		t.Fatalf("expected providerMessageId=%s, got %+v", expectedProviderID, evt)
	}

	if fake.sendCallCount() != 1 {
		t.Fatalf("expected WAClient.SendMessage called exactly once, got %d", fake.sendCallCount())
	}
	if len(fake.lastNodes) == 0 {
		t.Fatal("expected the buttons send to carry its binary nodes through to WAClient.SendMessage, got none")
	}
	if fake.lastNodes[0].Tag != "biz" {
		t.Fatalf("expected the outer node tag to be biz, got %q", fake.lastNodes[0].Tag)
	}

	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
}
