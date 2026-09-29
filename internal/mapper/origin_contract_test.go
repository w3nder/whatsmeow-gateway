package mapper_test

import (
	"encoding/json"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
)

const callAutoReplyWireContract = `{"phoneNumberId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","from":"5511888887777","senderPn":"5511888887777","fromMe":true,"providerMessageId":"3EB0AUTOREPLY","timestamp":"1754300000","type":"text","text":{"body":"Não atendemos ligações, escreva aqui."},"origin":"call_auto_reply"}`

func TestInboundEventCallAutoReplyWireContract(t *testing.T) {
	evt := mapper.InboundEvent{
		PhoneNumberID:     "2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d",
		From:              "5511888887777",
		SenderPn:          "5511888887777",
		FromMe:            true,
		ProviderMessageID: "3EB0AUTOREPLY",
		Timestamp:         "1754300000",
		Type:              "text",
		Text:              &mapper.InboundText{Body: "Não atendemos ligações, escreva aqui."},
		Origin:            mapper.OriginCallAutoReply,
	}

	body, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(body) != callAutoReplyWireContract {
		t.Fatalf("wire shape\n got %s\nwant %s", body, callAutoReplyWireContract)
	}
}

func TestInboundEventOmitsTheOriginWhenItIsNotSet(t *testing.T) {
	body, err := json.Marshal(mapper.InboundEvent{PhoneNumberID: "c", From: "5511888887777", ProviderMessageID: "X", Timestamp: "1", Type: "text"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, present := decoded["origin"]; present {
		t.Fatalf("origin must be omitted for ordinary inbound events, got %s", body)
	}
}
