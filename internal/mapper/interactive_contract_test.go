package mapper_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"go.mau.fi/whatsmeow"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
)

const interactiveButtonsSendCommandLiteral = `{"tenantId":"019f581e-778f-7a3d-af09-73ec4ede1d34","channelId":"019fa1e5-5cd0-72b9-ac61-2e791173416a","messageId":"019fda3c-3ada-7a73-a481-1003860cf9d5","to":"5511999998888@s.whatsapp.net","type":"buttons","interactive":{"body":"Como posso ajudar?","footer":"Atendimento","buttons":[{"id":"b1","text":"Quero saber mais"},{"text":"Ver catálogo","url":"https://exemplo.com/cat"}]}}`

func TestInteractiveButtonsSendCommandQueueContractBuildsQuickReplyAndCtaURLButtons(t *testing.T) {
	var cmd amqp.GatewaySendCommand
	if err := json.Unmarshal([]byte(interactiveButtonsSendCommandLiteral), &cmd); err != nil {
		t.Fatalf("unmarshal contract literal: %v", err)
	}

	expected := amqp.GatewaySendCommand{
		TenantID:  "019f581e-778f-7a3d-af09-73ec4ede1d34",
		ChannelID: "019fa1e5-5cd0-72b9-ac61-2e791173416a",
		MessageID: "019fda3c-3ada-7a73-a481-1003860cf9d5",
		To:        "5511999998888@s.whatsapp.net",
		Type:      "buttons",
		Interactive: &amqp.InteractivePayload{
			Body:   "Como posso ajudar?",
			Footer: "Atendimento",
			Buttons: []amqp.InteractiveButton{
				{ID: "b1", Text: "Quero saber mais"},
				{Text: "Ver catálogo", URL: "https://exemplo.com/cat"},
			},
		},
	}
	if !reflect.DeepEqual(cmd, expected) {
		t.Fatalf("contract literal decoded to %+v, want %+v", cmd, expected)
	}

	var cli *whatsmeow.Client
	to, msg, _, err := mapper.BuildOutbound(context.Background(), cli, cmd, stubFetch(nil, nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	if to.String() != "5511999998888@s.whatsapp.net" {
		t.Fatalf("expected recipient 5511999998888@s.whatsapp.net, got %q", to.String())
	}

	interactive := msg.GetInteractiveMessage()
	if interactive == nil {
		t.Fatalf("expected an InteractiveMessage, got %+v", msg)
	}
	if interactive.GetBody().GetText() != "Como posso ajudar?" {
		t.Fatalf("expected body %q, got %q", "Como posso ajudar?", interactive.GetBody().GetText())
	}
	if interactive.GetFooter().GetText() != "Atendimento" {
		t.Fatalf("expected footer %q, got %q", "Atendimento", interactive.GetFooter().GetText())
	}

	buttons := interactive.GetNativeFlowMessage().GetButtons()
	if len(buttons) != 2 {
		t.Fatalf("expected 2 buttons, got %d", len(buttons))
	}

	first := buttons[0]
	if first.GetName() != "quick_reply" {
		t.Fatalf("expected the first button to be quick_reply, got %q", first.GetName())
	}
	var firstParams struct {
		DisplayText string `json:"display_text"`
		ID          string `json:"id"`
	}
	if err := json.Unmarshal([]byte(first.GetButtonParamsJSON()), &firstParams); err != nil {
		t.Fatalf("unmarshal quick_reply params: %v", err)
	}
	if firstParams.ID != "b1" {
		t.Fatalf("expected quick_reply id %q, got %q", "b1", firstParams.ID)
	}
	if firstParams.DisplayText != "Quero saber mais" {
		t.Fatalf("expected quick_reply display text %q, got %q", "Quero saber mais", firstParams.DisplayText)
	}

	second := buttons[1]
	if second.GetName() != "cta_url" {
		t.Fatalf("expected the second button to be cta_url, got %q", second.GetName())
	}
	var secondParams struct {
		DisplayText string `json:"display_text"`
		URL         string `json:"url"`
		MerchantURL string `json:"merchant_url"`
	}
	if err := json.Unmarshal([]byte(second.GetButtonParamsJSON()), &secondParams); err != nil {
		t.Fatalf("unmarshal cta_url params: %v", err)
	}
	if secondParams.URL != "https://exemplo.com/cat" {
		t.Fatalf("expected cta_url url %q, got %q", "https://exemplo.com/cat", secondParams.URL)
	}
	if secondParams.DisplayText != "Ver catálogo" {
		t.Fatalf("expected cta_url display text %q, got %q", "Ver catálogo", secondParams.DisplayText)
	}
}
