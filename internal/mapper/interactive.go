package mapper

import (
	"encoding/json"
	"fmt"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

type InteractiveButton = amqp.InteractiveButton

type InteractivePayload = amqp.InteractivePayload

const (
	maxButtons        = 3
	maxButtonTitleLen = 20
	maxBodyLen        = 1024
	maxFooterLen      = 60
)

func BuildButtons(p InteractivePayload) (*waE2E.Message, []waBinary.Node, error) {
	if p.Body == "" {
		return nil, nil, fmt.Errorf("mapper: buttons message requires a body")
	}
	if len(p.Buttons) == 0 {
		return nil, nil, fmt.Errorf("mapper: buttons message requires at least one button")
	}
	if len(p.Buttons) > maxButtons {
		return nil, nil, fmt.Errorf("mapper: buttons cannot exceed %d, got %d", maxButtons, len(p.Buttons))
	}

	nfButtons := make([]*waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton, 0, len(p.Buttons))
	for _, button := range p.Buttons {
		name, paramsJSON, err := buildNativeFlowButtonParams(button)
		if err != nil {
			return nil, nil, fmt.Errorf("mapper: encode button params: %w", err)
		}
		nfButtons = append(nfButtons, &waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton{
			Name:             proto.String(name),
			ButtonParamsJSON: proto.String(string(paramsJSON)),
		})
	}

	interactiveMessage := &waE2E.InteractiveMessage{
		Body: &waE2E.InteractiveMessage_Body{
			Text: proto.String(truncateRunes(p.Body, maxBodyLen)),
		},
		InteractiveMessage: &waE2E.InteractiveMessage_NativeFlowMessage_{
			NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{
				Buttons:           nfButtons,
				MessageParamsJSON: proto.String(""),
			},
		},
	}
	if p.Footer != "" {
		interactiveMessage.Footer = &waE2E.InteractiveMessage_Footer{
			Text: proto.String(truncateRunes(p.Footer, maxFooterLen)),
		}
	}

	message := &waE2E.Message{InteractiveMessage: interactiveMessage}

	nodes := []waBinary.Node{
		{
			Tag: "biz",
			Content: []waBinary.Node{
				{
					Tag:   "interactive",
					Attrs: waBinary.Attrs{"type": "native_flow", "v": "1"},
					Content: []waBinary.Node{
						{Tag: "native_flow", Attrs: waBinary.Attrs{"v": "9", "name": "mixed"}},
					},
				},
			},
		},
	}

	return message, nodes, nil
}

func buildNativeFlowButtonParams(button InteractiveButton) (string, []byte, error) {
	displayText := truncateRunes(button.Text, maxButtonTitleLen)

	if button.URL != "" {
		paramsJSON, err := json.Marshal(struct {
			DisplayText string `json:"display_text"`
			URL         string `json:"url"`
			MerchantURL string `json:"merchant_url"`
		}{displayText, button.URL, button.URL})
		return "cta_url", paramsJSON, err
	}

	paramsJSON, err := json.Marshal(struct {
		DisplayText string `json:"display_text"`
		ID          string `json:"id"`
	}{displayText, button.ID})
	return "quick_reply", paramsJSON, err
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}
