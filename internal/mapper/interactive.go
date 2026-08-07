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

type InteractiveRow = amqp.InteractiveRow

type InteractiveSection = amqp.InteractiveSection

const (
	maxButtons        = 3
	maxButtonTitleLen = 20
	maxBodyLen        = 1024
	maxFooterLen      = 60
	maxListRows       = 10
	maxListTitleLen   = 24
	maxListDescLen    = 72
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

func BuildList(p InteractivePayload) (*waE2E.Message, []waBinary.Node, error) {
	if p.ButtonText == "" {
		return nil, nil, fmt.Errorf("mapper: list message requires a button text")
	}
	if len(p.Sections) == 0 {
		return nil, nil, fmt.Errorf("mapper: list message requires at least one section")
	}

	waSections := make([]*waE2E.ListMessage_Section, 0, len(p.Sections))
	totalRows := 0
	for _, section := range p.Sections {
		waRows := make([]*waE2E.ListMessage_Row, 0, len(section.Rows))
		for _, row := range section.Rows {
			totalRows++
			if totalRows > maxListRows {
				return nil, nil, fmt.Errorf("mapper: list rows cannot exceed %d", maxListRows)
			}

			rowID := row.ID
			if rowID == "" {
				rowID = row.Title
			}
			waRows = append(waRows, &waE2E.ListMessage_Row{
				RowID:       proto.String(rowID),
				Title:       proto.String(truncateRunes(row.Title, maxListTitleLen)),
				Description: proto.String(truncateRunes(row.Description, maxListDescLen)),
			})
		}
		if len(waRows) == 0 {
			continue
		}
		waSections = append(waSections, &waE2E.ListMessage_Section{
			Title: proto.String(truncateRunes(section.Title, maxListTitleLen)),
			Rows:  waRows,
		})
	}

	if len(waSections) == 0 {
		return nil, nil, fmt.Errorf("mapper: list message requires at least one row")
	}

	listMessage := &waE2E.ListMessage{
		Description: proto.String(truncateRunes(p.Body, maxBodyLen)),
		ButtonText:  proto.String(truncateRunes(p.ButtonText, maxButtonTitleLen)),
		ListType:    waE2E.ListMessage_SINGLE_SELECT.Enum(),
		Sections:    waSections,
	}
	if p.Footer != "" {
		listMessage.FooterText = proto.String(truncateRunes(p.Footer, maxFooterLen))
	}

	message := &waE2E.Message{
		DocumentWithCaptionMessage: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{
				ListMessage: listMessage,
			},
		},
	}

	nodes := []waBinary.Node{
		{
			Tag: "biz",
			Content: []waBinary.Node{
				{Tag: "list", Attrs: waBinary.Attrs{"type": "product_list", "v": "2"}},
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
