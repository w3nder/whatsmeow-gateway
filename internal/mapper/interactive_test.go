package mapper

import (
	"strings"
	"testing"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

func TestBuildButtonsMarksEachButtonWithItsNativeFlowName(t *testing.T) {
	msg, nodes, err := BuildButtons(InteractivePayload{
		Body:   "Como posso ajudar?",
		Footer: "Atendimento",
		Buttons: []InteractiveButton{
			{ID: "b1", Text: "Quero saber mais"},
			{Text: "Ver catálogo", URL: "https://exemplo.com/cat"},
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	im := msg.GetInteractiveMessage()
	if im.GetBody().GetText() != "Como posso ajudar?" {
		t.Fatalf("body = %q", im.GetBody().GetText())
	}
	if im.GetFooter().GetText() != "Atendimento" {
		t.Fatalf("footer = %q", im.GetFooter().GetText())
	}

	buttons := im.GetNativeFlowMessage().GetButtons()
	if len(buttons) != 2 {
		t.Fatalf("expected 2 buttons, got %d", len(buttons))
	}
	if buttons[0].GetName() != "quick_reply" {
		t.Fatalf("button 0 name = %q", buttons[0].GetName())
	}
	if buttons[1].GetName() != "cta_url" {
		t.Fatalf("button 1 name = %q", buttons[1].GetName())
	}
	if !strings.Contains(buttons[1].GetButtonParamsJSON(), "https://exemplo.com/cat") {
		t.Fatalf("cta_url params = %q", buttons[1].GetButtonParamsJSON())
	}

	if len(nodes) != 1 {
		t.Fatalf("expected exactly one top-level node, got %+v", nodes)
	}
	biz := nodes[0]
	if biz.Tag != "biz" {
		t.Fatalf("outer tag = %q", biz.Tag)
	}
	bizContent, ok := biz.Content.([]waBinary.Node)
	if !ok || len(bizContent) != 1 {
		t.Fatalf("biz content = %+v", biz.Content)
	}
	interactive := bizContent[0]
	if interactive.Tag != "interactive" {
		t.Fatalf("interactive tag = %q", interactive.Tag)
	}
	if interactive.Attrs["type"] != "native_flow" || interactive.Attrs["v"] != "1" {
		t.Fatalf("interactive attrs = %+v", interactive.Attrs)
	}
	interactiveContent, ok := interactive.Content.([]waBinary.Node)
	if !ok || len(interactiveContent) != 1 {
		t.Fatalf("interactive content = %+v", interactive.Content)
	}
	nativeFlow := interactiveContent[0]
	if nativeFlow.Tag != "native_flow" {
		t.Fatalf("native_flow tag = %q", nativeFlow.Tag)
	}
	if nativeFlow.Attrs["v"] != "9" || nativeFlow.Attrs["name"] != "mixed" {
		t.Fatalf("native_flow attrs = %+v", nativeFlow.Attrs)
	}
}

func TestBuildButtonsRefusesAnEmptyOrOversizedSet(t *testing.T) {
	if _, _, err := BuildButtons(InteractivePayload{Body: "oi"}, nil); err == nil {
		t.Fatal("expected an error with no buttons")
	}
	four := []InteractiveButton{{Text: "a"}, {Text: "b"}, {Text: "c"}, {Text: "d"}}
	if _, _, err := BuildButtons(InteractivePayload{Body: "oi", Buttons: four}, nil); err == nil {
		t.Fatalf("expected an error above %d buttons", maxButtons)
	}
}

func TestBuildButtonsRefusesABodylessMessage(t *testing.T) {
	if _, _, err := BuildButtons(InteractivePayload{Buttons: []InteractiveButton{{Text: "a"}}}, nil); err == nil {
		t.Fatal("expected an error with no body")
	}
}

func TestBuildButtonsFallsBackToTheDisplayTextWhenAButtonHasNoID(t *testing.T) {
	msg, _, err := BuildButtons(InteractivePayload{
		Body:    "Confirma?",
		Buttons: []InteractiveButton{{Text: "Sim"}, {Text: "Não"}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	buttons := msg.GetInteractiveMessage().GetNativeFlowMessage().GetButtons()
	if len(buttons) != 2 {
		t.Fatalf("expected 2 buttons, got %d", len(buttons))
	}
	if !strings.Contains(buttons[0].GetButtonParamsJSON(), `"id":"Sim"`) {
		t.Fatalf("button 0 params = %q", buttons[0].GetButtonParamsJSON())
	}
	if !strings.Contains(buttons[1].GetButtonParamsJSON(), `"id":"Não"`) {
		t.Fatalf("button 1 params = %q", buttons[1].GetButtonParamsJSON())
	}
}

func TestBuildListKeepsSectionsAndRowsInOrder(t *testing.T) {
	msg, nodes, err := BuildList(InteractivePayload{
		Body:       "Escolha um horário",
		Footer:     "Atendimento",
		ButtonText: "Ver horários",
		Sections: []amqp.InteractiveSection{
			{Title: "Manhã", Rows: []amqp.InteractiveRow{{ID: "m1", Title: "09:00"}, {ID: "m2", Title: "10:00", Description: "última vaga"}}},
			{Title: "Tarde", Rows: []amqp.InteractiveRow{{ID: "t1", Title: "14:00"}}},
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	list := msg.GetDocumentWithCaptionMessage().GetMessage().GetListMessage()
	if list == nil {
		t.Fatal("a list must travel inside DocumentWithCaptionMessage, or whatsapp will not render it")
	}
	if list.GetButtonText() != "Ver horários" {
		t.Fatalf("buttonText = %q", list.GetButtonText())
	}
	if list.GetListType() != waE2E.ListMessage_SINGLE_SELECT {
		t.Fatalf("listType = %v", list.GetListType())
	}

	sections := list.GetSections()
	if len(sections) != 2 || sections[0].GetTitle() != "Manhã" || sections[1].GetTitle() != "Tarde" {
		t.Fatalf("sections = %+v", sections)
	}
	rows := sections[0].GetRows()
	if len(rows) != 2 || rows[0].GetTitle() != "09:00" || rows[1].GetDescription() != "última vaga" {
		t.Fatalf("rows = %+v", rows)
	}

	if len(nodes) == 0 || nodes[0].Tag != "biz" {
		t.Fatalf("expected the biz node, got %+v", nodes)
	}
	bizContent, ok := nodes[0].Content.([]waBinary.Node)
	if !ok || len(bizContent) != 1 {
		t.Fatalf("biz content = %+v", nodes[0].Content)
	}
	listNode := bizContent[0]
	if listNode.Tag != "list" {
		t.Fatalf("expected the list node, got %q", listNode.Tag)
	}
	if listNode.Attrs["type"] != "product_list" || listNode.Attrs["v"] != "2" {
		t.Fatalf("list attrs = %+v", listNode.Attrs)
	}
}

func TestBuildListRefusesMoreRowsThanWhatsappAccepts(t *testing.T) {
	rows := make([]amqp.InteractiveRow, maxListRows+1)
	for i := range rows {
		rows[i] = amqp.InteractiveRow{Title: "linha"}
	}
	if _, _, err := BuildList(InteractivePayload{
		Body:       "oi",
		ButtonText: "abrir",
		Sections:   []amqp.InteractiveSection{{Rows: rows}},
	}, nil); err == nil {
		t.Fatalf("expected an error above %d rows", maxListRows)
	}
}

func TestBuildListCountsRowsAcrossAllSections(t *testing.T) {
	firstSectionRows := make([]amqp.InteractiveRow, maxListRows)
	for i := range firstSectionRows {
		firstSectionRows[i] = amqp.InteractiveRow{Title: "linha"}
	}
	if _, _, err := BuildList(InteractivePayload{
		Body:       "oi",
		ButtonText: "abrir",
		Sections: []amqp.InteractiveSection{
			{Title: "A", Rows: firstSectionRows},
			{Title: "B", Rows: []amqp.InteractiveRow{{Title: "extra"}}},
		},
	}, nil); err == nil {
		t.Fatalf("expected an error when the sum across sections exceeds %d rows, each section individually staying under the limit", maxListRows)
	}
}

func TestBuildListRefusesAnEmptyListOrAMissingButtonText(t *testing.T) {
	if _, _, err := BuildList(InteractivePayload{Body: "oi", ButtonText: "abrir"}, nil); err == nil {
		t.Fatal("expected an error with no sections")
	}
	if _, _, err := BuildList(InteractivePayload{
		Body:     "oi",
		Sections: []amqp.InteractiveSection{{Rows: []amqp.InteractiveRow{{Title: "a"}}}},
	}, nil); err == nil {
		t.Fatal("expected an error with no button text")
	}
}

func TestBuildListRefusesABodylessMessage(t *testing.T) {
	if _, _, err := BuildList(InteractivePayload{
		ButtonText: "abrir",
		Sections:   []amqp.InteractiveSection{{Rows: []amqp.InteractiveRow{{Title: "a"}}}},
	}, nil); err == nil {
		t.Fatal("expected an error with no body")
	}
}
