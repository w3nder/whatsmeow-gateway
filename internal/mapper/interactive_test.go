package mapper

import (
	"strings"
	"testing"

	waBinary "go.mau.fi/whatsmeow/binary"
)

func TestBuildButtonsMarksEachButtonWithItsNativeFlowName(t *testing.T) {
	msg, nodes, err := BuildButtons(InteractivePayload{
		Body:   "Como posso ajudar?",
		Footer: "Atendimento",
		Buttons: []InteractiveButton{
			{ID: "b1", Text: "Quero saber mais"},
			{Text: "Ver catálogo", URL: "https://exemplo.com/cat"},
		},
	})
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
	if _, _, err := BuildButtons(InteractivePayload{Body: "oi"}); err == nil {
		t.Fatal("expected an error with no buttons")
	}
	four := []InteractiveButton{{Text: "a"}, {Text: "b"}, {Text: "c"}, {Text: "d"}}
	if _, _, err := BuildButtons(InteractivePayload{Body: "oi", Buttons: four}); err == nil {
		t.Fatalf("expected an error above %d buttons", maxButtons)
	}
}

func TestBuildButtonsRefusesABodylessMessage(t *testing.T) {
	if _, _, err := BuildButtons(InteractivePayload{Buttons: []InteractiveButton{{Text: "a"}}}); err == nil {
		t.Fatal("expected an error with no body")
	}
}
