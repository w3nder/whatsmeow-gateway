package mapper_test

import (
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
)

func TestChangesAnotherMessageOnlyForLifecycleEvents(t *testing.T) {
	cases := map[string]bool{
		"reaction":    true,
		"edit":        true,
		"revoke":      true,
		"poll_vote":   true,
		"text":        false,
		"image":       false,
		"poll":        false,
		"unsupported": false,
	}
	for eventType, want := range cases {
		if got := (mapper.InboundEvent{Type: eventType}).ChangesAnotherMessage(); got != want {
			t.Fatalf("ChangesAnotherMessage(%q) = %v, want %v", eventType, got, want)
		}
	}
}
