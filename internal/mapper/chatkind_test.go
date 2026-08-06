package mapper_test

import (
	"testing"

	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
)

func TestKindOf(t *testing.T) {
	cases := []struct {
		name string
		jid  types.JID
		want mapper.ChatKind
	}{
		{"private", types.NewJID("5511999998888", types.DefaultUserServer), mapper.ChatPrivate},
		{"lid", types.NewJID("189512345678901", types.HiddenUserServer), mapper.ChatPrivate},
		{"group", types.NewJID("120363000000000000", types.GroupServer), mapper.ChatGroup},
		{"broadcast", types.NewJID("status", types.BroadcastServer), mapper.ChatIgnored},
		{"newsletter", types.NewJID("120363111111111111", types.NewsletterServer), mapper.ChatIgnored},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mapper.KindOf(tc.jid); got != tc.want {
				t.Fatalf("KindOf(%s) = %v, want %v", tc.jid, got, tc.want)
			}
		})
	}
}
