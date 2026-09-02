package gateway

import (
	"testing"

	"go.mau.fi/whatsmeow/types"
)

func TestPhoneFromJID(t *testing.T) {
	withDevice := types.NewJID("5511999990000", types.DefaultUserServer)
	withDevice.Device = 12

	for _, tc := range []struct {
		name string
		jid  *types.JID
		want string
	}{
		{"sem sessao", nil, ""},
		{"numero simples", ptr(types.NewJID("5511988887777", types.DefaultUserServer)), "5511988887777"},
		{"numero com aparelho", &withDevice, "5511999990000"},
		{"jid vazio", &types.JID{}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := phoneFromJID(tc.jid); got != tc.want {
				t.Fatalf("phoneFromJID = %q, want %q", got, tc.want)
			}
		})
	}
}

func ptr(j types.JID) *types.JID { return &j }
