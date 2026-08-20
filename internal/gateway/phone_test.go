package gateway

import (
	"testing"

	"go.mau.fi/whatsmeow/types"
)

// The channel list showed the channel's name where the connected number belongs, because the
// number never left the gateway: it lives in the device JID and the status event had no field
// for it. Reading it must never be able to break the status publish, so a session that is not
// there yet yields an empty number rather than an error.
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
