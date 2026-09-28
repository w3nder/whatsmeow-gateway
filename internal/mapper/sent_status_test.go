package mapper_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
)

const resolvedRecipientSentStatusLiteral = `{"providerMessageId":"3EB0C0FFEE5D1A7E2B","opaqueMessageId":"3EB0PHONE:reaction","status":"sent","timestamp":"1790000000","tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","resolvedRecipient":{"phone":"5511999887766","lid":"2002125877314"}}`

func TestPhoneReactionSentStatusReportsTheResolvedLIDAsTheContractLiteral(t *testing.T) {
	var cmd amqp.GatewaySendCommand
	if err := json.Unmarshal([]byte(phoneReactionCommandLiteral), &cmd); err != nil {
		t.Fatalf("unmarshal contract literal: %v", err)
	}
	cli := realKeys{lids: map[types.JID]types.JID{
		types.NewJID("5511999887766", types.DefaultUserServer): types.NewJID("2002125877314", types.HiddenUserServer),
	}}
	chat, _, _, err := mapper.BuildOutbound(context.Background(), cli, cmd, stubFetch(nil, nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}

	body, err := json.Marshal(mapper.SentStatus(cmd, chat, "3EB0C0FFEE5D1A7E2B", time.Unix(1790000000, 0)))
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	if string(body) != resolvedRecipientSentStatusLiteral {
		t.Fatalf("status drifted from the backend contract literal:\n got %s\nwant %s", body, resolvedRecipientSentStatusLiteral)
	}
}

func TestSentStatusWithoutResolutionKeepsTheOldShape(t *testing.T) {
	cases := map[string]struct {
		to   string
		chat types.JID
	}{
		"phone chat without a lid":     {"5511888777666@s.whatsapp.net", types.NewJID("5511888777666", types.DefaultUserServer)},
		"lid addressed by the backend": {"2002125877314@lid", types.NewJID("2002125877314", types.HiddenUserServer)},
		"group":                        {"120363000000000000@g.us", types.NewJID("120363000000000000", types.GroupServer)},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cmd := amqp.GatewaySendCommand{TenantID: "tenant-1", MessageID: "msg-1", To: tc.to, Kind: "reaction"}
			body, err := json.Marshal(mapper.SentStatus(cmd, tc.chat, "3EB0ABC", time.Unix(1790000000, 0)))
			if err != nil {
				t.Fatalf("marshal status: %v", err)
			}
			want := `{"providerMessageId":"3EB0ABC","opaqueMessageId":"msg-1","status":"sent","timestamp":"1790000000"}`
			if string(body) != want {
				t.Fatalf("got %s, want %s", body, want)
			}
		})
	}
}
