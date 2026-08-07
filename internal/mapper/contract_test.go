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

const groupReactionCommandLiteral = `{"tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","messageId":"3EB068F90C62346073B954:reaction","to":"120363422547615282@g.us","type":"text","kind":"reaction","targetProviderMessageId":"3EB068F90C62346073B954","targetFromMe":false,"targetParticipantJid":"2002125877314@lid","emoji":"❤️"}`

func TestGroupReactionContractLiteralBuildsParticipantScopedKey(t *testing.T) {
	var cmd amqp.GatewaySendCommand
	if err := json.Unmarshal([]byte(groupReactionCommandLiteral), &cmd); err != nil {
		t.Fatalf("unmarshal contract literal: %v", err)
	}

	expected := amqp.GatewaySendCommand{
		TenantID:                "1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c",
		ChannelID:               "2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d",
		MessageID:               "3EB068F90C62346073B954:reaction",
		To:                      "120363422547615282@g.us",
		Type:                    "text",
		Kind:                    "reaction",
		TargetProviderMessageID: "3EB068F90C62346073B954",
		TargetFromMe:            false,
		TargetParticipantJID:    "2002125877314@lid",
		Emoji:                   "❤️",
	}
	if !reflect.DeepEqual(cmd, expected) {
		t.Fatalf("contract literal decoded to %+v, want %+v", cmd, expected)
	}

	var cli *whatsmeow.Client
	to, msg, err := mapper.BuildOutbound(context.Background(), cli, cmd, stubFetch(nil, nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	if to.String() != "120363422547615282@g.us" {
		t.Fatalf("expected the group JID as recipient, got %q", to.String())
	}

	reaction := msg.GetReactionMessage()
	if reaction == nil {
		t.Fatalf("expected a ReactionMessage, got %+v", msg)
	}
	if reaction.GetText() != "❤️" {
		t.Fatalf("expected text ❤️, got %q", reaction.GetText())
	}

	key := reaction.GetKey()
	if key.GetRemoteJID() != "120363422547615282@g.us" {
		t.Fatalf("expected remoteJID 120363422547615282@g.us, got %q", key.GetRemoteJID())
	}
	if key.GetFromMe() {
		t.Fatalf("expected fromMe=false for a reaction to someone else's group message")
	}
	if key.GetID() != "3EB068F90C62346073B954" {
		t.Fatalf("expected ID 3EB068F90C62346073B954, got %q", key.GetID())
	}
	if key.GetParticipant() != "2002125877314@lid" {
		t.Fatalf("expected participant 2002125877314@lid, got %q", key.GetParticipant())
	}
}

func TestGroupReactionWithoutParticipantFallsBackToChatJID(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:                      "5511999887766@s.whatsapp.net",
		Kind:                    "reaction",
		TargetProviderMessageID: "3EB0PRIVATE",
		Emoji:                   "❤️",
	}

	var cli *whatsmeow.Client
	_, msg, err := mapper.BuildOutbound(context.Background(), cli, cmd, stubFetch(nil, nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}

	key := msg.GetReactionMessage().GetKey()
	if key.GetFromMe() {
		t.Fatalf("expected fromMe=false for a reaction to the contact's message")
	}
	if key.GetRemoteJID() != "5511999887766@s.whatsapp.net" {
		t.Fatalf("expected remoteJID 5511999887766@s.whatsapp.net, got %q", key.GetRemoteJID())
	}
	if key.GetParticipant() != "" {
		t.Fatalf("expected no participant in a one-to-one chat, got %q", key.GetParticipant())
	}
}

func TestReactionParticipantJIDMustParse(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:                      "120363422547615282@g.us",
		Kind:                    "reaction",
		TargetProviderMessageID: "3EB0BAD",
		TargetParticipantJID:    "2002125877314.abc@lid",
		Emoji:                   "❤️",
	}

	if _, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil)); err == nil {
		t.Fatalf("expected an error for an unparseable participant jid")
	}
}
