package amqp_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

const groupCommandLiteral = `{"commandId":"01J9ZK3S3Y2Q0N4R8T6V1W5X7Z","tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","action":"remove_participants","groupJids":["120363422547615282@g.us","120363422547615283@g.us"],"params":{"phones":["5511999887766"]}}`

const groupActionEventLiteral = `{"tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","commandId":"01J9ZK3S3Y2Q0N4R8T6V1W5X7Z","groupJid":"120363422547615282@g.us","action":"remove_participants","ok":true,"removed":1}`

const groupParticipantsEventLiteral = `{"tenantId":"1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c","channelId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","groupJid":"120363422547615282@g.us","type":"join","participants":[{"jid":"2002125877314@lid","lid":"2002125877314@lid","phone":"5511999887766"}],"eventId":"3f1c2a9b8d7e6f5a4b3c2d1e0f9a8b7c","occurredAt":"2026-09-24T12:00:00Z"}`

func TestGroupCommandContractLiteral(t *testing.T) {
	var cmd amqp.GatewayGroupCommand
	if err := json.Unmarshal([]byte(groupCommandLiteral), &cmd); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	expected := amqp.GatewayGroupCommand{
		CommandID: "01J9ZK3S3Y2Q0N4R8T6V1W5X7Z",
		TenantID:  "1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c",
		ChannelID: "2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d",
		Action:    "remove_participants",
		GroupJIDs: []string{"120363422547615282@g.us", "120363422547615283@g.us"},
		Params:    amqp.GroupActionParams{Phones: []string{"5511999887766"}},
	}
	if !reflect.DeepEqual(cmd, expected) {
		t.Fatalf("decoded %+v, want %+v", cmd, expected)
	}
}

func TestGroupActionEventContractLiteral(t *testing.T) {
	removed := 1
	evt := amqp.GroupActionEvent{
		TenantID:  "1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c",
		ChannelID: "2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d",
		CommandID: "01J9ZK3S3Y2Q0N4R8T6V1W5X7Z",
		GroupJID:  "120363422547615282@g.us",
		Action:    "remove_participants",
		OK:        true,
		Removed:   &removed,
	}
	body, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(body) != groupActionEventLiteral {
		t.Fatalf("wire shape\n got %s\nwant %s", body, groupActionEventLiteral)
	}
}

func TestGroupParticipantsEventContractLiteral(t *testing.T) {
	evt := amqp.GroupParticipantsEvent{
		TenantID:  "1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c",
		ChannelID: "2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d",
		GroupJID:  "120363422547615282@g.us",
		Type:      "join",
		Participants: []amqp.GroupParticipant{{
			JID:   "2002125877314@lid",
			LID:   "2002125877314@lid",
			Phone: "5511999887766",
		}},
		EventID:    "3f1c2a9b8d7e6f5a4b3c2d1e0f9a8b7c",
		OccurredAt: "2026-09-24T12:00:00Z",
	}
	body, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(body) != groupParticipantsEventLiteral {
		t.Fatalf("wire shape\n got %s\nwant %s", body, groupParticipantsEventLiteral)
	}
}

func TestGroupTopologyNames(t *testing.T) {
	if amqp.GatewayGroupExchange != "whatsapp.gateway.group.v1" || amqp.GatewayGroupQueue != "gateway.group" {
		t.Fatalf("group command topology: %s / %s", amqp.GatewayGroupExchange, amqp.GatewayGroupQueue)
	}
	if amqp.GroupActionRoutingKey != "whatsapp.group.action.v1" || amqp.GroupParticipantsRoutingKey != "whatsapp.group.participants.v1" {
		t.Fatalf("group event routing keys: %s / %s", amqp.GroupActionRoutingKey, amqp.GroupParticipantsRoutingKey)
	}
	if amqp.RpcQueueName("group.create") != "rpc.gateway.group.create" {
		t.Fatalf("rpc queue name: %s", amqp.RpcQueueName("group.create"))
	}
}
