package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/gateway"
)

var groupRpcOperations = []string{"group.create", "group.invite_link", "group.info", "group.joined"}

func setupGroupGateway(t *testing.T, fake *fakeWAClient, channelID string) (conn *rabbitmq.Connection, cancel context.CancelFunc, runErrCh chan error) {
	t.Helper()
	infra := startGatewayInfra(t, channelID)
	cancel, runErrCh = startGroupGateway(t, infra, fake, "gateway-groups")
	return infra.conn, cancel, runErrCh
}

func startGroupGateway(t *testing.T, infra *gatewayInfra, fake *fakeWAClient, name string) (context.CancelFunc, chan error) {
	t.Helper()
	return startGroupGatewayWith(t, infra, gatewayDepsOn(t, infra, fake, name))
}

func startGroupGatewayWith(t *testing.T, infra *gatewayInfra, deps gateway.Deps) (context.CancelFunc, chan error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- gateway.Run(ctx, deps)
	}()

	waitForGroupRpcHandlersReady(t, infra.conn)

	return cancel, runErrCh
}

func waitForGroupRpcHandlersReady(t *testing.T, conn *rabbitmq.Connection) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for _, operation := range groupRpcOperations {
		queue := gatewayamqp.RpcQueueName(operation)
		for {
			ready, err := groupRpcQueueHasConsumer(conn, queue)
			if err == nil && ready {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("group rpc handler for %s not ready after 10s: %v", operation, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func groupRpcQueueHasConsumer(conn *rabbitmq.Connection, queue string) (bool, error) {
	ch, err := conn.Channel()
	if err != nil {
		return false, err
	}
	defer func() { _ = ch.Close() }()
	info, err := ch.QueueDeclarePassive(queue, true, false, false, false, rabbitmq.Table{"x-queue-type": "quorum"})
	if err != nil {
		return false, err
	}
	return info.Consumers >= 1, nil
}

func TestGroupCreateRpcCreatesAnnouncesAndReturnsLink(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)

	probe := newRpcProbe(t, conn)
	reply := probe.call(t, "group.create", "c1", `{"tenantId":"tenant-status-roundtrip","channelId":"channel-groups","name":"GRUPO #1","description":"Regras","announce":true}`, 10*time.Second)
	if reply["ok"] != true {
		t.Fatalf("reply %v", reply)
	}
	result := reply["result"].(map[string]any)
	groupJID := result["groupJid"].(string)
	if groupJID == "" || result["inviteUrl"].(string) == "" || result["participantCount"].(float64) != 1 || result["createdAt"].(string) == "" {
		t.Fatalf("result %v", result)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.createdGroups) != 1 || !fake.createdGroups[0].IsAnnounce || fake.createdGroups[0].Name != "GRUPO #1" {
		t.Fatalf("created %+v", fake.createdGroups)
	}
	if fake.topicCalls[groupJID] != "Regras" {
		t.Fatalf("topic %v", fake.topicCalls)
	}
}

func TestGroupInviteLinkResetAndInfoAndJoined(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)

	probe := newRpcProbe(t, conn)
	created := probe.call(t, "group.create", "c1", `{"tenantId":"t","channelId":"channel-groups","name":"G","announce":false}`, 10*time.Second)
	groupJID := created["result"].(map[string]any)["groupJid"].(string)
	first := created["result"].(map[string]any)["inviteUrl"].(string)

	req, _ := json.Marshal(map[string]any{"tenantId": "t", "channelId": "channel-groups", "groupJid": groupJID, "reset": true})
	reset := probe.call(t, "group.invite_link", "c2", string(req), 10*time.Second)
	if reset["ok"] != true || reset["result"].(map[string]any)["inviteUrl"] == first {
		t.Fatalf("reset must return a new link: %v", reset)
	}

	info := probe.call(t, "group.info", "c3", string(req), 10*time.Second)
	r := info["result"].(map[string]any)
	if r["name"] != "G" || r["announce"] != false || r["participantCount"].(float64) != 1 {
		t.Fatalf("info %v", r)
	}

	joined := probe.call(t, "group.joined", "c4", `{"tenantId":"t","channelId":"channel-groups"}`, 10*time.Second)
	list := joined["result"].(map[string]any)["groups"].([]any)
	if len(list) != 1 || list[0].(map[string]any)["groupJid"] != groupJID {
		t.Fatalf("joined %v", joined)
	}
}

func TestGroupRpcDistinguishesNotPairedFromOffline(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	fake.staysDown = true
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)

	probe := newRpcProbe(t, conn)
	notPaired := probe.call(t, "group.create", "n1", `{"tenantId":"t","channelId":"channel-unknown","name":"G"}`, 15*time.Second)
	if notPaired["ok"] != false || notPaired["error"].(map[string]any)["code"] != "not_found" {
		t.Fatalf("unknown channel → %v", notPaired)
	}
	offline := probe.call(t, "group.create", "o1", `{"tenantId":"t","channelId":"channel-groups","name":"G"}`, 30*time.Second)
	if offline["ok"] != false || offline["error"].(map[string]any)["code"] != "unavailable" {
		t.Fatalf("paired but down → %v", offline)
	}
}

func TestGroupRpcRejectsInvalidPayload(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)

	probe := newRpcProbe(t, conn)
	reply := probe.call(t, "group.info", "i1", `{"tenantId":"t","channelId":"channel-groups","groupJid":"not a jid"}`, 10*time.Second)
	if reply["ok"] != false || reply["error"].(map[string]any)["code"] != "invalid_request" {
		t.Fatalf("bad jid → %v", reply)
	}
}

func TestGroupInfoUnknownGroupIsBadGateway(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	conn, cancel, runErrCh := setupGroupGateway(t, fake, "channel-groups")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)

	probe := newRpcProbe(t, conn)
	reply := probe.call(t, "group.info", "u1", `{"tenantId":"t","channelId":"channel-groups","groupJid":"120363000000000099@g.us"}`, 10*time.Second)
	if reply["ok"] != false || reply["error"].(map[string]any)["code"] != "bad_gateway" {
		t.Fatalf("unknown group → %v", reply)
	}
}
