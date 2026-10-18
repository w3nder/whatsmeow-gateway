package test

import (
	"encoding/json"
	"reflect"
	"strconv"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

func waitForContactCheckRpcReady(t *testing.T, infra *gatewayInfra) {
	t.Helper()
	queue := gatewayamqp.RpcQueueName("contact.check")
	deadline := time.Now().Add(10 * time.Second)
	for {
		ready, err := groupRpcQueueHasConsumer(infra.conn, queue)
		if err == nil && ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("contact.check rpc handler not ready after 10s: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestContactCheckRpcAsksWhatsAppOnceAndAnswersEachPhone(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	knownPN := types.NewJID("5511988880002", types.DefaultUserServer)
	fake.knownLIDs = map[types.JID]types.JID{knownPN: types.NewJID("222000222", types.HiddenUserServer)}
	fake.whatsappAccounts = map[string]types.IsOnWhatsAppResponse{
		"+5511999990001": {
			IsIn:        true,
			JID:         types.NewJID("111000111", types.HiddenUserServer),
			PhoneNumber: types.NewJID("551199990001", types.DefaultUserServer),
		},
		"+5511988880002": {IsIn: true, JID: knownPN},
	}
	infra := startGatewayInfra(t, "channel-contacts")
	cancel, runErrCh := startGroupGateway(t, infra, fake, "gateway-contacts")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	waitForContactCheckRpcReady(t, infra)

	probe := newRpcProbe(t, infra.conn)
	reply := probe.call(t, "contact.check", "k1", `{"tenantId":"tenant-status-roundtrip","channelId":"channel-contacts","phones":["5511999990001","+5511988880002","5511977770003"]}`, 10*time.Second)
	if reply["ok"] != true {
		t.Fatalf("reply %v", reply)
	}
	got, _ := json.Marshal(reply["result"])
	want := `{"results":[` +
		`{"jid":"551199990001@s.whatsapp.net","lid":"111000111@lid","onWhatsapp":true,"phone":"5511999990001"},` +
		`{"jid":"5511988880002@s.whatsapp.net","lid":"222000222@lid","onWhatsapp":true,"phone":"+5511988880002"},` +
		`{"jid":null,"lid":null,"onWhatsapp":false,"phone":"5511977770003"}]}`
	if string(got) != want {
		t.Fatalf("result\n got %s\nwant %s", got, want)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	wantCalls := [][]string{{"+5511999990001", "+5511988880002", "+5511977770003"}}
	if !reflect.DeepEqual(fake.isOnWhatsAppCalls, wantCalls) {
		t.Fatalf("IsOnWhatsApp calls %v, want one call for the whole batch", fake.isOnWhatsAppCalls)
	}
}

func TestContactCheckRpcRefusesInvalidRequestsWithoutAsking(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	infra := startGatewayInfra(t, "channel-contacts")
	cancel, runErrCh := startGroupGateway(t, infra, fake, "gateway-contacts")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	waitForContactCheckRpcReady(t, infra)

	tooMany := make([]string, 51)
	for i := range tooMany {
		tooMany[i] = "55119000" + strconv.Itoa(10000+i)
	}
	tooManyBody, _ := json.Marshal(map[string]any{"tenantId": "t", "channelId": "channel-contacts", "phones": tooMany})

	cases := []struct {
		name    string
		body    string
		message string
	}{
		{"more than fifty", string(tooManyBody), "at most 50 phones per request, got 51"},
		{"no phones", `{"tenantId":"t","channelId":"channel-contacts","phones":[]}`, "phones is required"},
		{"not a number", `{"tenantId":"t","channelId":"channel-contacts","phones":["55 11 abc"]}`, `phone "55 11 abc" is not a number`},
		{"no tenant", `{"channelId":"channel-contacts","phones":["5511999990001"]}`, "tenantId is required"},
		{"no channel", `{"tenantId":"t","phones":["5511999990001"]}`, "channelId is required"},
	}
	probe := newRpcProbe(t, infra.conn)
	for i, tc := range cases {
		reply := probe.call(t, "contact.check", "v"+strconv.Itoa(i), tc.body, 10*time.Second)
		errBody, _ := reply["error"].(map[string]any)
		if reply["ok"] != false || errBody["code"] != "invalid_request" || errBody["message"] != tc.message {
			t.Fatalf("%s: reply %v", tc.name, reply)
		}
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.isOnWhatsAppCalls) != 0 {
		t.Fatalf("invalid requests must not reach WhatsApp, got %v", fake.isOnWhatsAppCalls)
	}
}

func TestContactCheckRpcOnlyUsesTheRequestedChannel(t *testing.T) {
	fake := newFakeWAClient()
	fake.markPaired()
	fake.staysDown = true
	infra := startGatewayInfra(t, "channel-contacts")
	cancel, runErrCh := startGroupGateway(t, infra, fake, "gateway-contacts")
	defer shutdownStatusRoundtripGateway(t, cancel, runErrCh)
	waitForContactCheckRpcReady(t, infra)

	probe := newRpcProbe(t, infra.conn)
	unknown := probe.call(t, "contact.check", "u1", `{"tenantId":"t","channelId":"channel-unknown","phones":["5511999990001"]}`, 15*time.Second)
	if unknown["ok"] != false || unknown["error"].(map[string]any)["code"] != "not_found" {
		t.Fatalf("unknown channel → %v", unknown)
	}
	offline := probe.call(t, "contact.check", "o1", `{"tenantId":"t","channelId":"channel-contacts","phones":["5511999990001"]}`, 30*time.Second)
	if offline["ok"] != false || offline["error"].(map[string]any)["code"] != "unavailable" {
		t.Fatalf("paired but down → %v", offline)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.isOnWhatsAppCalls) != 0 {
		t.Fatalf("a channel that is not connected must not be asked, got %v", fake.isOnWhatsAppCalls)
	}
}
