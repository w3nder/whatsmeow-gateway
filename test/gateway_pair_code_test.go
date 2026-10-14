package test

import (
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

func TestGatewayPairByCodeAnswersWithTheCodeInsteadOfAQR(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrFeed = make(chan whatsmeow.QRChannelItem)
	fake.pairingCode = "ABCD-EFGH"

	probeCh, deliveries, cancel, runErrCh := setupPairGateway(t, func(channelID string, jid *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	const channelID = "channel-pair-code-1"

	publishPairCommand(t, probeCh, gatewayamqp.PairCommand{TenantID: "tenant-pair-code", ChannelID: channelID, UserID: "user-code", PhoneNumber: "5511987654321"})
	feedQR(t, fake, whatsmeow.QRChannelItem{Event: "code", Code: "qr-ignored", Timeout: time.Minute})

	got := waitForChannelQR(t, deliveries, channelID, pairWait)
	if got.PairingCode != "ABCD-EFGH" || got.QR != "" || got.UserID != "user-code" || got.TenantID != "tenant-pair-code" {
		t.Fatalf("expected the pairing code for the operator who asked, without a QR, got %+v", got)
	}
	if phones := fake.phonesAskedForACode(); len(phones) != 1 || phones[0] != "5511987654321" {
		t.Fatalf("expected one code asked for the number in the command, got %v", phones)
	}

	close(fake.qrFeed)
	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
}

func TestGatewayPairByCodeThatExpiresIsRefusedInPortuguese(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrFeed = make(chan whatsmeow.QRChannelItem)
	fake.pairingCode = "WXYZ-2345"

	probeCh, deliveries, cancel, runErrCh := setupPairGateway(t, func(channelID string, jid *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	const channelID = "channel-pair-code-expired-1"

	publishPairCommand(t, probeCh, gatewayamqp.PairCommand{TenantID: "tenant-pair-code-expired", ChannelID: channelID, UserID: "user-expired", PhoneNumber: "5511987654321"})
	feedQR(t, fake, whatsmeow.QRChannelItem{Event: "code", Code: "qr-ignored", Timeout: time.Minute})
	waitForChannelQR(t, deliveries, channelID, pairWait)

	feedQR(t, fake, whatsmeow.QRChannelTimeout)

	evt := waitForChannelStatus(t, deliveries, channelID, "error", pairWait)
	if evt.Reason != "O código de pareamento venceu antes de ser usado. Peça um novo código." {
		t.Fatalf("the owner must be told the code expired, got %+v", evt)
	}
	if evt.UserID != "user-expired" {
		t.Fatalf("the refusal must go back to the operator who asked, got %+v", evt)
	}

	close(fake.qrFeed)
	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
}

func TestGatewayPairByCodeWithAnInvalidNumberIsRefusedInPortugueseAndAcked(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrFeed = make(chan whatsmeow.QRChannelItem, 1)
	fake.pairPhoneErr = whatsmeow.ErrPhoneNumberTooShort

	probeCh, deliveries, cancel, runErrCh := setupPairGateway(t, func(channelID string, jid *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	dlq, err := probeCh.Consume(gatewayamqp.GatewayPairDLQ, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume %s: %v", gatewayamqp.GatewayPairDLQ, err)
	}

	const channelID = "channel-pair-code-invalid-1"

	publishPairCommand(t, probeCh, gatewayamqp.PairCommand{TenantID: "tenant-pair-code-invalid", ChannelID: channelID, UserID: "user-invalid", PhoneNumber: "12345"})
	feedQR(t, fake, whatsmeow.QRChannelItem{Event: "code", Code: "qr-ignored", Timeout: time.Minute})

	evt := waitForChannelStatus(t, deliveries, channelID, "error", pairWait)
	if evt.Reason != "Número inválido. Informe o número com DDI e DDD, só com dígitos, por exemplo 5511987654321." {
		t.Fatalf("the owner must be told the number is invalid, got %+v", evt)
	}
	if fake.disconnectCount() == 0 {
		t.Fatal("a refused number must close the pairing socket instead of leaving it open")
	}
	select {
	case d := <-dlq:
		t.Fatalf("a refused number must be acked, got it dead-lettered: %s", d.Body)
	case <-time.After(3 * time.Second):
	}

	close(fake.qrFeed)
	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
}

func TestGatewayPairByCodeRefusedByWhatsAppIsToldInPortugueseWithoutTheTechnicalDetail(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrFeed = make(chan whatsmeow.QRChannelItem, 1)
	fake.pairPhoneErr = errors.New("server returned error 400: bad-request")

	probeCh, deliveries, cancel, runErrCh := setupPairGateway(t, func(channelID string, jid *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	dlq, err := probeCh.Consume(gatewayamqp.GatewayPairDLQ, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume %s: %v", gatewayamqp.GatewayPairDLQ, err)
	}

	const channelID = "channel-pair-code-refused-1"

	publishPairCommand(t, probeCh, gatewayamqp.PairCommand{TenantID: "tenant-pair-code-refused", ChannelID: channelID, UserID: "user-refused", PhoneNumber: "5511987654321"})
	feedQR(t, fake, whatsmeow.QRChannelItem{Event: "code", Code: "qr-ignored", Timeout: time.Minute})

	evt := waitForChannelStatus(t, deliveries, channelID, "error", pairWait)
	if evt.Reason != "O WhatsApp não gerou o código para este número. Confira se o número tem WhatsApp e peça um novo código." {
		t.Fatalf("the owner must read a reason in Portuguese, without the technical detail, got %+v", evt)
	}
	select {
	case d := <-dlq:
		t.Fatalf("a refused pairing must be acked, got it dead-lettered: %s", d.Body)
	case <-time.After(3 * time.Second):
	}

	close(fake.qrFeed)
	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
}

func TestGatewayPairByCodeThatIsTypedArrivesAsAConnectedChannelLikeTheQR(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrFeed = make(chan whatsmeow.QRChannelItem)
	fake.pairingCode = "JKLM-6789"

	probeCh, deliveries, cancel, runErrCh := setupPairGateway(t, func(channelID string, jid *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	const channelID = "channel-pair-code-connected-1"

	publishPairCommand(t, probeCh, gatewayamqp.PairCommand{
		TenantID:      "tenant-pair-code-connected",
		ChannelID:     channelID,
		UserID:        "user-connected",
		ImportHistory: true,
		ImportID:      "import-pair-code-1",
		PhoneNumber:   "5511987654321",
	})
	feedQR(t, fake, whatsmeow.QRChannelItem{Event: "code", Code: "qr-ignored", Timeout: time.Minute})
	if got := waitForChannelQR(t, deliveries, channelID, pairWait); got.PairingCode != "JKLM-6789" {
		t.Fatalf("expected the pairing code before the connection, got %+v", got)
	}

	fake.markPaired()
	feedQR(t, fake, whatsmeow.QRChannelSuccess)

	evt := waitForChannelStatus(t, deliveries, channelID, "connected", pairWait)
	if evt.PhoneNumber != "15550000000" || evt.UserID != "user-connected" || evt.TenantID != "tenant-pair-code-connected" {
		t.Fatalf("expected the connected channel with its number for the operator who asked, got %+v", evt)
	}
	if !fake.tookOverHistoryBeforeConnecting() {
		t.Fatal("a code pairing that asked for the history must take it over before connecting, like the QR")
	}

	close(fake.qrFeed)
	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
}

func TestGatewayPairByCodeWithHistoryForAnAlreadyPairedChannelUnlinksItAndAnswersWithANewCode(t *testing.T) {
	paired := newFakeWAClient()
	paired.markPaired()
	fresh := newFakeWAClient()
	fresh.qrFeed = make(chan whatsmeow.QRChannelItem)
	fresh.pairingCode = "RLNK-2345"

	probeCh, deliveries, cancel, runErrCh := setupPairGatewayWith(t, managerOver(paired, fresh))

	const channelID = "channel-pair-code-relink-1"

	publishPairCommand(t, probeCh, gatewayamqp.PairCommand{
		TenantID:      "tenant-pair-code-relink",
		ChannelID:     channelID,
		UserID:        "user-relink",
		ImportHistory: true,
		ImportID:      "import-pair-code-relink-1",
		PhoneNumber:   "5511987654321",
	})
	feedQR(t, fresh, whatsmeow.QRChannelItem{Event: "code", Code: "qr-ignored", Timeout: time.Minute})
	if got := waitForChannelQR(t, deliveries, channelID, pairWait); got.PairingCode != "RLNK-2345" || got.UserID != "user-relink" {
		t.Fatalf("an import by code for a paired channel must answer with a new code, got %+v", got)
	}
	if paired.logoutCount() != 1 {
		t.Fatalf("the paired device must be unlinked before the new code, got %d logouts", paired.logoutCount())
	}

	fresh.markPaired()
	feedQR(t, fresh, whatsmeow.QRChannelSuccess)
	if evt := waitForChannelStatus(t, deliveries, channelID, "connected", pairWait); evt.UserID != "user-relink" {
		t.Fatalf("expected the channel connected for the operator who asked, got %+v", evt)
	}
	if !fresh.tookOverHistoryBeforeConnecting() {
		t.Fatal("the fresh device paired by code must take over the history before connecting")
	}

	close(fresh.qrFeed)
	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
}
