package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
)

type stubSender struct {
	err error

	calls     int
	channelID string
	to        types.JID
	msg       *waE2E.Message
	id        string
}

func (s *stubSender) Send(_ context.Context, channelID string, to types.JID, msg *waE2E.Message, id string, _ []waBinary.Node) (string, time.Time, error) {
	s.calls++
	s.channelID, s.to, s.msg, s.id = channelID, to, msg, id
	if s.err != nil {
		return "", time.Time{}, s.err
	}
	return "3EB0AUTOREPLY", time.Unix(1754300000, 0), nil
}

type stubInboundPublisher struct {
	err    error
	events []any
}

func (p *stubInboundPublisher) PublishInbound(_ context.Context, evt any) error {
	if p.err != nil {
		return p.err
	}
	p.events = append(p.events, evt)
	return nil
}

func newTestReplier(sender *stubSender, publisher *stubInboundPublisher) callAutoReplier {
	return callAutoReplier{sender: sender, publisher: publisher, timeout: time.Second}
}

func TestReplySendsTheTextToThePhoneAndPublishesItAsAnOutboundMessage(t *testing.T) {
	sender, publisher := &stubSender{}, &stubInboundPublisher{}

	err := newTestReplier(sender, publisher).Reply(context.Background(), call.AutoReply{
		ChannelID: "chan-a", CallID: "CALL1", SenderLid: "173907587899617", SenderPn: "5511888887777", Text: "Não atendemos ligações.",
	})
	if err != nil {
		t.Fatalf("Reply failed: %v", err)
	}

	if sender.calls != 1 || sender.channelID != "chan-a" {
		t.Fatalf("send calls = %d on %q, want 1 on chan-a", sender.calls, sender.channelID)
	}
	if want := types.NewJID("5511888887777", types.DefaultUserServer); sender.to != want {
		t.Errorf("recipient = %s, want the phone number %s", sender.to, want)
	}
	if sender.msg.GetConversation() != "Não atendemos ligações." {
		t.Errorf("text = %q", sender.msg.GetConversation())
	}
	if len(publisher.events) != 1 {
		t.Fatalf("published %d events, want 1", len(publisher.events))
	}
	evt, ok := publisher.events[0].(mapper.InboundEvent)
	if !ok {
		t.Fatalf("published %T, want mapper.InboundEvent", publisher.events[0])
	}
	if !evt.FromMe || evt.Origin != mapper.OriginCallAutoReply || evt.Type != "text" {
		t.Errorf("event = %+v, want a fromMe text with origin call_auto_reply", evt)
	}
	if evt.ProviderMessageID != "3EB0AUTOREPLY" || evt.Timestamp != "1754300000" {
		t.Errorf("provider id/timestamp = %q/%q, want the ones returned by the send", evt.ProviderMessageID, evt.Timestamp)
	}
	if evt.PhoneNumberID != "chan-a" || evt.From != "5511888887777" || evt.SenderPn != "5511888887777" || evt.SenderLid != "173907587899617" {
		t.Errorf("identity = %+v, want the same caller identity as the call event", evt)
	}
	if evt.Text == nil || evt.Text.Body != "Não atendemos ligações." {
		t.Errorf("text body = %+v", evt.Text)
	}
}

func TestReplyFallsBackToTheLidWhenThePhoneIsUnknown(t *testing.T) {
	sender, publisher := &stubSender{}, &stubInboundPublisher{}

	err := newTestReplier(sender, publisher).Reply(context.Background(), call.AutoReply{
		ChannelID: "chan-a", CallID: "CALL1", SenderLid: "173907587899617", Text: "texto",
	})
	if err != nil {
		t.Fatalf("Reply failed: %v", err)
	}
	if want := types.NewJID("173907587899617", types.HiddenUserServer); sender.to != want {
		t.Errorf("recipient = %s, want the lid %s", sender.to, want)
	}
}

func TestReplyUsesAProviderIDThatIsStablePerCall(t *testing.T) {
	first, second := &stubSender{}, &stubSender{}
	reply := call.AutoReply{ChannelID: "chan-a", CallID: "CALL1", SenderPn: "5511888887777", Text: "texto"}

	_ = newTestReplier(first, &stubInboundPublisher{}).Reply(context.Background(), reply)
	_ = newTestReplier(second, &stubInboundPublisher{}).Reply(context.Background(), reply)

	if first.id == "" || first.id != second.id {
		t.Fatalf("provider ids = %q and %q, want the same non-empty id for the same call", first.id, second.id)
	}
}

func TestReplyWithoutARecipientSendsNothing(t *testing.T) {
	sender, publisher := &stubSender{}, &stubInboundPublisher{}

	err := newTestReplier(sender, publisher).Reply(context.Background(), call.AutoReply{ChannelID: "chan-a", CallID: "CALL1", Text: "texto"})

	if err == nil {
		t.Fatal("a reply with no recipient must fail")
	}
	if sender.calls != 0 || len(publisher.events) != 0 {
		t.Fatalf("nothing may be sent or published, sends %d events %d", sender.calls, len(publisher.events))
	}
}

func TestReplyPublishesNothingWhenTheSendFails(t *testing.T) {
	sender, publisher := &stubSender{err: errors.New("not connected")}, &stubInboundPublisher{}

	err := newTestReplier(sender, publisher).Reply(context.Background(), call.AutoReply{ChannelID: "chan-a", CallID: "CALL1", SenderPn: "5511888887777", Text: "texto"})

	if err == nil {
		t.Fatal("a failed send must surface as an error")
	}
	if len(publisher.events) != 0 {
		t.Fatalf("a message that was not sent must not enter the history, published %d", len(publisher.events))
	}
}

func TestReplySurfacesAPublishFailure(t *testing.T) {
	sender, publisher := &stubSender{}, &stubInboundPublisher{err: errors.New("broker down")}

	err := newTestReplier(sender, publisher).Reply(context.Background(), call.AutoReply{ChannelID: "chan-a", CallID: "CALL1", SenderPn: "5511888887777", Text: "texto"})

	if err == nil {
		t.Fatal("a failed publish must surface as an error")
	}
}
