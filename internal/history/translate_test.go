package history_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/w3nder/whatsmeow-gateway/internal/history"
	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
)

const (
	ownPhone = "5511900000000"
	maria    = "5511999998888@s.whatsapp.net"
	mariaLid = "123456789012345@lid"
)

type fakeSource struct {
	own        types.JID
	pns        map[string]types.JID
	media      []byte
	mediaErr   error
	hold       time.Duration
	blockMedia bool
	inFlight   atomic.Int32
	peak       atomic.Int32
}

func raisePeak(peak *atomic.Int32, current int32) {
	for {
		seen := peak.Load()
		if current <= seen || peak.CompareAndSwap(seen, current) {
			return
		}
	}
}

func newFakeSource() *fakeSource {
	return &fakeSource{
		own:   types.NewJID(ownPhone, types.DefaultUserServer),
		pns:   map[string]types.JID{},
		media: []byte("media bytes"),
	}
}

func (s *fakeSource) ParseWebMessage(chat types.JID, msg *waWeb.WebMessageInfo) (*events.Message, error) {
	fromMe := msg.GetKey().GetFromMe()
	sender := chat
	if fromMe {
		sender = s.own
	}
	info := types.MessageInfo{
		MessageSource: types.MessageSource{Chat: chat, Sender: sender, IsFromMe: fromMe},
		ID:            msg.GetKey().GetID(),
		PushName:      msg.GetPushName(),
		Timestamp:     time.Unix(int64(msg.GetMessageTimestamp()), 0),
	}
	evt := &events.Message{
		RawMessage: msg.GetMessage(),
		Info:       info,
	}
	return evt.UnwrapRaw(), nil
}

func (s *fakeSource) Download(ctx context.Context, _ whatsmeow.DownloadableMessage) ([]byte, error) {
	raisePeak(&s.peak, s.inFlight.Add(1))
	defer s.inFlight.Add(-1)
	if s.blockMedia {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if s.hold > 0 {
		time.Sleep(s.hold)
	}
	return s.media, s.mediaErr
}

func (s *fakeSource) PNForLID(_ context.Context, lid types.JID) (types.JID, bool, error) {
	pn, ok := s.pns[lid.User]
	return pn, ok, nil
}

func (s *fakeSource) DecryptSecretEncryptedMessage(context.Context, *events.Message) (*waE2E.Message, error) {
	return nil, errors.New("fake: no message secrets")
}

func (s *fakeSource) DecryptPollVote(context.Context, *events.Message) (*waE2E.PollVoteMessage, error) {
	return nil, errors.New("fake: no message secrets")
}

type memoryMedia struct {
	mu   sync.Mutex
	keys []string
}

func (m *memoryMedia) Put(_ context.Context, key, _ string, _ []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keys = append(m.keys, key)
	return nil
}

func translateDeps(source *fakeSource) history.TranslateDeps {
	return history.TranslateDeps{Source: source, Media: &memoryMedia{}, ChannelID: "channel-1", TenantID: "tenant-1"}
}

func text(body string) *waE2E.Message {
	return &waE2E.Message{Conversation: proto.String(body)}
}

func webMessage(chat, id string, fromMe bool, pushName string, at time.Time, content *waE2E.Message) *waHistorySync.HistorySyncMsg {
	return &waHistorySync.HistorySyncMsg{Message: &waWeb.WebMessageInfo{
		Key:              &waCommon.MessageKey{RemoteJID: proto.String(chat), FromMe: proto.Bool(fromMe), ID: proto.String(id)},
		MessageTimestamp: proto.Uint64(uint64(at.Unix())),
		PushName:         proto.String(pushName),
		Message:          content,
	}}
}

func conversation(id string, messages ...*waHistorySync.HistorySyncMsg) *waHistorySync.Conversation {
	return &waHistorySync.Conversation{ID: proto.String(id), Messages: messages}
}

func chunkOf(syncType waHistorySync.HistorySync_HistorySyncType, progress uint32, conversations ...*waHistorySync.Conversation) *waHistorySync.HistorySync {
	return &waHistorySync.HistorySync{
		SyncType:      syncType.Enum(),
		ChunkOrder:    proto.Uint32(3),
		Progress:      proto.Uint32(progress),
		Conversations: conversations,
	}
}

func decodeMessage(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("history message is not a json object: %v", err)
	}
	return fields
}

func translate(t *testing.T, source *fakeSource, data *waHistorySync.HistorySync, now time.Time) history.Translation {
	t.Helper()
	got, err := history.Translate(context.Background(), translateDeps(source), data, history.DefaultLimits(), now)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	return got
}

func TestTranslateGroupsAChatThroughTheLiveMapper(t *testing.T) {
	now := time.Now()
	data := chunkOf(waHistorySync.HistorySync_RECENT, 40, conversation(maria,
		webMessage(maria, "3EB0IN", false, "Maria", now.Add(-time.Hour), text("Olá")),
		webMessage(maria, "3EB0OUT", true, "", now.Add(-30*time.Minute), text("Oi, Maria")),
	))

	got := translate(t, newFakeSource(), data, now)

	if got.Messages != 2 || len(got.Chats) != 1 {
		t.Fatalf("want one chat with two messages, got %+v", got)
	}
	chat := got.Chats[0]
	if chat.Phone != "5511999998888" || chat.Lid != nil || chat.ProfileName == nil || *chat.ProfileName != "Maria" {
		t.Fatalf("chat header wrong: phone %q lid %v profileName %v", chat.Phone, chat.Lid, chat.ProfileName)
	}
	inbound := decodeMessage(t, chat.Messages[0])
	outbound := decodeMessage(t, chat.Messages[1])
	body, _ := inbound["text"].(map[string]any)
	if inbound["providerMessageId"] != "3EB0IN" || inbound["type"] != "text" || body["body"] != "Olá" || inbound["timestamp"] == nil {
		t.Fatalf("inbound message wrong: %v", inbound)
	}
	if outbound["providerMessageId"] != "3EB0OUT" || outbound["fromMe"] != true {
		t.Fatalf("outbound message wrong: %v", outbound)
	}
	for _, field := range []string{"phoneNumberId", "from", "senderPn", "senderLid", "profileName"} {
		if _, present := inbound[field]; present {
			t.Fatalf("%s belongs to the chat, never to the message", field)
		}
	}
}

func TestTranslatedMessagePlusTheChatRebuildsTheLiveInboundEvent(t *testing.T) {
	now := time.Now()
	source := newFakeSource()
	item := webMessage(maria, "3EB0REBUILD", false, "Maria", now.Add(-time.Hour), text("Olá"))
	deps := translateDeps(source)

	got, err := history.Translate(context.Background(), deps, chunkOf(waHistorySync.HistorySync_RECENT, 40, conversation(maria, item)), history.DefaultLimits(), now)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}

	chatJID, err := types.ParseJID(maria)
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	evt, err := source.ParseWebMessage(chatJID, item.GetMessage())
	if err != nil {
		t.Fatalf("ParseWebMessage: %v", err)
	}
	live, err := mapper.BuildInbound(context.Background(), mapper.InboundDeps{
		Downloader: source,
		Resolver:   source,
		Media:      deps.Media,
		Secrets:    source,
		ChannelID:  deps.ChannelID,
		TenantID:   deps.TenantID,
	}, evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	encoded, err := json.Marshal(live)
	if err != nil {
		t.Fatalf("marshal live: %v", err)
	}
	var want map[string]any
	if err := json.Unmarshal(encoded, &want); err != nil {
		t.Fatalf("unmarshal live: %v", err)
	}

	chat := got.Chats[0]
	rebuilt := decodeMessage(t, chat.Messages[0])
	rebuilt["phoneNumberId"] = deps.ChannelID
	rebuilt["from"] = chat.Phone
	rebuilt["senderPn"] = chat.Phone
	rebuilt["profileName"] = *chat.ProfileName

	if !reflect.DeepEqual(rebuilt, want) {
		t.Fatalf("the backend could not rebuild the live event\nrebuilt %v\nlive    %v", rebuilt, want)
	}
}

func TestTranslateSkipsGroupStatusAndNewsletterChats(t *testing.T) {
	now := time.Now()
	data := chunkOf(waHistorySync.HistorySync_RECENT, 40,
		conversation("120363000000000000@g.us", webMessage("120363000000000000@g.us", "3EB0G", false, "Ana", now.Add(-time.Hour), text("grupo"))),
		conversation("status@broadcast", webMessage("status@broadcast", "3EB0S", false, "Ana", now.Add(-time.Hour), text("status"))),
		conversation("120363111111111111@newsletter", webMessage("120363111111111111@newsletter", "3EB0N", false, "Canal", now.Add(-time.Hour), text("canal"))),
	)

	got := translate(t, newFakeSource(), data, now)

	if len(got.Chats) != 0 || got.Messages != 0 || got.Skipped != 3 {
		t.Fatalf("groups, status and newsletters are never imported, got %+v", got)
	}
}

func TestTranslateSkipsMessagesThatChangeAnotherMessage(t *testing.T) {
	now := time.Now()
	target := &waCommon.MessageKey{ID: proto.String("3EB0TEXT")}
	data := chunkOf(waHistorySync.HistorySync_RECENT, 40, conversation(maria,
		webMessage(maria, "3EB0TEXT", false, "Maria", now.Add(-time.Hour), text("original")),
		webMessage(maria, "3EB0REACT", false, "Maria", now.Add(-50*time.Minute), &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{Key: target, Text: proto.String("ok")}}),
		webMessage(maria, "3EB0REVOKE", false, "Maria", now.Add(-40*time.Minute), &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_REVOKE.Enum(), Key: target}}),
		webMessage(maria, "3EB0EDIT", false, "Maria", now.Add(-30*time.Minute), &waE2E.Message{EditedMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(), Key: target, EditedMessage: text("editada")}}}}),
		webMessage(maria, "3EB0VOTE", false, "Maria", now.Add(-20*time.Minute), &waE2E.Message{PollUpdateMessage: &waE2E.PollUpdateMessage{PollCreationMessageKey: &waCommon.MessageKey{ID: proto.String("3EB0POLL")}}}),
	))

	got := translate(t, newFakeSource(), data, now)

	if got.Messages != 1 || got.Skipped != 4 {
		t.Fatalf("only the original message is imported, got %+v", got)
	}
	if decodeMessage(t, got.Chats[0].Messages[0])["providerMessageId"] != "3EB0TEXT" {
		t.Fatalf("the kept message must be the original, got %s", got.Chats[0].Messages[0])
	}
}

func TestTranslateSkipsAMessageWithoutContent(t *testing.T) {
	now := time.Now()
	data := chunkOf(waHistorySync.HistorySync_RECENT, 40, conversation(maria,
		webMessage(maria, "3EB0STUB", false, "Maria", now.Add(-time.Hour), nil),
		webMessage(maria, "3EB0TEXT", false, "Maria", now.Add(-30*time.Minute), text("oi")),
	))

	got := translate(t, newFakeSource(), data, now)

	if got.Messages != 1 || got.Skipped != 1 {
		t.Fatalf("a system stub without content is skipped, got %+v", got)
	}
}

func TestTranslateCountsMessagesOlderThanTheWindow(t *testing.T) {
	now := time.Now()
	day := 24 * time.Hour
	data := chunkOf(waHistorySync.HistorySync_FULL, 60, conversation(maria,
		webMessage(maria, "3EB0OLD", false, "Maria", now.Add(-91*day), text("antiga")),
		webMessage(maria, "3EB0KEPT", false, "Maria", now.Add(-89*day), text("dentro")),
	))

	got := translate(t, newFakeSource(), data, now)

	if got.OutOfWindow != 1 || got.Messages != 1 {
		t.Fatalf("only the last 90 days are imported, got %+v", got)
	}
}

func TestTranslateSkipsALidChatWithoutAResolvablePhone(t *testing.T) {
	now := time.Now()
	data := chunkOf(waHistorySync.HistorySync_RECENT, 40, conversation(mariaLid,
		webMessage(mariaLid, "3EB0LID", false, "Maria", now.Add(-time.Hour), text("oi")),
	))

	got := translate(t, newFakeSource(), data, now)

	if len(got.Chats) != 0 || got.ChatsWithoutPhone != 1 {
		t.Fatalf("a chat without a resolvable phone creates no contact, got %+v", got)
	}
}

func TestTranslateResolvesALidChatThroughTheConversationPhone(t *testing.T) {
	now := time.Now()
	conv := conversation(mariaLid, webMessage(mariaLid, "3EB0LIDPN", false, "Maria", now.Add(-time.Hour), text("oi")))
	conv.PnJID = proto.String("5511988887777@s.whatsapp.net")

	got := translate(t, newFakeSource(), chunkOf(waHistorySync.HistorySync_RECENT, 40, conv), now)

	if len(got.Chats) != 1 || got.Chats[0].Phone != "5511988887777" || got.Chats[0].Lid == nil || *got.Chats[0].Lid != "123456789012345" {
		t.Fatalf("the conversation phone must resolve the lid chat, got %+v", got.Chats)
	}
}

func TestTranslateResolvesALidChatThroughTheStoredMapping(t *testing.T) {
	now := time.Now()
	source := newFakeSource()
	source.pns["123456789012345"] = types.NewJID("5511977776666", types.DefaultUserServer)

	got := translate(t, source, chunkOf(waHistorySync.HistorySync_RECENT, 40, conversation(mariaLid,
		webMessage(mariaLid, "3EB0LIDMAP", true, "", now.Add(-time.Hour), text("oi")),
	)), now)

	if len(got.Chats) != 1 || got.Chats[0].Phone != "5511977776666" || got.Chats[0].ProfileName != nil {
		t.Fatalf("the stored lid mapping must resolve the chat and a chat with only sent messages has no profile name, got %+v", got.Chats)
	}
}

func TestTranslateCarriesTheLidOfAPhoneChat(t *testing.T) {
	now := time.Now()
	conv := conversation(maria, webMessage(maria, "3EB0PNLID", false, "Maria", now.Add(-time.Hour), text("oi")))
	conv.LidJID = proto.String(mariaLid)

	got := translate(t, newFakeSource(), chunkOf(waHistorySync.HistorySync_RECENT, 40, conv), now)

	if len(got.Chats) != 1 || got.Chats[0].Lid == nil || *got.Chats[0].Lid != "123456789012345" {
		t.Fatalf("the conversation lid must be carried on the chat, got %+v", got.Chats)
	}
}
