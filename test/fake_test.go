package test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

type fakeWAClient struct {
	mu sync.Mutex

	deviceJID   *types.JID
	displayName string
	loggedIn    bool
	connected   bool

	staysDown bool

	qrItems    []whatsmeow.QRChannelItem
	qrFeed     chan whatsmeow.QRChannelItem
	connectErr error

	connectCalls      int
	qrChannelCalls    int
	waitCalls         int
	handlersAtConnect int

	sendResp  whatsmeow.SendResponse
	sendErr   error
	sendCalls int
	lastTo    types.JID
	lastMsg   *waE2E.Message
	lastID    types.MessageID
	lastNodes []waBinary.Node

	disconnectCalls int

	handlers []func(any)

	caller call.Caller

	groups           map[string]*types.GroupInfo
	createdGroups    []whatsmeow.ReqCreateGroup
	announceCalls    map[string]bool
	nameCalls        map[string][]string
	topicCalls       map[string]string
	photoCalls       map[string][]byte
	participantCalls []participantCall
	inviteLinks      map[string]string
	groupErr         error
	nextGroupSeq     int
	announceDelay    time.Duration
	announceCount    int
}

type participantCall struct {
	group        string
	participants []types.JID
	change       whatsmeow.ParticipantChange
}

var _ session.WAClient = (*fakeWAClient)(nil)

func newFakeWAClient() *fakeWAClient {
	return &fakeWAClient{}
}

func (f *fakeWAClient) QRChannel(ctx context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	f.mu.Lock()
	f.qrChannelCalls++
	if f.connected {
		f.mu.Unlock()
		return nil, whatsmeow.ErrQRAlreadyConnected
	}
	if f.qrFeed != nil {
		feed := f.qrFeed
		f.mu.Unlock()
		return feed, nil
	}
	items := f.qrItems
	if f.deviceJID == nil {
		for _, item := range items {
			if item == whatsmeow.QRChannelSuccess {
				jid := types.NewJID("15550000000", types.DefaultUserServer)
				f.deviceJID = &jid
				break
			}
		}
	}
	f.mu.Unlock()

	ch := make(chan whatsmeow.QRChannelItem, len(items))
	for _, item := range items {
		ch <- item
	}
	close(ch)
	return ch, nil
}

func (f *fakeWAClient) Connect() error {
	f.mu.Lock()
	f.connectCalls++
	f.handlersAtConnect = len(f.handlers)
	err := f.connectErr
	if err == nil && !f.staysDown {
		f.connected = true
		f.loggedIn = true
	}
	f.mu.Unlock()
	return err
}

func (f *fakeWAClient) IsLoggedIn() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loggedIn
}

func (f *fakeWAClient) IsConnected() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connected
}

func (f *fakeWAClient) WaitForConnection(time.Duration) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.waitCalls++
	return f.connected && f.loggedIn
}

func (f *fakeWAClient) DeviceJID() *types.JID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deviceJID
}

func (f *fakeWAClient) DeviceLID() types.JID {
	return types.EmptyJID
}

func (f *fakeWAClient) DisplayName() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.displayName
}

func (f *fakeWAClient) SendMessage(ctx context.Context, to types.JID, msg *waE2E.Message, id types.MessageID, nodes []waBinary.Node) (whatsmeow.SendResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sendCalls++
	f.lastTo = to
	f.lastMsg = msg
	f.lastID = id
	f.lastNodes = nodes
	return f.sendResp, f.sendErr
}

func (f *fakeWAClient) BuildEdit(chat types.JID, id types.MessageID, newContent *waE2E.Message) *waE2E.Message {
	return newContent
}

func (f *fakeWAClient) BuildRevoke(chat, sender types.JID, id types.MessageID) *waE2E.Message {
	return &waE2E.Message{}
}

func (f *fakeWAClient) BuildReaction(chat, sender types.JID, id types.MessageID, reaction string) *waE2E.Message {
	return &waE2E.Message{
		ReactionMessage: &waE2E.ReactionMessage{
			Key:  &waCommon.MessageKey{ID: proto.String(string(id))},
			Text: proto.String(reaction),
		},
	}
}

func (f *fakeWAClient) Upload(ctx context.Context, data []byte, mt whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	return whatsmeow.UploadResponse{}, nil
}

func (f *fakeWAClient) Download(ctx context.Context, msg whatsmeow.DownloadableMessage) ([]byte, error) {
	return nil, nil
}

func (f *fakeWAClient) PNForLID(ctx context.Context, lid types.JID) (types.JID, bool, error) {
	return types.JID{}, false, nil
}

func (f *fakeWAClient) DecryptSecretEncryptedMessage(ctx context.Context, evt *events.Message) (*waE2E.Message, error) {
	return nil, nil
}

func (f *fakeWAClient) DecryptPollVote(ctx context.Context, evt *events.Message) (*waE2E.PollVoteMessage, error) {
	return nil, nil
}

func (f *fakeWAClient) GetProfilePictureInfo(ctx context.Context, jid types.JID, params *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
	return nil, whatsmeow.ErrProfilePictureNotSet
}

func (f *fakeWAClient) CreateGroup(ctx context.Context, req whatsmeow.ReqCreateGroup) (*types.GroupInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.groupErr != nil {
		return nil, f.groupErr
	}
	f.nextGroupSeq++
	jid := types.NewJID(fmt.Sprintf("12036342254761%04d", f.nextGroupSeq), types.GroupServer)
	info := &types.GroupInfo{JID: jid, GroupCreated: time.Now()}
	info.Name = req.Name
	info.IsAnnounce = req.IsAnnounce
	info.Participants = []types.GroupParticipant{{JID: types.NewJID("15550000000", types.DefaultUserServer), IsSuperAdmin: true}}
	if f.groups == nil {
		f.groups = map[string]*types.GroupInfo{}
	}
	f.groups[jid.String()] = info
	f.createdGroups = append(f.createdGroups, req)
	return info, nil
}

func (f *fakeWAClient) GetGroupInfo(ctx context.Context, jid types.JID) (*types.GroupInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if info, ok := f.groups[jid.String()]; ok {
		return info, nil
	}
	return nil, whatsmeow.ErrGroupNotFound
}

func (f *fakeWAClient) GetGroupInviteLink(ctx context.Context, jid types.JID, reset bool) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.groupErr != nil {
		return "", f.groupErr
	}
	if _, ok := f.groups[jid.String()]; !ok {
		return "", whatsmeow.ErrGroupNotFound
	}
	if f.inviteLinks == nil {
		f.inviteLinks = map[string]string{}
	}
	if link, ok := f.inviteLinks[jid.String()]; ok && !reset {
		return link, nil
	}
	link := whatsmeow.InviteLinkPrefix + jid.User + "-" + strconv.Itoa(len(f.inviteLinks)+1)
	f.inviteLinks[jid.String()] = link
	return link, nil
}

func (f *fakeWAClient) SetGroupAnnounce(ctx context.Context, jid types.JID, announce bool) error {
	f.mu.Lock()
	delay := f.announceDelay
	f.mu.Unlock()
	time.Sleep(delay)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.announceCount++
	if f.groupErr != nil {
		return f.groupErr
	}
	if _, ok := f.groups[jid.String()]; !ok {
		return whatsmeow.ErrGroupNotFound
	}
	if f.announceCalls == nil {
		f.announceCalls = map[string]bool{}
	}
	f.announceCalls[jid.String()] = announce
	if info, ok := f.groups[jid.String()]; ok {
		info.IsAnnounce = announce
	}
	return nil
}

func (f *fakeWAClient) SetGroupName(ctx context.Context, jid types.JID, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.nameCalls == nil {
		f.nameCalls = map[string][]string{}
	}
	f.nameCalls[jid.String()] = append(f.nameCalls[jid.String()], name)
	return f.groupErr
}

func (f *fakeWAClient) SetGroupTopic(ctx context.Context, jid types.JID, topic string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.topicCalls == nil {
		f.topicCalls = map[string]string{}
	}
	f.topicCalls[jid.String()] = topic
	return f.groupErr
}

func (f *fakeWAClient) SetGroupPhoto(ctx context.Context, jid types.JID, jpeg []byte) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.photoCalls == nil {
		f.photoCalls = map[string][]byte{}
	}
	f.photoCalls[jid.String()] = jpeg
	return "pic-1", f.groupErr
}

func (f *fakeWAClient) UpdateGroupParticipants(ctx context.Context, jid types.JID, participants []types.JID, change whatsmeow.ParticipantChange) ([]types.GroupParticipant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.participantCalls = append(f.participantCalls, participantCall{group: jid.String(), participants: participants, change: change})
	out := make([]types.GroupParticipant, 0, len(participants))
	for _, p := range participants {
		out = append(out, types.GroupParticipant{JID: p})
	}
	return out, f.groupErr
}

func (f *fakeWAClient) GetJoinedGroups(ctx context.Context) ([]*types.GroupInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*types.GroupInfo, 0, len(f.groups))
	for _, info := range f.groups {
		out = append(out, info)
	}
	return out, f.groupErr
}

func (f *fakeWAClient) AddEventHandler(handler func(any)) uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handlers = append(f.handlers, handler)
	return uint32(len(f.handlers))
}

func (f *fakeWAClient) Calls() call.Caller {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.caller
}

func (f *fakeWAClient) Disconnect() {
	f.mu.Lock()
	f.disconnectCalls++
	f.connected = false
	f.mu.Unlock()
}

func (f *fakeWAClient) dropSocket() {
	f.mu.Lock()
	f.connected = false
	f.loggedIn = true
	f.mu.Unlock()
}

func (f *fakeWAClient) markPaired() {
	jid := types.NewJID("15550000000", types.DefaultUserServer)
	f.mu.Lock()
	f.deviceJID = &jid
	f.mu.Unlock()
}

func (f *fakeWAClient) addGroupParticipant(group string, participant types.GroupParticipant) {
	f.mu.Lock()
	defer f.mu.Unlock()
	info := f.groups[group]
	info.Participants = append(info.Participants, participant)
}

func (f *fakeWAClient) announceCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.announceCount
}

func (f *fakeWAClient) handlerCountAtConnect() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.handlersAtConnect
}

func (f *fakeWAClient) disconnectCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.disconnectCalls
}

func (f *fakeWAClient) sendCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sendCalls
}

func (f *fakeWAClient) qrChannelCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.qrChannelCalls
}

func (f *fakeWAClient) connectCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connectCalls
}

func (f *fakeWAClient) emit(evt any) {
	f.mu.Lock()
	handlers := append([]func(any){}, f.handlers...)
	f.mu.Unlock()

	for _, h := range handlers {
		h(evt)
	}
}
