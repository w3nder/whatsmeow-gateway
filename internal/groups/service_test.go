package groups_test

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/jpeg"
	"image/png"
	"log/slog"
	"testing"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/groups"
)

type stubClient struct {
	created        []whatsmeow.ReqCreateGroup
	topics         map[string]string
	photos         map[string][]byte
	announce       map[string]bool
	names          map[string]string
	removed        map[string][]types.JID
	info           map[string]*types.GroupInfo
	inviteErr      error
	inviteFailures int
	inviteCalls    int
	createErr      error
	photoErr       error
	own            *types.JID
	ownLID         types.JID
	refused        map[string]bool
}

func newStub() *stubClient {
	return &stubClient{topics: map[string]string{}, photos: map[string][]byte{}, announce: map[string]bool{}, names: map[string]string{}, removed: map[string][]types.JID{}, info: map[string]*types.GroupInfo{}}
}

func (s *stubClient) GetGroupInfo(_ context.Context, jid types.JID) (*types.GroupInfo, error) {
	if info, ok := s.info[jid.String()]; ok {
		return info, nil
	}
	return nil, whatsmeow.ErrGroupNotFound
}

func (s *stubClient) CreateGroup(_ context.Context, req whatsmeow.ReqCreateGroup) (*types.GroupInfo, error) {
	if s.createErr != nil {
		return nil, s.createErr
	}
	s.created = append(s.created, req)
	jid := types.NewJID("120363000000000001", types.GroupServer)
	info := &types.GroupInfo{JID: jid}
	info.Name = req.Name
	info.IsAnnounce = req.IsAnnounce
	info.Participants = []types.GroupParticipant{{JID: types.NewJID("15550000000", types.DefaultUserServer)}}
	s.info[jid.String()] = info
	return info, nil
}

func (s *stubClient) GetGroupInviteLink(_ context.Context, jid types.JID, reset bool) (string, error) {
	s.inviteCalls++
	if s.inviteCalls <= s.inviteFailures {
		return "", whatsmeow.ErrIQInternalServerError
	}
	if s.inviteErr != nil {
		return "", s.inviteErr
	}
	if reset {
		return whatsmeow.InviteLinkPrefix + "reset-" + jid.User, nil
	}
	return whatsmeow.InviteLinkPrefix + jid.User, nil
}

func (s *stubClient) SetGroupAnnounce(_ context.Context, jid types.JID, announce bool) error {
	s.announce[jid.String()] = announce
	return nil
}

func (s *stubClient) SetGroupName(_ context.Context, jid types.JID, name string) error {
	s.names[jid.String()] = name
	return nil
}

func (s *stubClient) SetGroupTopic(_ context.Context, jid types.JID, topic string) error {
	s.topics[jid.String()] = topic
	return nil
}

func (s *stubClient) SetGroupPhoto(_ context.Context, jid types.JID, jpeg []byte) (string, error) {
	if s.photoErr != nil {
		return "", s.photoErr
	}
	s.photos[jid.String()] = jpeg
	return "pic", nil
}

func (s *stubClient) UpdateGroupParticipants(_ context.Context, jid types.JID, participants []types.JID, change whatsmeow.ParticipantChange) ([]types.GroupParticipant, error) {
	s.removed[jid.String()] = append(s.removed[jid.String()], participants...)
	out := make([]types.GroupParticipant, 0, len(participants))
	for _, p := range participants {
		result := types.GroupParticipant{JID: p}
		if s.refused[p.User] {
			result.Error = 403
		}
		out = append(out, result)
	}
	return out, nil
}

func (s *stubClient) DeviceJID() *types.JID { return s.own }

func (s *stubClient) DeviceLID() types.JID { return s.ownLID }

func (s *stubClient) GetJoinedGroups(context.Context) ([]*types.GroupInfo, error) {
	out := make([]*types.GroupInfo, 0, len(s.info))
	for _, info := range s.info {
		out = append(out, info)
	}
	return out, nil
}

func (s *stubClient) PNForLID(_ context.Context, lid types.JID) (types.JID, bool, error) {
	if lid.User == "2002125877314" {
		return types.NewJID("5511999887766", types.DefaultUserServer), true, nil
	}
	return types.JID{}, false, nil
}

func pngBytes(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := png.Encode(&buf, image.NewRGBA(image.Rect(0, 0, 2, 2))); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func fetchOf(data []byte, err error) groups.Fetch {
	return func(context.Context, string) ([]byte, error) { return data, err }
}

func TestCreateAppliesEverythingAndReturnsLink(t *testing.T) {
	stub := newStub()
	res, err := groups.Create(context.Background(), stub, fetchOf(pngBytes(t), nil), groups.CreateRequest{
		Name: "GRUPO #1", Description: "Regras", PhotoURL: "https://s3/photo.png", Announce: true,
	}, slog.Default())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if res.GroupJID != "120363000000000001@g.us" || res.InviteURL != whatsmeow.InviteLinkPrefix+"120363000000000001" {
		t.Fatalf("result %+v", res)
	}
	if res.ParticipantCount != 1 {
		t.Fatalf("participantCount %d, want 1", res.ParticipantCount)
	}
	if !stub.created[0].IsAnnounce {
		t.Fatal("announce must be set on CreateGroup itself")
	}
	if stub.topics[res.GroupJID] != "Regras" {
		t.Fatalf("topic %q", stub.topics[res.GroupJID])
	}
	if _, err := jpeg.Decode(bytes.NewReader(stub.photos[res.GroupJID])); err != nil {
		t.Fatalf("photo must be sent as jpeg: %v", err)
	}
}

func TestCreateSurvivesPhotoFailure(t *testing.T) {
	stub := newStub()
	stub.photoErr = whatsmeow.ErrInvalidImageFormat
	res, err := groups.Create(context.Background(), stub, fetchOf(pngBytes(t), nil), groups.CreateRequest{Name: "G", PhotoURL: "https://s3/x"}, slog.Default())
	if err != nil || res.InviteURL == "" {
		t.Fatalf("a photo failure must not fail the creation: %v %+v", err, res)
	}
}

func TestCreateRetriesTheInviteLink(t *testing.T) {
	stub := newStub()
	stub.inviteFailures = 2
	res, err := groups.Create(context.Background(), stub, nil, groups.CreateRequest{Name: "G"}, slog.Default())
	if err != nil || res.InviteURL != whatsmeow.InviteLinkPrefix+"120363000000000001" {
		t.Fatalf("the link must come on the third try: %v %+v", err, res)
	}
	if stub.inviteCalls != 3 {
		t.Fatalf("invite calls %d, want 3", stub.inviteCalls)
	}
}

func TestCreateWithoutInviteLinkStillReturnsTheGroup(t *testing.T) {
	stub := newStub()
	stub.inviteErr = whatsmeow.ErrIQNotAuthorized
	res, err := groups.Create(context.Background(), stub, nil, groups.CreateRequest{Name: "G"}, slog.Default())
	if err != nil {
		t.Fatalf("a group that exists must not be reported as a failure: %v", err)
	}
	if res.GroupJID != "120363000000000001@g.us" || res.InviteURL != "" {
		t.Fatalf("want the real jid and an empty link, got %+v", res)
	}
	if stub.inviteCalls != 4 {
		t.Fatalf("invite calls %d, want 1 try and 3 retries", stub.inviteCalls)
	}
}

func TestCreateRejectsEmptyName(t *testing.T) {
	_, err := groups.Create(context.Background(), newStub(), nil, groups.CreateRequest{}, slog.Default())
	var rpcErr *amqp.RpcError
	if !errors.As(err, &rpcErr) || rpcErr.Code != amqp.RpcCodeInvalidRequest {
		t.Fatalf("want invalid_request, got %v", err)
	}
}

func TestApplyRemoveParticipantsResolvesPhonesAgainstTheGroup(t *testing.T) {
	stub := newStub()
	jid := types.NewJID("120363000000000009", types.GroupServer)
	stub.info[jid.String()] = &types.GroupInfo{JID: jid, Participants: []types.GroupParticipant{
		{JID: types.NewJID("2002125877314", types.HiddenUserServer), LID: types.NewJID("2002125877314", types.HiddenUserServer), PhoneNumber: types.NewJID("5511999887766", types.DefaultUserServer)},
		{JID: types.NewJID("5511888887777", types.DefaultUserServer), PhoneNumber: types.NewJID("5511888887777", types.DefaultUserServer)},
	}}

	res, err := groups.Apply(context.Background(), stub, groups.ActionRemoveParticipants, jid, amqp.GroupActionParams{Phones: []string{"5511999887766", "5500000000000"}}, nil)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if res.Removed == nil || *res.Removed != 1 {
		t.Fatalf("removed %v, want 1", res.Removed)
	}
	if got := stub.removed[jid.String()]; len(got) != 1 || got[0].User != "2002125877314" {
		t.Fatalf("must remove by the participant's primary JID (the LID here), got %v", got)
	}
}

func TestApplyRemoveParticipantsResolvesLIDOnlyParticipantThroughPNForLID(t *testing.T) {
	stub := newStub()
	jid := types.NewJID("120363000000000009", types.GroupServer)
	lid := types.NewJID("2002125877314", types.HiddenUserServer)
	stub.info[jid.String()] = &types.GroupInfo{JID: jid, Participants: []types.GroupParticipant{
		{JID: lid, LID: lid},
	}}

	res, err := groups.Apply(context.Background(), stub, groups.ActionRemoveParticipants, jid, amqp.GroupActionParams{Phones: []string{"5511999887766"}}, nil)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if res.Removed == nil || *res.Removed != 1 {
		t.Fatalf("removed %v, want 1", res.Removed)
	}
	if got := stub.removed[jid.String()]; len(got) != 1 || got[0].User != lid.User {
		t.Fatalf("must remove the lid-only participant resolved through PNForLID, got %v", got)
	}
}

func TestApplyRemoveParticipantsWithNoMatchIsOkWithZero(t *testing.T) {
	stub := newStub()
	jid := types.NewJID("120363000000000009", types.GroupServer)
	stub.info[jid.String()] = &types.GroupInfo{JID: jid}
	res, err := groups.Apply(context.Background(), stub, groups.ActionRemoveParticipants, jid, amqp.GroupActionParams{Phones: []string{"5500000000000"}}, nil)
	if err != nil || res.Removed == nil || *res.Removed != 0 {
		t.Fatalf("got %v %+v", err, res)
	}
	if len(stub.removed[jid.String()]) != 0 {
		t.Fatal("must not call UpdateGroupParticipants with an empty list")
	}
}

func TestApplyLockUnlockNameDescriptionPhoto(t *testing.T) {
	stub := newStub()
	jid := types.NewJID("120363000000000009", types.GroupServer)
	ctx := context.Background()
	if _, err := groups.Apply(ctx, stub, groups.ActionLock, jid, amqp.GroupActionParams{}, nil); err != nil || !stub.announce[jid.String()] {
		t.Fatalf("lock: %v %v", err, stub.announce)
	}
	if _, err := groups.Apply(ctx, stub, groups.ActionUnlock, jid, amqp.GroupActionParams{}, nil); err != nil || stub.announce[jid.String()] {
		t.Fatalf("unlock: %v %v", err, stub.announce)
	}
	if _, err := groups.Apply(ctx, stub, groups.ActionSetName, jid, amqp.GroupActionParams{Name: "Novo"}, nil); err != nil || stub.names[jid.String()] != "Novo" {
		t.Fatalf("set_name: %v %v", err, stub.names)
	}
	if _, err := groups.Apply(ctx, stub, groups.ActionSetDescription, jid, amqp.GroupActionParams{Description: "Desc"}, nil); err != nil || stub.topics[jid.String()] != "Desc" {
		t.Fatalf("set_description: %v %v", err, stub.topics)
	}
	photo, err := groups.PreparePhoto(ctx, fetchOf(pngBytes(t), nil), "https://s3/p")
	if err != nil {
		t.Fatalf("PreparePhoto: %v", err)
	}
	if _, err := groups.Apply(ctx, stub, groups.ActionSetPhoto, jid, amqp.GroupActionParams{PhotoURL: "https://s3/p"}, photo); err != nil || !bytes.Equal(stub.photos[jid.String()], photo) {
		t.Fatalf("set_photo: %v", err)
	}
	if _, err := groups.Apply(ctx, stub, groups.ActionSetPhoto, jid, amqp.GroupActionParams{PhotoURL: "https://s3/p"}, nil); err == nil {
		t.Fatal("set_photo without a prepared photo must fail")
	}
	if _, err := groups.Apply(ctx, stub, "explode", jid, amqp.GroupActionParams{}, nil); err == nil {
		t.Fatal("unknown action must fail")
	}
}

func TestPreparePhotoConvertsOnceAndRejectsMissingURL(t *testing.T) {
	fetches := 0
	fetch := func(context.Context, string) ([]byte, error) {
		fetches++
		return pngBytes(t), nil
	}
	photo, err := groups.PreparePhoto(context.Background(), fetch, "https://s3/p")
	if err != nil {
		t.Fatalf("PreparePhoto: %v", err)
	}
	if _, err := jpeg.Decode(bytes.NewReader(photo)); err != nil || fetches != 1 {
		t.Fatalf("want one fetch and a jpeg, got %d fetches, %v", fetches, err)
	}
	_, err = groups.PreparePhoto(context.Background(), fetch, "")
	var rpcErr *amqp.RpcError
	if !errors.As(err, &rpcErr) || rpcErr.Code != amqp.RpcCodeInvalidRequest {
		t.Fatalf("missing url must be invalid_request, got %v", err)
	}
}

func TestApplyRemoveParticipantsNeverRemovesTheChannelItself(t *testing.T) {
	stub := newStub()
	own := types.NewADJID("5511999887766", 0, 12)
	stub.own = &own
	stub.ownLID = types.NewJID("2002125877314", types.HiddenUserServer)
	jid := types.NewJID("120363000000000009", types.GroupServer)
	stub.info[jid.String()] = &types.GroupInfo{JID: jid, Participants: []types.GroupParticipant{
		{JID: types.NewJID("2002125877314", types.HiddenUserServer), LID: types.NewJID("2002125877314", types.HiddenUserServer)},
		{JID: types.NewJID("5511999887766", types.DefaultUserServer)},
		{JID: types.NewJID("5511888887777", types.DefaultUserServer)},
	}}

	res, err := groups.Apply(context.Background(), stub, groups.ActionRemoveParticipants, jid, amqp.GroupActionParams{Phones: []string{"5511999887766", "5511888887777"}}, nil)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := stub.removed[jid.String()]; len(got) != 1 || got[0].User != "5511888887777" {
		t.Fatalf("the channel's own pn and lid must never be targeted, got %v", got)
	}
	if res.Removed == nil || *res.Removed != 1 {
		t.Fatalf("removed %v, want 1", res.Removed)
	}
}

func TestApplyRemoveParticipantsCountsOnlyAcceptedRemovals(t *testing.T) {
	stub := newStub()
	stub.refused = map[string]bool{"5511777776666": true}
	jid := types.NewJID("120363000000000009", types.GroupServer)
	stub.info[jid.String()] = &types.GroupInfo{JID: jid, Participants: []types.GroupParticipant{
		{JID: types.NewJID("5511888887777", types.DefaultUserServer)},
		{JID: types.NewJID("5511777776666", types.DefaultUserServer)},
	}}

	res, err := groups.Apply(context.Background(), stub, groups.ActionRemoveParticipants, jid, amqp.GroupActionParams{Phones: []string{"5511888887777", "5511777776666"}}, nil)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(stub.removed[jid.String()]) != 2 || res.Removed == nil || *res.Removed != 1 {
		t.Fatalf("a participant whatsapp refused must not count, removed %v", res.Removed)
	}
}

func TestApplyRemoveParticipantsMatchesTheBrazilianNinthDigitBothWays(t *testing.T) {
	jid := types.NewJID("120363000000000009", types.GroupServer)
	cases := []struct {
		name        string
		phone       string
		participant string
	}{
		{"asked without the 9, member has it", "551188887777", "5511988887777"},
		{"asked with the 9, member lacks it", "+5511988887777", "551188887777"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := newStub()
			stub.info[jid.String()] = &types.GroupInfo{JID: jid, Participants: []types.GroupParticipant{
				{JID: types.NewJID(tc.participant, types.DefaultUserServer)},
				{JID: types.NewJID("5521988887777", types.DefaultUserServer)},
			}}
			res, err := groups.Apply(context.Background(), stub, groups.ActionRemoveParticipants, jid, amqp.GroupActionParams{Phones: []string{tc.phone}}, nil)
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if got := stub.removed[jid.String()]; len(got) != 1 || got[0].User != tc.participant || *res.Removed != 1 {
				t.Fatalf("want only %s removed, got %v", tc.participant, got)
			}
		})
	}
}

func TestClassifyMapsWhatsmeowErrors(t *testing.T) {
	cases := map[error]string{
		whatsmeow.ErrGroupNotFound:         amqp.RpcCodeBadGateway,
		whatsmeow.ErrNotInGroup:            amqp.RpcCodeBadGateway,
		whatsmeow.ErrIQNotAuthorized:       amqp.RpcCodeBadGateway,
		whatsmeow.ErrInvalidImageFormat:    amqp.RpcCodeInvalidRequest,
		whatsmeow.ErrIQTimedOut:            amqp.RpcCodeUnavailable,
		whatsmeow.ErrNotConnected:          amqp.RpcCodeUnavailable,
		whatsmeow.ErrNotLoggedIn:           amqp.RpcCodeUnavailable,
		whatsmeow.ErrIQRateOverLimit:       amqp.RpcCodeUnavailable,
		whatsmeow.ErrIQInternalServerError: amqp.RpcCodeUnavailable,
		whatsmeow.ErrIQServiceUnavailable:  amqp.RpcCodeUnavailable,
		context.DeadlineExceeded:           amqp.RpcCodeUnavailable,
		context.Canceled:                   amqp.RpcCodeUnavailable,
		errors.New("anything else"):        amqp.RpcCodeInternal,
	}
	for in, want := range cases {
		var rpcErr *amqp.RpcError
		if !errors.As(groups.Classify(in), &rpcErr) || rpcErr.Code != want {
			t.Fatalf("%v → %v, want %s", in, groups.Classify(in), want)
		}
	}
}
