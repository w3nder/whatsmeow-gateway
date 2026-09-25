package groups

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

const (
	ActionLock               = "lock"
	ActionUnlock             = "unlock"
	ActionRemoveParticipants = "remove_participants"
	ActionSetName            = "set_name"
	ActionSetDescription     = "set_description"
	ActionSetPhoto           = "set_photo"
)

var Actions = []string{ActionLock, ActionUnlock, ActionRemoveParticipants, ActionSetName, ActionSetDescription, ActionSetPhoto}

type CreateRequest struct {
	Name        string
	Description string
	PhotoURL    string
	Announce    bool
}

type CreateResult struct {
	GroupJID         string
	InviteURL        string
	ParticipantCount int
	CreatedAt        time.Time
}

type Info struct {
	GroupJID         string
	Name             string
	Announce         bool
	ParticipantCount int
	CreatedAt        time.Time
}

type ActionResult struct {
	Removed *int
}

func Create(ctx context.Context, c GroupClient, fetch Fetch, req CreateRequest, log *slog.Logger) (CreateResult, error) {
	if strings.TrimSpace(req.Name) == "" {
		return CreateResult{}, amqp.RpcInvalidRequest("group name is required")
	}
	info, err := c.CreateGroup(ctx, whatsmeow.ReqCreateGroup{Name: req.Name, GroupAnnounce: types.GroupAnnounce{IsAnnounce: req.Announce}})
	if err != nil {
		return CreateResult{}, Classify(err)
	}
	if req.Description != "" {
		if err := c.SetGroupTopic(ctx, info.JID, req.Description); err != nil {
			log.Warn("groups: set description after create", "group_jid", info.JID.String(), "error", err)
		}
	}
	if req.PhotoURL != "" {
		if err := setPhotoFromURL(ctx, c, fetch, info.JID, req.PhotoURL); err != nil {
			log.Warn("groups: set photo after create", "group_jid", info.JID.String(), "error", err)
		}
	}
	link, err := inviteLinkWithRetry(ctx, c, info.JID)
	if err != nil {
		log.Warn("groups: invite link after create, the client must fetch it later", "group_jid", info.JID.String(), "error", err)
	}
	return CreateResult{
		GroupJID:         info.JID.String(),
		InviteURL:        link,
		ParticipantCount: participantCount(info),
		CreatedAt:        createdAt(info),
	}, nil
}

var inviteRetryDelays = []time.Duration{200 * time.Millisecond, 500 * time.Millisecond, time.Second}

func inviteLinkWithRetry(ctx context.Context, c GroupClient, jid types.JID) (string, error) {
	link, err := c.GetGroupInviteLink(ctx, jid, false)
	for _, delay := range inviteRetryDelays {
		if err == nil {
			return link, nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", err
		case <-timer.C:
		}
		link, err = c.GetGroupInviteLink(ctx, jid, false)
	}
	return link, err
}

func InviteLink(ctx context.Context, c GroupClient, jid types.JID, reset bool) (string, error) {
	link, err := c.GetGroupInviteLink(ctx, jid, reset)
	if err != nil {
		return "", Classify(err)
	}
	return link, nil
}

func Describe(ctx context.Context, c GroupClient, jid types.JID) (Info, error) {
	info, err := c.GetGroupInfo(ctx, jid)
	if err != nil {
		return Info{}, Classify(err)
	}
	return infoOf(info), nil
}

func Joined(ctx context.Context, c GroupClient) ([]Info, error) {
	list, err := c.GetJoinedGroups(ctx)
	if err != nil {
		return nil, Classify(err)
	}
	out := make([]Info, 0, len(list))
	for _, info := range list {
		out = append(out, infoOf(info))
	}
	return out, nil
}

func Apply(ctx context.Context, c GroupClient, action string, jid types.JID, params amqp.GroupActionParams, photo []byte) (ActionResult, error) {
	var err error
	switch action {
	case ActionLock:
		err = c.SetGroupAnnounce(ctx, jid, true)
	case ActionUnlock:
		err = c.SetGroupAnnounce(ctx, jid, false)
	case ActionSetName:
		if strings.TrimSpace(params.Name) == "" {
			return ActionResult{}, amqp.RpcInvalidRequest("name is required")
		}
		err = c.SetGroupName(ctx, jid, params.Name)
	case ActionSetDescription:
		err = c.SetGroupTopic(ctx, jid, params.Description)
	case ActionSetPhoto:
		if len(photo) == 0 {
			return ActionResult{}, amqp.RpcInvalidRequest("photo is required")
		}
		_, err = c.SetGroupPhoto(ctx, jid, photo)
	case ActionRemoveParticipants:
		return removeParticipants(ctx, c, jid, params.Phones)
	default:
		return ActionResult{}, amqp.RpcInvalidRequest(fmt.Sprintf("unknown action %q", action))
	}
	if err != nil {
		return ActionResult{}, Classify(err)
	}
	return ActionResult{}, nil
}

func removeParticipants(ctx context.Context, c GroupClient, jid types.JID, phones []string) (ActionResult, error) {
	info, err := c.GetGroupInfo(ctx, jid)
	if err != nil {
		return ActionResult{}, Classify(err)
	}
	wanted := wantedPhones(phones)
	self := selfOf(c)
	targets := make([]types.JID, 0, len(phones))
	for _, p := range info.Participants {
		if self.is(p) {
			continue
		}
		if matchesPhone(ctx, c, p, wanted) {
			targets = append(targets, p.JID)
		}
	}
	removed := 0
	if len(targets) > 0 {
		results, err := c.UpdateGroupParticipants(ctx, jid, targets, whatsmeow.ParticipantChangeRemove)
		if err != nil {
			return ActionResult{}, Classify(err)
		}
		for _, r := range results {
			if r.Error == 0 {
				removed++
			}
		}
	}
	return ActionResult{Removed: &removed}, nil
}

func matchesPhone(ctx context.Context, c GroupClient, p types.GroupParticipant, wanted map[string]struct{}) bool {
	if _, ok := wanted[p.PhoneNumber.User]; ok && p.PhoneNumber.User != "" {
		return true
	}
	if p.JID.Server == types.DefaultUserServer {
		if _, ok := wanted[p.JID.User]; ok {
			return true
		}
	}
	if p.PhoneNumber.User == "" && p.LID.User != "" {
		resolved, ok, err := c.PNForLID(ctx, p.LID)
		if err == nil && ok {
			_, match := wanted[resolved.User]
			return match
		}
	}
	return false
}

func wantedPhones(phones []string) map[string]struct{} {
	wanted := make(map[string]struct{}, len(phones)*2)
	for _, phone := range phones {
		number := strings.TrimLeft(phone, "+")
		wanted[number] = struct{}{}
		if sibling, ok := brazilianSibling(number); ok {
			wanted[sibling] = struct{}{}
		}
	}
	return wanted
}

const (
	brazilCountryCode = "55"
	brazilPrefixLen   = 4
	brazilLandlineLen = 12
	brazilMobileLen   = 13
	brazilMobileDigit = '9'
)

func brazilianSibling(number string) (string, bool) {
	if !strings.HasPrefix(number, brazilCountryCode) {
		return "", false
	}
	switch len(number) {
	case brazilLandlineLen:
		return number[:brazilPrefixLen] + string(brazilMobileDigit) + number[brazilPrefixLen:], true
	case brazilMobileLen:
		if number[brazilPrefixLen] != brazilMobileDigit {
			return "", false
		}
		return number[:brazilPrefixLen] + number[brazilPrefixLen+1:], true
	default:
		return "", false
	}
}

type selfIdentity struct {
	phone string
	lid   string
}

func selfOf(c GroupClient) selfIdentity {
	var self selfIdentity
	if jid := c.DeviceJID(); jid != nil {
		self.phone = jid.User
	}
	self.lid = c.DeviceLID().User
	return self
}

func (s selfIdentity) is(p types.GroupParticipant) bool {
	if s.phone != "" && (p.PhoneNumber.User == s.phone || (p.JID.Server == types.DefaultUserServer && p.JID.User == s.phone)) {
		return true
	}
	return s.lid != "" && (p.LID.User == s.lid || (p.JID.Server == types.HiddenUserServer && p.JID.User == s.lid))
}

func PreparePhoto(ctx context.Context, fetch Fetch, url string) ([]byte, error) {
	if url == "" {
		return nil, amqp.RpcInvalidRequest("photoUrl is required")
	}
	if fetch == nil {
		return nil, amqp.RpcInvalidRequest("photo fetch is not available")
	}
	raw, err := fetch(ctx, url)
	if err != nil {
		return nil, amqp.RpcInvalidRequest(fmt.Sprintf("fetch photo: %v", err))
	}
	jpg, err := ToJPEG(raw)
	if err != nil {
		return nil, amqp.RpcInvalidRequest(err.Error())
	}
	return jpg, nil
}

func setPhotoFromURL(ctx context.Context, c GroupClient, fetch Fetch, jid types.JID, url string) error {
	jpg, err := PreparePhoto(ctx, fetch, url)
	if err != nil {
		return err
	}
	_, err = c.SetGroupPhoto(ctx, jid, jpg)
	return err
}

func infoOf(info *types.GroupInfo) Info {
	return Info{
		GroupJID:         info.JID.String(),
		Name:             info.Name,
		Announce:         info.IsAnnounce,
		ParticipantCount: participantCount(info),
		CreatedAt:        createdAt(info),
	}
}

func participantCount(info *types.GroupInfo) int {
	if info.ParticipantCount > 0 {
		return info.ParticipantCount
	}
	if len(info.Participants) > 0 {
		return len(info.Participants)
	}
	return 1
}

func createdAt(info *types.GroupInfo) time.Time {
	if info.GroupCreated.IsZero() {
		return time.Now().UTC()
	}
	return info.GroupCreated.UTC()
}
