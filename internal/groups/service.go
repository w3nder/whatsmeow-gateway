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
		if err := setPhoto(ctx, c, fetch, info.JID, req.PhotoURL); err != nil {
			log.Warn("groups: set photo after create", "group_jid", info.JID.String(), "error", err)
		}
	}
	link, err := c.GetGroupInviteLink(ctx, info.JID, false)
	if err != nil {
		return CreateResult{}, Classify(err)
	}
	return CreateResult{
		GroupJID:         info.JID.String(),
		InviteURL:        link,
		ParticipantCount: participantCount(info),
		CreatedAt:        createdAt(info),
	}, nil
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

func Apply(ctx context.Context, c GroupClient, fetch Fetch, action string, jid types.JID, params amqp.GroupActionParams) (ActionResult, error) {
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
		if params.PhotoURL == "" {
			return ActionResult{}, amqp.RpcInvalidRequest("photoUrl is required")
		}
		err = setPhoto(ctx, c, fetch, jid, params.PhotoURL)
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
	wanted := make(map[string]struct{}, len(phones))
	for _, phone := range phones {
		wanted[strings.TrimLeft(phone, "+")] = struct{}{}
	}
	targets := make([]types.JID, 0, len(phones))
	for _, p := range info.Participants {
		if _, ok := wanted[p.PhoneNumber.User]; ok {
			targets = append(targets, p.JID)
			continue
		}
		if p.JID.Server == types.DefaultUserServer {
			if _, ok := wanted[p.JID.User]; ok {
				targets = append(targets, p.JID)
			}
		}
	}
	removed := 0
	if len(targets) > 0 {
		if _, err := c.UpdateGroupParticipants(ctx, jid, targets, whatsmeow.ParticipantChangeRemove); err != nil {
			return ActionResult{}, Classify(err)
		}
		removed = len(targets)
	}
	return ActionResult{Removed: &removed}, nil
}

func setPhoto(ctx context.Context, c GroupClient, fetch Fetch, jid types.JID, url string) error {
	if fetch == nil {
		return amqp.RpcInvalidRequest("photo fetch is not available")
	}
	raw, err := fetch(ctx, url)
	if err != nil {
		return amqp.RpcInvalidRequest(fmt.Sprintf("fetch photo: %v", err))
	}
	jpg, err := ToJPEG(raw)
	if err != nil {
		return amqp.RpcInvalidRequest(err.Error())
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
