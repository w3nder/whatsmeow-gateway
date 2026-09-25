package groups

import (
	"context"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

type GroupClient interface {
	GetGroupInfo(ctx context.Context, jid types.JID) (*types.GroupInfo, error)
	CreateGroup(ctx context.Context, req whatsmeow.ReqCreateGroup) (*types.GroupInfo, error)
	GetGroupInviteLink(ctx context.Context, jid types.JID, reset bool) (string, error)
	SetGroupAnnounce(ctx context.Context, jid types.JID, announce bool) error
	SetGroupName(ctx context.Context, jid types.JID, name string) error
	SetGroupTopic(ctx context.Context, jid types.JID, topic string) error
	SetGroupPhoto(ctx context.Context, jid types.JID, jpeg []byte) (string, error)
	UpdateGroupParticipants(ctx context.Context, jid types.JID, participants []types.JID, change whatsmeow.ParticipantChange) ([]types.GroupParticipant, error)
	GetJoinedGroups(ctx context.Context) ([]*types.GroupInfo, error)
	PNForLID(ctx context.Context, lid types.JID) (types.JID, bool, error)
}

type Fetch func(ctx context.Context, url string) ([]byte, error)
