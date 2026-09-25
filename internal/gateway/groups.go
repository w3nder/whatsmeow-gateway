package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/groups"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

const (
	rpcGroupCreate     = "group.create"
	rpcGroupInviteLink = "group.invite_link"
	rpcGroupInfo       = "group.info"
	rpcGroupJoined     = "group.joined"
)

type groupCreateRequest struct {
	TenantID    string `json:"tenantId"`
	ChannelID   string `json:"channelId"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	PhotoURL    string `json:"photoUrl,omitempty"`
	Announce    bool   `json:"announce"`
}

type groupCreateResponse struct {
	GroupJID         string `json:"groupJid"`
	InviteURL        string `json:"inviteUrl"`
	ParticipantCount int    `json:"participantCount"`
	CreatedAt        string `json:"createdAt"`
}

type groupInviteLinkRequest struct {
	TenantID  string `json:"tenantId"`
	ChannelID string `json:"channelId"`
	GroupJID  string `json:"groupJid"`
	Reset     bool   `json:"reset"`
}

type groupInviteLinkResponse struct {
	InviteURL string `json:"inviteUrl"`
}

type groupInfoRequest struct {
	TenantID  string `json:"tenantId"`
	ChannelID string `json:"channelId"`
	GroupJID  string `json:"groupJid"`
}

type groupInfoResponse struct {
	GroupJID         string `json:"groupJid"`
	Name             string `json:"name"`
	Announce         bool   `json:"announce"`
	ParticipantCount int    `json:"participantCount"`
	CreatedAt        string `json:"createdAt"`
}

type groupJoinedRequest struct {
	TenantID  string `json:"tenantId"`
	ChannelID string `json:"channelId"`
}

type groupJoinedResponse struct {
	Groups []groupInfoResponse `json:"groups"`
}

func (g *gateway) registerGroupRpc(ctx context.Context) error {
	handlers := map[string]amqp.RpcHandler{
		rpcGroupCreate:     g.rpcGroupCreate,
		rpcGroupInviteLink: g.rpcGroupInviteLink,
		rpcGroupInfo:       g.rpcGroupInfo,
		rpcGroupJoined:     g.rpcGroupJoined,
	}
	for operation, handler := range handlers {
		if err := g.rpc.Handle(ctx, operation, handler); err != nil {
			return fmt.Errorf("gateway: register rpc %s: %w", operation, err)
		}
	}
	return nil
}

func decodeRpc[T any](payload json.RawMessage) (T, error) {
	var req T
	if err := json.Unmarshal(payload, &req); err != nil {
		return req, amqp.RpcInvalidRequest(err.Error())
	}
	return req, nil
}

func (g *gateway) groupClient(ctx context.Context, tenantID, channelID string) (session.WAClient, error) {
	if channelID == "" {
		return nil, amqp.RpcInvalidRequest("channelId is required")
	}
	g.setTenant(channelID, tenantID)
	if err := g.ensureChannelConnected(ctx, channelID); err != nil {
		if errors.Is(err, session.ErrNoSession) {
			return nil, amqp.RpcNotFound(err.Error())
		}
		return nil, amqp.RpcUnavailable(err.Error())
	}
	client, err := g.waClientFor(channelID)
	if err != nil {
		return nil, amqp.RpcUnavailable(err.Error())
	}
	return client, nil
}

func parseGroupJID(raw string) (types.JID, error) {
	jid, err := types.ParseJID(raw)
	if err != nil || jid.Server != types.GroupServer {
		return types.JID{}, amqp.RpcInvalidRequest(fmt.Sprintf("groupJid %q is not a group jid", raw))
	}
	return jid, nil
}

func (g *gateway) rpcGroupCreate(ctx context.Context, payload json.RawMessage) (any, error) {
	req, err := decodeRpc[groupCreateRequest](payload)
	if err != nil {
		return nil, err
	}
	client, err := g.groupClient(ctx, req.TenantID, req.ChannelID)
	if err != nil {
		return nil, err
	}
	res, err := groups.Create(ctx, client, fetchMediaURL, groups.CreateRequest{
		Name: req.Name, Description: req.Description, PhotoURL: req.PhotoURL, Announce: req.Announce,
	}, g.logger)
	if err != nil {
		return nil, err
	}
	g.logger.Info("gateway: group created", "channel_id", req.ChannelID, "group_jid", res.GroupJID)
	return groupCreateResponse{
		GroupJID:         res.GroupJID,
		InviteURL:        res.InviteURL,
		ParticipantCount: res.ParticipantCount,
		CreatedAt:        res.CreatedAt.Format(time.RFC3339),
	}, nil
}

func (g *gateway) rpcGroupInviteLink(ctx context.Context, payload json.RawMessage) (any, error) {
	req, err := decodeRpc[groupInviteLinkRequest](payload)
	if err != nil {
		return nil, err
	}
	jid, err := parseGroupJID(req.GroupJID)
	if err != nil {
		return nil, err
	}
	client, err := g.groupClient(ctx, req.TenantID, req.ChannelID)
	if err != nil {
		return nil, err
	}
	link, err := groups.InviteLink(ctx, client, jid, req.Reset)
	if err != nil {
		return nil, err
	}
	return groupInviteLinkResponse{InviteURL: link}, nil
}

func (g *gateway) rpcGroupInfo(ctx context.Context, payload json.RawMessage) (any, error) {
	req, err := decodeRpc[groupInfoRequest](payload)
	if err != nil {
		return nil, err
	}
	jid, err := parseGroupJID(req.GroupJID)
	if err != nil {
		return nil, err
	}
	client, err := g.groupClient(ctx, req.TenantID, req.ChannelID)
	if err != nil {
		return nil, err
	}
	info, err := groups.Describe(ctx, client, jid)
	if err != nil {
		return nil, err
	}
	return infoResponse(info), nil
}

func (g *gateway) rpcGroupJoined(ctx context.Context, payload json.RawMessage) (any, error) {
	req, err := decodeRpc[groupJoinedRequest](payload)
	if err != nil {
		return nil, err
	}
	client, err := g.groupClient(ctx, req.TenantID, req.ChannelID)
	if err != nil {
		return nil, err
	}
	list, err := groups.Joined(ctx, client)
	if err != nil {
		return nil, err
	}
	out := groupJoinedResponse{Groups: make([]groupInfoResponse, 0, len(list))}
	for _, info := range list {
		out.Groups = append(out.Groups, infoResponse(info))
	}
	return out, nil
}

func infoResponse(info groups.Info) groupInfoResponse {
	return groupInfoResponse{
		GroupJID:         info.GroupJID,
		Name:             info.Name,
		Announce:         info.Announce,
		ParticipantCount: info.ParticipantCount,
		CreatedAt:        info.CreatedAt.Format(time.RFC3339),
	}
}
