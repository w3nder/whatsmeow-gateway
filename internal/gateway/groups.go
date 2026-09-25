package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

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

type groupRpcRoute struct {
	operation string
	handler   amqp.RpcHandler
}

func (g *gateway) registerGroupRpc(ctx context.Context) error {
	routes := []groupRpcRoute{
		{rpcGroupCreate, g.rpcGroupCreate},
		{rpcGroupInviteLink, g.rpcGroupInviteLink},
		{rpcGroupInfo, g.rpcGroupInfo},
		{rpcGroupJoined, g.rpcGroupJoined},
	}
	for _, route := range routes {
		if err := g.rpc.Handle(ctx, route.operation, route.handler); err != nil {
			return fmt.Errorf("gateway: register rpc %s: %w", route.operation, err)
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
	if g.stopping.Load() {
		return nil, amqp.RpcUnavailable("gateway is shutting down, the channel is not reopened here")
	}
	if err := g.ensureChannelConnected(ctx, channelID); err != nil {
		if errors.Is(err, session.ErrNoSession) {
			return nil, amqp.RpcNotFound(err.Error())
		}
		return nil, amqp.RpcUnavailable(err.Error())
	}
	g.setTenant(channelID, tenantID)
	client, err := g.waClientFor(channelID)
	if err != nil {
		return nil, amqp.RpcUnavailable(err.Error())
	}
	return client, nil
}

func (g *gateway) handleGroupInfo(channelID string, e *events.GroupInfo) {
	if e.Name != nil {
		g.groups.Invalidate(channelID, e.JID)
	}
	client, err := g.waClientFor(channelID)
	if err != nil {
		g.logger.Error("gateway: resolve client for group info", "channel_id", channelID, "error", err)
		return
	}
	for _, evt := range BuildGroupParticipants(g.workCtx, client, g.logger, g.tenantFor(channelID), channelID, e) {
		if err := g.publisher.PublishGroupParticipants(g.workCtx, evt); err != nil {
			g.logger.Error("gateway: publish group participants", "channel_id", channelID, "group_jid", evt.GroupJID, "type", evt.Type, "error", err)
		}
	}
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
	res, err := groups.Create(ctx, client, fetchGroupPhoto, groups.CreateRequest{
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

const (
	groupItemTimeout = 30 * time.Second
	groupGapMin      = 300 * time.Millisecond
	groupGapJitter   = 500 * time.Millisecond
)

var errShuttingDown = fmt.Errorf("gateway: shutting down in the middle of a group command: %w", amqp.ErrRequeue)

type groupCommandRun struct {
	cmd      amqp.GatewayGroupCommand
	client   session.WAClient
	setupErr error
	photo    []byte
	touched  bool
}

func (g *gateway) GroupHandler(ctx context.Context, cmd amqp.GatewayGroupCommand) error {
	g.logger.Info("gateway: group command received", "command_id", cmd.CommandID, "channel_id", cmd.ChannelID, "action", cmd.Action, "groups", len(cmd.GroupJIDs))
	if ctx.Err() != nil {
		return errShuttingDown
	}
	work := context.WithoutCancel(ctx)
	run := g.prepareGroupCommand(work, cmd)
	for _, raw := range cmd.GroupJIDs {
		if ctx.Err() != nil {
			return errShuttingDown
		}
		alreadyDone, removed, err := g.dedupe.BeginAction(work, cmd.CommandID, raw)
		if err != nil {
			return fmt.Errorf("gateway: begin action %s/%s: %w", cmd.CommandID, raw, err)
		}
		result := amqp.GroupActionEvent{TenantID: cmd.TenantID, ChannelID: cmd.ChannelID, CommandID: cmd.CommandID, GroupJID: raw, Action: cmd.Action, OK: true}
		if alreadyDone {
			result.Removed = removed
		} else {
			result, err = g.applyGroupAction(ctx, work, run, raw, result)
			if err != nil {
				return err
			}
		}
		if err := g.publisher.PublishGroupAction(work, result); err != nil {
			return fmt.Errorf("gateway: publish group action %s/%s: %w", cmd.CommandID, raw, err)
		}
	}
	return nil
}

func (g *gateway) prepareGroupCommand(ctx context.Context, cmd amqp.GatewayGroupCommand) *groupCommandRun {
	run := &groupCommandRun{cmd: cmd}
	run.client, run.setupErr = g.groupClient(ctx, cmd.TenantID, cmd.ChannelID)
	if run.setupErr != nil || cmd.Action != groups.ActionSetPhoto {
		return run
	}
	photoCtx, cancel := context.WithTimeout(ctx, groupItemTimeout)
	defer cancel()
	run.photo, run.setupErr = groups.PreparePhoto(photoCtx, fetchGroupPhoto, cmd.Params.PhotoURL)
	return run
}

func (g *gateway) applyGroupAction(ctx, work context.Context, run *groupCommandRun, raw string, result amqp.GroupActionEvent) (amqp.GroupActionEvent, error) {
	fail := func(err error) (amqp.GroupActionEvent, error) {
		result.OK = false
		result.Error = err.Error()
		return result, nil
	}
	if run.setupErr != nil {
		return fail(run.setupErr)
	}
	jid, err := parseGroupJID(raw)
	if err != nil {
		return fail(err)
	}
	if run.touched {
		if err := pauseBetweenGroups(ctx); err != nil {
			return result, err
		}
	}
	run.touched = true
	itemCtx, cancel := context.WithTimeout(work, groupItemTimeout)
	defer cancel()
	applied, err := groups.Apply(itemCtx, run.client, run.cmd.Action, jid, run.cmd.Params, run.photo, g.logger)
	if groups.IsUnavailable(err) && ctx.Err() != nil {
		return result, errShuttingDown
	}
	if groups.IsUnavailable(err) {
		return result, fmt.Errorf("gateway: whatsapp unavailable at %s/%s, halting the command for a later replay: %w", run.cmd.CommandID, raw, err)
	}
	if err != nil {
		return fail(err)
	}
	result.Removed = applied.Removed
	if err := g.dedupe.MarkActionDone(work, run.cmd.CommandID, raw, applied.Removed); err != nil {
		g.logger.Error("gateway: mark group action done", "command_id", run.cmd.CommandID, "group_jid", raw, "error", err)
	}
	return result, nil
}

func pauseBetweenGroups(ctx context.Context) error {
	timer := time.NewTimer(groupGapMin + rand.N(groupGapJitter))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return errShuttingDown
	case <-timer.C:
		return nil
	}
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
