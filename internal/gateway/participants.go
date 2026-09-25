package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

type PhoneResolver interface {
	PNForLID(ctx context.Context, lid types.JID) (types.JID, bool, error)
}

type rosterChange struct {
	ctx       context.Context
	resolver  PhoneResolver
	log       *slog.Logger
	channelID string
	event     *events.GroupInfo
	phones    map[types.JID]string
}

func BuildGroupParticipants(ctx context.Context, resolver PhoneResolver, log *slog.Logger, tenantID, channelID string, e *events.GroupInfo) []amqp.GroupParticipantsEvent {
	change := &rosterChange{ctx: ctx, resolver: resolver, log: log, channelID: channelID, event: e, phones: make(map[types.JID]string)}
	var out []amqp.GroupParticipantsEvent
	add := func(kind string, jids []types.JID) {
		if len(jids) == 0 {
			return
		}
		out = append(out, amqp.GroupParticipantsEvent{
			TenantID:     tenantID,
			ChannelID:    channelID,
			GroupJID:     e.JID.String(),
			Type:         kind,
			Participants: change.participants(jids),
			EventID:      participantsEventID(e.JID, kind, e.Timestamp, jids),
			OccurredAt:   e.Timestamp.UTC().Format(time.RFC3339),
		})
	}
	left, removed := change.splitLeave()
	add("join", e.Join)
	add("leave", left)
	add("removed", removed)
	add("promoted", e.Promote)
	add("demoted", e.Demote)
	return out
}

func (c *rosterChange) splitLeave() (left, removed []types.JID) {
	for _, jid := range c.event.Leave {
		if c.leftByThemselves(jid) {
			left = append(left, jid)
		} else {
			removed = append(removed, jid)
		}
	}
	return left, removed
}

func (c *rosterChange) leftByThemselves(jid types.JID) bool {
	if c.event.Sender == nil {
		return false
	}
	if c.senderMatches(jid.User) {
		return true
	}
	if jid.Server != types.HiddenUserServer {
		return false
	}
	phone := c.phoneOf(jid)
	return phone != "" && c.senderMatches(phone)
}

func (c *rosterChange) senderMatches(user string) bool {
	if c.event.Sender.User == user {
		return true
	}
	return c.event.SenderPN != nil && c.event.SenderPN.User == user
}

func (c *rosterChange) phoneOf(lid types.JID) string {
	if phone, done := c.phones[lid]; done {
		return phone
	}
	pn, ok, err := c.resolver.PNForLID(c.ctx, lid)
	switch {
	case err != nil:
		c.log.Warn("gateway: resolve phone of a lid group participant", "channel_id", c.channelID, "group_jid", c.event.JID.String(), "lid", lid.String(), "error", err)
	case !ok:
		c.log.Debug("gateway: lid group participant has no known phone", "channel_id", c.channelID, "group_jid", c.event.JID.String(), "lid", lid.String())
	}
	phone := ""
	if err == nil && ok {
		phone = pn.User
	}
	c.phones[lid] = phone
	return phone
}

func (c *rosterChange) participants(jids []types.JID) []amqp.GroupParticipant {
	out := make([]amqp.GroupParticipant, 0, len(jids))
	for _, jid := range jids {
		p := amqp.GroupParticipant{JID: jid.String()}
		switch jid.Server {
		case types.HiddenUserServer:
			p.LID = jid.String()
			p.Phone = c.phoneOf(jid)
		case types.DefaultUserServer:
			p.Phone = jid.User
		}
		out = append(out, p)
	}
	return out
}

func participantsEventID(group types.JID, kind string, at time.Time, jids []types.JID) string {
	users := make([]string, 0, len(jids))
	for _, jid := range jids {
		users = append(users, jid.String())
	}
	slices.Sort(users)
	sum := sha256.Sum256([]byte(group.String() + "|" + kind + "|" + strconv.FormatInt(at.Unix(), 10) + "|" + strings.Join(users, ",")))
	return hex.EncodeToString(sum[:16])
}
