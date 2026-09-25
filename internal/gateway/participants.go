package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

func BuildGroupParticipants(ctx context.Context, resolver PhoneResolver, tenantID, channelID string, e *events.GroupInfo) []amqp.GroupParticipantsEvent {
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
			Participants: participantsOf(ctx, resolver, jids),
			EventID:      participantsEventID(e.JID, kind, e.Timestamp, jids),
			OccurredAt:   e.Timestamp.UTC().Format(time.RFC3339),
		})
	}
	add("join", e.Join)
	add(leaveKind(ctx, resolver, e), e.Leave)
	add("promoted", e.Promote)
	add("demoted", e.Demote)
	return out
}

func leaveKind(ctx context.Context, resolver PhoneResolver, e *events.GroupInfo) string {
	if e.Sender == nil {
		return "removed"
	}
	for _, jid := range e.Leave {
		if senderMatches(e, jid.User) {
			return "leave"
		}
		if jid.Server == types.HiddenUserServer {
			if pn, ok, err := resolver.PNForLID(ctx, jid); err == nil && ok && senderMatches(e, pn.User) {
				return "leave"
			}
		}
	}
	return "removed"
}

func senderMatches(e *events.GroupInfo, user string) bool {
	if e.Sender.User == user {
		return true
	}
	return e.SenderPN != nil && e.SenderPN.User == user
}

func participantsOf(ctx context.Context, resolver PhoneResolver, jids []types.JID) []amqp.GroupParticipant {
	out := make([]amqp.GroupParticipant, 0, len(jids))
	for _, jid := range jids {
		p := amqp.GroupParticipant{JID: jid.String()}
		switch jid.Server {
		case types.HiddenUserServer:
			p.LID = jid.String()
			if pn, ok, err := resolver.PNForLID(ctx, jid); err == nil && ok {
				p.Phone = pn.User
			}
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
