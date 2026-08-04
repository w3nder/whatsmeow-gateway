// Package senderid resolves a WhatsApp sender's @lid and phone-number
// identifiers the one way, for every event that carries a sender.
//
// An inbound message and an inbound call both name the person who sent them,
// but they learn that identity from different places -- a message gets a
// Sender/SenderAlt pair straight off the protocol, a call gets only the raw
// peer JID from the calling library. Without a shared resolution, the two
// event kinds would drift: exactly what happened when the call path stamped
// the raw @lid peer as From while messages stamped the resolved phone
// number, and the backend's contact lookup -- keyed on these strings -- found
// nobody for a call it otherwise had perfect information about. Both
// internal/mapper and internal/call depend on this package instead of on
// each other, so neither event-domain package owns the other's identity
// logic.
package senderid

import (
	"context"

	"go.mau.fi/whatsmeow/types"
)

// Resolver maps a @lid JID to the phone number whatsmeow's own LID store has
// recorded for it.
type Resolver interface {
	PNForLID(ctx context.Context, lid types.JID) (types.JID, bool, error)
}

// Resolve derives the bare-user @lid and phone-number strings for sender.
//
// senderAlt is whatsmeow's own alternate-identity hint for sender -- Info.
// SenderAlt on a message -- and is preferred over a resolver round trip when
// it is present. Pass the zero JID when the caller has no such hint (a
// call's peer carries none): Resolve then falls back to resolver, which may
// itself be nil. Either way, a lid with no resolvable phone number leaves pn
// empty -- the caller decides what "no known phone number" falls back to.
func Resolve(ctx context.Context, resolver Resolver, sender, senderAlt types.JID) (lid, pn string) {
	switch sender.Server {
	case types.HiddenUserServer:
		lid = sender.User
		if senderAlt.Server == types.DefaultUserServer {
			pn = senderAlt.User
		} else if resolver != nil {
			if resolved, ok, err := resolver.PNForLID(ctx, sender); err == nil && ok && resolved.Server == types.DefaultUserServer {
				pn = resolved.User
			}
		}
	case types.DefaultUserServer:
		pn = sender.User
		if senderAlt.Server == types.HiddenUserServer {
			lid = senderAlt.User
		}
	default:
		pn = sender.User
	}
	return lid, pn
}

// From picks the identifier an event's top-level "from" field carries: the
// phone number when one is known, the lid otherwise. Every sender-carrying
// event uses this so "from" means the same thing wherever the backend reads
// it.
func From(lid, pn string) string {
	if pn != "" {
		return pn
	}
	return lid
}
