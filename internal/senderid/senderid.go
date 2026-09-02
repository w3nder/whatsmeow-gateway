package senderid

import (
	"context"

	"go.mau.fi/whatsmeow/types"
)

type Resolver interface {
	PNForLID(ctx context.Context, lid types.JID) (types.JID, bool, error)
}

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

func From(lid, pn string) string {
	if pn != "" {
		return pn
	}
	return lid
}
