package store

import (
	"context"

	_ "github.com/jackc/pgx/v5/stdlib"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func Open(ctx context.Context, dsn string, log waLog.Logger) (*sqlstore.Container, error) {
	return sqlstore.New(ctx, "pgx", dsn, log)
}

func DeviceFor(ctx context.Context, c *sqlstore.Container, jid *types.JID) (*store.Device, error) {
	if jid == nil {
		return c.NewDevice(), nil
	}
	return c.GetDevice(ctx, *jid)
}
