package gateway

import (
	"context"
	"fmt"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

func historyNotification(evt *events.Message) *waE2E.HistorySyncNotification {
	if !evt.Info.IsFromMe {
		return nil
	}
	return evt.Message.GetProtocolMessage().GetHistorySyncNotification()
}

func (g *gateway) acceptHistory(channelID string, notif *waE2E.HistorySyncNotification) bool {
	client, err := g.waClientFor(channelID)
	if err != nil || !client.TakesOverHistory() {
		return false
	}
	g.importer.Accept(channelID, client, notif)
	return true
}

const (
	pairingBusyBehindHistory       = "Outro pareamento com histórico está em andamento neste servidor. Tente novamente em instantes."
	historyPairingBusyBehindOthers = "Há outros pareamentos em andamento neste servidor e o pareamento com histórico precisa acontecer sozinho. Tente novamente em instantes."
)

func pairingBusyReason(importHistory bool) string {
	if importHistory {
		return historyPairingBusyBehindOthers
	}
	return pairingBusyBehindHistory
}

func (g *gateway) recordHistoryChoice(ctx context.Context, cmd amqp.PairCommand) error {
	if !cmd.ImportsHistory() {
		if err := g.registry.ClearHistoryImport(ctx, cmd.ChannelID); err != nil {
			return fmt.Errorf("gateway: clear the history import of %s: %w", cmd.ChannelID, err)
		}
		return nil
	}
	if err := g.registry.BeginHistoryImport(ctx, cmd.ChannelID, cmd.TenantID, cmd.ImportID); err != nil {
		return fmt.Errorf("gateway: begin history import %s: %w", cmd.ChannelID, err)
	}
	return nil
}

func (g *gateway) endEmptyImport(ctx context.Context, cmd amqp.PairCommand) error {
	if err := g.publisher.PublishHistoryDone(ctx, amqp.HistoryDone{TenantID: cmd.TenantID, ChannelID: cmd.ChannelID, ImportID: cmd.ImportID}); err != nil {
		return fmt.Errorf("gateway: publish the empty history import end of %s: %w", cmd.ChannelID, err)
	}
	return g.registry.FinishHistoryImport(ctx, cmd.ChannelID, cmd.ImportID)
}
