package gateway

import (
	"context"
	"fmt"
	"strings"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/channelsettings"
)

func settingsFrom(cmd amqp.SettingsCommand) channelsettings.Settings {
	settings := channelsettings.Defaults()
	if cmd.ListenGroups != nil {
		settings.ListenGroups = *cmd.ListenGroups
	}
	if cmd.ReceiveCalls != nil {
		settings.ReceiveCalls = *cmd.ReceiveCalls
	}
	if cmd.CallRejectMessage != nil {
		settings.CallRejectMessage = strings.TrimSpace(*cmd.CallRejectMessage)
	}
	return settings
}

func (g *gateway) SettingsHandler(ctx context.Context, cmd amqp.SettingsCommand) error {
	if cmd.ChannelID == "" || cmd.TenantID == "" {
		return fmt.Errorf("gateway: settings command without tenantId or channelId")
	}
	if known := g.tenantFor(cmd.ChannelID); known != "" && known != cmd.TenantID {
		return fmt.Errorf("gateway: settings command for channel %s comes from tenant %s, the channel belongs to another", cmd.ChannelID, cmd.TenantID)
	}

	next := settingsFrom(cmd)
	g.settings.Set(cmd.ChannelID, next)

	if err := g.registry.SaveSettings(ctx, cmd.ChannelID, cmd.TenantID, next); err != nil {
		return fmt.Errorf("gateway: persist settings %s: %w", cmd.ChannelID, err)
	}

	g.logger.Info("gateway: channel settings applied",
		"channel_id", cmd.ChannelID,
		"listen_groups", next.ListenGroups,
		"receive_calls", next.ReceiveCalls,
		"has_call_reject_message", next.CallRejectMessage != "")
	return nil
}
