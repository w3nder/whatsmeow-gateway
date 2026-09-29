package gateway

import (
	"context"
	"fmt"
	"strings"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/channelsettings"
	"github.com/w3nder/whatsmeow-gateway/internal/registry"
)

func settingsFrom(cmd amqp.SettingsCommand) channelsettings.Settings {
	settings := channelsettings.Defaults()
	settings.Version = cmd.SettingsVersion
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

	if current := g.settings.For(cmd.ChannelID).Version; cmd.SettingsVersion < current {
		g.logger.Info("gateway: stale channel settings ignored",
			"channel_id", cmd.ChannelID,
			"command_version", cmd.SettingsVersion,
			"current_version", current)
		return nil
	}

	next := settingsFrom(cmd)
	saved, err := g.registry.SaveSettings(ctx, cmd.ChannelID, cmd.TenantID, next)
	if err != nil {
		return fmt.Errorf("gateway: persist settings %s: %w", cmd.ChannelID, err)
	}
	if saved == registry.SettingsStale {
		g.logger.Info("gateway: stale channel settings ignored, the registry holds a higher version",
			"channel_id", cmd.ChannelID,
			"command_version", cmd.SettingsVersion)
		return nil
	}
	g.settings.Set(cmd.ChannelID, next)

	g.logger.Info("gateway: channel settings applied",
		"channel_id", cmd.ChannelID,
		"listen_groups", next.ListenGroups,
		"receive_calls", next.ReceiveCalls,
		"has_call_reject_message", next.CallRejectMessage != "")
	return nil
}
