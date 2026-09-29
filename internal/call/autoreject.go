package call

import (
	"strconv"

	"github.com/w3nder/whatsmeow-gateway/internal/channelsettings"
)

const InboundStateAutoRejected = "auto_rejected"

type SettingsSource interface {
	For(channelID string) channelsettings.Settings
}

func (m *Manager) settingsFor(channelID string) channelsettings.Settings {
	if m.opts.Settings == nil {
		return channelsettings.Defaults()
	}
	return m.opts.Settings.For(channelID)
}

func (m *Manager) autoReject(channelID string, lc LiveCall) bool {
	settings := m.settingsFor(channelID)
	if settings.ReceiveCalls {
		return false
	}

	if err := lc.Reject(); err != nil {
		m.log.Error("call: auto reject failed, leaving the call to the operator",
			"channel_id", channelID, "call_id", lc.ID(), "error", err)
		return false
	}

	peer := m.parsePeerJID(channelID, lc.Peer())
	senderLid, senderPn := m.resolveSenderIdentity(channelID, peer)

	m.publishInboundEvent(channelID, lc.ID(), func() InboundCallEvent {
		return NewInboundCallEvent(m.identity(channelID), channelID, lc.ID(), senderLid, senderPn,
			DirectionInbound, false, lc.IsVideo(), strconv.FormatInt(m.opts.Now().Unix(), 10), nil).
			WithState(InboundStateAutoRejected)
	})

	m.log.Info("call: incoming call auto rejected",
		"channel_id", channelID, "call_id", lc.ID(), "is_video", lc.IsVideo())

	m.autoReply(channelID, lc, settings.CallRejectMessage, senderLid, senderPn)
	return true
}
