package mapper

import (
	"context"
	"encoding/hex"
	"log/slog"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
)

func decryptEnvelopes(ctx context.Context, deps InboundDeps, evt *events.Message, msg *waE2E.Message) (*waE2E.Message, error) {
	if deps.Secrets == nil {
		return msg, nil
	}
	if sec := msg.GetSecretEncryptedMessage(); sec != nil {
		return editFromSecret(ctx, deps, evt, sec)
	}
	if poll := msg.GetPollUpdateMessage(); poll != nil {
		logPollVote(ctx, deps, evt, poll)
		return nil, ErrSkip
	}
	return msg, nil
}

func logPollVote(ctx context.Context, deps InboundDeps, evt *events.Message, poll *waE2E.PollUpdateMessage) {
	if !rawDebugEnabled() {
		return
	}

	vote, err := deps.Secrets.DecryptPollVote(ctx, evt)
	if err != nil || vote == nil {
		slog.Default().Error("mapper: decrypt poll vote", "providerMessageId", evt.Info.ID, "error", err)
		return
	}

	selected := make([]string, 0, len(vote.GetSelectedOptions()))
	for _, option := range vote.GetSelectedOptions() {
		selected = append(selected, hex.EncodeToString(option))
	}

	key := poll.GetPollCreationMessageKey()
	slog.Default().Info("mapper: poll vote dump",
		"providerMessageId", evt.Info.ID,
		"pollMessageId", key.GetID(),
		"pollRemoteJid", key.GetRemoteJID(),
		"pollFromMe", key.GetFromMe(),
		"voterJid", evt.Info.Sender.String(),
		"voterJidNonAD", evt.Info.Sender.ToNonAD().String(),
		"voterFromMe", evt.Info.IsFromMe,
		"pushName", evt.Info.PushName,
		"selectedOptionHashes", selected)
}

func editFromSecret(ctx context.Context, deps InboundDeps, evt *events.Message, sec *waE2E.SecretEncryptedMessage) (*waE2E.Message, error) {
	if sec.GetSecretEncType() != waE2E.SecretEncryptedMessage_MESSAGE_EDIT {
		slog.Default().Info("mapper: ignoring an unsupported secret enc type",
			"providerMessageId", evt.Info.ID, "secretEncType", sec.GetSecretEncType().String())
		return nil, ErrSkip
	}

	targetKey := sec.GetTargetMessageKey()
	if targetKey.GetID() == "" {
		slog.Default().Error("mapper: secret encrypted edit without a target message key", "providerMessageId", evt.Info.ID)
		return nil, ErrSkip
	}

	decrypted, err := deps.Secrets.DecryptSecretEncryptedMessage(ctx, evt)
	if err != nil || decrypted == nil {
		slog.Default().Error("mapper: decrypt secret encrypted edit", "providerMessageId", evt.Info.ID, "error", err)
		return nil, ErrSkip
	}

	edited := decrypted
	if pm := decrypted.GetProtocolMessage(); pm != nil &&
		pm.GetType() == waE2E.ProtocolMessage_MESSAGE_EDIT && pm.GetEditedMessage() != nil {
		edited = pm.GetEditedMessage()
	}

	return &waE2E.Message{
		ProtocolMessage: &waE2E.ProtocolMessage{
			Key:           targetKey,
			Type:          waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(),
			EditedMessage: edited,
		},
	}, nil
}
