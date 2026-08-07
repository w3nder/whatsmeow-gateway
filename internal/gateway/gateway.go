package gateway

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/avatar"
	"github.com/w3nder/whatsmeow-gateway/internal/call"
	"github.com/w3nder/whatsmeow-gateway/internal/dedupe"
	"github.com/w3nder/whatsmeow-gateway/internal/groupinfo"
	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
	"github.com/w3nder/whatsmeow-gateway/internal/ownership"
	"github.com/w3nder/whatsmeow-gateway/internal/registry"
	"github.com/w3nder/whatsmeow-gateway/internal/senderid"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
	"github.com/w3nder/whatsmeow-gateway/internal/store"
)

type Deps struct {
	Consumer             *amqp.Consumer
	Publisher            *amqp.Publisher
	Manager              *session.Manager
	Ownership            *ownership.Store
	Dedupe               *dedupe.Store
	Registry             *registry.Store
	MediaStore           mapper.MediaStore
	InstanceID           string
	ShardLockTTL         time.Duration
	SendTimeout          time.Duration
	ShutdownDrainTimeout time.Duration
	// CallOptions configures call tracking. Run builds the call manager itself:
	// the identity it stamps on every event comes from the channel's tenant and
	// the channel id itself, neither of which main can reach.
	CallOptions call.Options
	// OnCallManager, if set, is handed the call manager as soon as it exists --
	// before Run blocks in its own serving loop. This is the only way anything
	// outside this package (namely main's media websocket, which must attach
	// streams on the very registry the dispatcher populates) ever sees the
	// manager: Run doesn't return one, since it doesn't return at all until
	// shutdown.
	OnCallManager func(*call.Manager)
	Logger        *slog.Logger
}

type gateway struct {
	consumer             *amqp.Consumer
	publisher            *amqp.Publisher
	manager              *session.Manager
	ownership            *ownership.Store
	dedupe               *dedupe.Store
	registry             *registry.Store
	mediaStore           mapper.MediaStore
	avatars              *avatar.Cache
	groups               *groupinfo.Cache
	calls                *call.Manager
	instanceID           string
	shardLockTTL         time.Duration
	sendTimeout          time.Duration
	shutdownDrainTimeout time.Duration
	logger               *slog.Logger

	workCtx context.Context

	tenantMu        sync.RWMutex
	tenantByChannel map[string]string
}

func Run(ctx context.Context, deps Deps) error {
	g := &gateway{
		consumer:             deps.Consumer,
		publisher:            deps.Publisher,
		manager:              deps.Manager,
		ownership:            deps.Ownership,
		dedupe:               deps.Dedupe,
		registry:             deps.Registry,
		mediaStore:           deps.MediaStore,
		avatars:              avatar.New(deps.MediaStore, fetchMediaURL, avatar.Options{}, deps.Logger),
		groups:               groupinfo.New(groupinfo.Options{}, deps.Logger),
		instanceID:           deps.InstanceID,
		shardLockTTL:         deps.ShardLockTTL,
		sendTimeout:          sendTimeout(deps.SendTimeout),
		shutdownDrainTimeout: deps.ShutdownDrainTimeout,
		logger:               deps.Logger,
		workCtx:              context.WithoutCancel(ctx),
		tenantByChannel:      make(map[string]string),
	}

	recordingStore, ok := deps.MediaStore.(call.RecordingStore)
	if !ok {
		return fmt.Errorf("gateway: media store does not support streaming uploads, which call recording requires")
	}

	g.calls = call.NewManager(
		callPublisher{deps.Publisher},
		recordingStore,
		g.callIdentity,
		g.callSenderResolver,
		g.callAvatars,
		deps.CallOptions,
		deps.Logger,
	)

	if deps.OnCallManager != nil {
		deps.OnCallManager(g.calls)
	}

	return g.run(ctx)
}

// callPublisher adapts the amqp publisher to call.Publisher. The amqp package
// takes `any` for every event, as it does for inbound and status, so it stays
// unaware of the call package.
type callPublisher struct {
	publisher *amqp.Publisher
}

func (c callPublisher) PublishCall(ctx context.Context, evt call.Event) error {
	return c.publisher.PublishCall(ctx, evt)
}

func (c callPublisher) PublishInbound(ctx context.Context, evt call.InboundCallEvent) error {
	return c.publisher.PublishInbound(ctx, evt)
}

// callIdentity resolves the tenant and phone-number id a call event belongs
// to. PhoneNumberID is the channel id, not the device's JID: the backend
// resolves a channel by its phoneNumberId column, and for a gateway channel
// that column holds the channel's own UUID, not a phone number. The inbound
// message path (mapper.BuildInbound) stamps the same value for the same
// reason -- match it here rather than the device JID, or channel lookups on
// the backend silently fail to find every call event.
func (g *gateway) callIdentity(channelID string) call.Identity {
	return call.Identity{TenantID: g.tenantFor(channelID), PhoneNumberID: channelID}
}

// callSenderResolver hands the call manager the same PNForLID lookup
// mapper.BuildInbound uses for a message's sender, reached through the
// channel's own client, so a call's @lid peer resolves to the identical
// phone number a message from that person would. A channel with no live
// client -- not yet paired, or gone -- resolves nothing: a call already in
// flight must not be failed over an identity lookup.
func (g *gateway) callSenderResolver(channelID string) senderid.Resolver {
	client, err := g.waClientFor(channelID)
	if err != nil {
		return nil
	}
	return client
}

// channelAvatars binds the shared photo cache to one channel: its own client
// does the asking, since WhatsApp evaluates photo privacy against the account
// that asks, and its tenant decides where the photo is stored. A channel with
// no live client resolves nothing, exactly as callSenderResolver does -- a
// call or a message must never be lost over a profile photo.
//
// It satisfies both mapper.AvatarSource and call.AvatarSource, which is the
// point: a message and a call from the same person name the same photo.
func (g *gateway) channelAvatars(channelID string) *channelAvatarSource {
	// A channel with no live client leaves lookup nil, which the cache reads
	// as "cannot ask" and answers with no photo.
	var lookup avatar.Lookup
	if client, err := g.waClientFor(channelID); err == nil {
		lookup = client
	}
	return &channelAvatarSource{
		cache:     g.avatars,
		lookup:    lookup,
		channelID: channelID,
		tenantID:  g.tenantFor(channelID),
	}
}

// callAvatars is channelAvatars as the call manager's own interface. The two
// packages each declare the interface they consume, so the gateway is where
// the one implementation is named as both.
func (g *gateway) callAvatars(channelID string) call.AvatarSource {
	return g.channelAvatars(channelID)
}

type channelAvatarSource struct {
	cache     *avatar.Cache
	lookup    avatar.Lookup
	channelID string
	tenantID  string
}

func (s *channelAvatarSource) For(ctx context.Context, jid types.JID) *avatar.Picture {
	if s == nil {
		return nil
	}
	return s.cache.For(ctx, s.lookup, s.channelID, s.tenantID, jid)
}

func (g *gateway) channelGroups(channelID string) *channelGroupSource {
	var lookup groupinfo.Lookup
	if client, err := g.waClientFor(channelID); err == nil {
		lookup = client
	}
	return &channelGroupSource{
		cache:     g.groups,
		lookup:    lookup,
		channelID: channelID,
	}
}

type channelGroupSource struct {
	cache     *groupinfo.Cache
	lookup    groupinfo.Lookup
	channelID string
}

func (s *channelGroupSource) Name(ctx context.Context, jid types.JID) string {
	if s == nil {
		return ""
	}
	return s.cache.Name(ctx, s.lookup, s.channelID, jid)
}

const defaultSendTimeout = 30 * time.Second

func sendTimeout(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultSendTimeout
	}
	return d
}

func (g *gateway) run(ctx context.Context) error {
	if err := g.ownership.ClaimAll(ctx, g.instanceID, g.shardLockTTL); err != nil {
		return fmt.Errorf("gateway: claim shards: %w", err)
	}

	g.manager.OnEvent(g.handleSessionEvent)

	g.resumeOwnedSessions(ctx)

	if err := g.consumer.StartPair(ctx, g.PairHandler); err != nil {
		g.closeConsumerForFailedBoot()
		_ = g.ownership.ReleaseAll(g.workCtx, g.instanceID)
		return fmt.Errorf("gateway: start pair consumer: %w", err)
	}
	if err := g.consumer.StartSend(g.workCtx, g.SendHandler); err != nil {
		g.closeConsumerForFailedBoot()
		_ = g.ownership.ReleaseAll(g.workCtx, g.instanceID)
		return fmt.Errorf("gateway: start send consumer: %w", err)
	}
	if err := g.consumer.StartCall(g.workCtx, g.CallHandler); err != nil {
		g.closeConsumerForFailedBoot()
		_ = g.ownership.ReleaseAll(g.workCtx, g.instanceID)
		return fmt.Errorf("gateway: start call consumer: %w", err)
	}

	g.logger.Info("gateway started", "instance_id", g.instanceID)

	// A dead broker leaves the consumers stopped and the publisher failing every event.
	// Rather than idle on forever in that state, shut down and report it: main exits
	// non-zero and the orchestrator restarts us onto a fresh connection.
	var fatal error
	select {
	case <-ctx.Done():
		g.logger.Info("gateway stopping")
	case consumerErr := <-g.consumer.Failed():
		fatal = fmt.Errorf("gateway: amqp consumer died: %w", consumerErr)
		g.logger.Error("gateway: amqp consumer died, shutting down for restart", "error", consumerErr)
	}

	g.closeConsumerWithDrainDeadline()

	// End live calls before the sockets go: a call left registered would never
	// report its end to the backend. AbortAll only starts each recording's
	// upload -- it runs off the call's teardown path -- so WaitForRecordings
	// gives it a bounded window to finish rather than letting the process
	// exit and kill an upload that was nearly done.
	g.calls.AbortAll(g.workCtx, "gateway_shutdown")
	g.calls.WaitForRecordings(g.shutdownDrainTimeout)

	g.manager.DisconnectAll()

	if err := g.ownership.ReleaseAll(g.workCtx, g.instanceID); err != nil {
		g.logger.Error("gateway: release shards", "error", err)
		return fmt.Errorf("gateway: release shards: %w", err)
	}

	return fatal
}

func (g *gateway) resumeOwnedSessions(ctx context.Context) {
	shardCount := g.ownership.ShardCount()
	ownedShards := make([]int, shardCount)
	for i := range ownedShards {
		ownedShards[i] = i
	}

	sessions, err := g.registry.ForShards(ctx, ownedShards, func(channelID string) int {
		return ownership.Shard(channelID, shardCount)
	})
	if err != nil {
		g.logger.Error("gateway: list sessions to resume", "error", err)
		return
	}

	for _, cs := range sessions {
		jid, err := types.ParseJID(cs.JID)
		if err != nil {
			g.logger.Error("gateway: parse stored jid for resume", "channel_id", cs.ChannelID, "error", err)
			continue
		}

		g.setTenant(cs.ChannelID, cs.TenantID)

		if err := g.manager.Resume(ctx, cs.ChannelID, jid); err != nil {
			g.logger.Error("gateway: resume session", "channel_id", cs.ChannelID, "error", err)
			continue
		}

		g.attachCalls(cs.ChannelID)
	}
}

func (g *gateway) closeConsumerForFailedBoot() {
	if err := g.consumer.Close(); err != nil {
		g.logger.Error("gateway: close consumer after failed boot", "error", err)
	}
}

func (g *gateway) closeConsumerWithDrainDeadline() {
	done := make(chan error, 1)
	go func() {
		done <- g.consumer.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			g.logger.Error("gateway: close consumer", "error", err)
		}
	case <-time.After(g.shutdownDrainTimeout):
		g.logger.Error("gateway: consumer drain deadline exceeded, proceeding with shutdown", "timeout", g.shutdownDrainTimeout)
	}
}

func (g *gateway) PairHandler(ctx context.Context, cmd amqp.PairCommand) error {
	g.setTenant(cmd.ChannelID, cmd.TenantID)

	updates, err := g.manager.Pair(ctx, cmd.ChannelID)
	if err != nil {
		g.publishChannelError(ctx, cmd.TenantID, cmd.UserID, cmd.ChannelID, err)
		return fmt.Errorf("gateway: pair channel %s: %w", cmd.ChannelID, err)
	}

	for update := range updates {
		switch {
		case update.Err != nil:
			g.publishChannelError(ctx, cmd.TenantID, cmd.UserID, cmd.ChannelID, update.Err)
			return fmt.Errorf("gateway: pair channel %s: %w", cmd.ChannelID, update.Err)
		case update.Connected:
			if err := g.persistSession(ctx, cmd.ChannelID, cmd.TenantID); err != nil {
				return fmt.Errorf("gateway: persist session %s: %w", cmd.ChannelID, err)
			}
			g.attachCalls(cmd.ChannelID)
			if err := g.publisher.PublishChannelStatus(ctx, amqp.ChannelStatusEvent{
				TenantID:  cmd.TenantID,
				UserID:    cmd.UserID,
				ChannelID: cmd.ChannelID,
				Status:    "connected",
			}); err != nil {
				return fmt.Errorf("gateway: publish channel.status connected: %w", err)
			}
		case update.QR != "":
			if err := g.publisher.PublishChannelQR(ctx, amqp.ChannelQREvent{
				TenantID:  cmd.TenantID,
				UserID:    cmd.UserID,
				ChannelID: cmd.ChannelID,
				QR:        update.QR,
			}); err != nil {
				return fmt.Errorf("gateway: publish channel.qr: %w", err)
			}
		}
	}

	return nil
}

func (g *gateway) persistSession(ctx context.Context, channelID, tenantID string) error {
	client, err := g.waClientFor(channelID)
	if err != nil {
		return fmt.Errorf("resolve client: %w", err)
	}

	jid := client.DeviceJID()
	if jid == nil {
		err := fmt.Errorf("device jid is nil after pair success")
		g.logger.Error("gateway: persist session", "channel_id", channelID, "error", err)
		return err
	}

	if err := g.registry.Save(ctx, channelID, jid.String(), tenantID); err != nil {
		return err
	}
	return nil
}

func (g *gateway) publishChannelError(ctx context.Context, tenantID, userID, channelID string, cause error) {
	if err := g.publisher.PublishChannelStatus(ctx, amqp.ChannelStatusEvent{
		TenantID:  tenantID,
		UserID:    userID,
		ChannelID: channelID,
		Status:    "error",
		Reason:    cause.Error(),
	}); err != nil {
		g.logger.Error("gateway: publish channel.status error", "channel_id", channelID, "error", err)
	}
}

// CallHandler runs one call command.
//
// It reports a bad command as an event rather than as an error: nacking would
// requeue it, and a command for a call that does not exist would loop until the
// DLQ. Only a failure worth retrying comes back as an error.
func (g *gateway) CallHandler(ctx context.Context, cmd amqp.GatewayCallCommand) error {
	g.setTenant(cmd.ChannelID, cmd.TenantID)

	g.logger.Info("gateway: call command received",
		"command_id", cmd.CommandID,
		"channel_id", cmd.ChannelID,
		"call_id", cmd.CallID,
		"action", cmd.Action)

	if err := g.ensureChannelConnected(ctx, cmd.ChannelID); err != nil {
		return fmt.Errorf("gateway: ensure connected %s: %w", cmd.ChannelID, err)
	}

	client, err := g.waClientFor(cmd.ChannelID)
	if err != nil {
		return fmt.Errorf("gateway: resolve client %s: %w", cmd.ChannelID, err)
	}

	return g.calls.Dispatch(ctx, client.Calls(), cmd, fetchMediaURL)
}

// attachCalls subscribes the call manager to a channel's inbound calls. Called
// once a session is live, from both the pair and the resume paths.
func (g *gateway) attachCalls(channelID string) {
	client, err := g.waClientFor(channelID)
	if err != nil {
		g.logger.Error("gateway: resolve client to attach calls", "channel_id", channelID, "error", err)
		return
	}

	caller := client.Calls()
	if caller == nil {
		g.logger.Warn("gateway: channel has no calling client, calls disabled", "channel_id", channelID)
		return
	}
	g.calls.Attach(channelID, caller)
	// The failure path above already logs; without this, a successful attach is
	// silent and indistinguishable from attachCalls never having run at all.
	g.logger.Info("gateway: calling client attached", "channel_id", channelID)
}

func (g *gateway) SendHandler(ctx context.Context, cmd amqp.GatewaySendCommand) error {
	g.setTenant(cmd.ChannelID, cmd.TenantID)

	g.logger.Info("gateway: send command received",
		"message_id", cmd.MessageID,
		"channel_id", cmd.ChannelID,
		"to", cmd.To,
		"type", cmd.Type,
		"kind", cmd.Kind,
		"target_provider_message_id", cmd.TargetProviderMessageID)

	dedupeKey := dedupeKeyFor(cmd)
	providerID := dedupe.DeterministicProviderID(dedupeKey)

	alreadySent, existingProviderID, err := g.dedupe.Begin(ctx, dedupeKey, providerID)
	if err != nil {
		return fmt.Errorf("gateway: dedupe begin %s: %w", dedupeKey, err)
	}
	if alreadySent {
		g.logger.Info("gateway: command already sent, replaying sent status (skipped)",
			"message_id", cmd.MessageID,
			"kind", cmd.Kind,
			"existing_provider_message_id", existingProviderID)
		if err := g.publisher.PublishStatus(ctx, mapper.StatusEvent{
			ProviderMessageID: existingProviderID,
			OpaqueMessageID:   cmd.MessageID,
			Status:            "sent",
			Timestamp:         strconv.FormatInt(time.Now().Unix(), 10),
		}); err != nil {
			return fmt.Errorf("gateway: publish sent status (dedup replay) %s: %w", cmd.MessageID, err)
		}
		return nil
	}

	sendCtx, cancel := context.WithTimeout(ctx, g.sendTimeout)
	defer cancel()

	if err := g.ensureChannelConnected(ctx, cmd.ChannelID); err != nil {
		return g.publishSendFailure(ctx, cmd, providerID, fmt.Errorf("gateway: ensure connected %s: %w", cmd.ChannelID, err))
	}

	client, err := g.waClientFor(cmd.ChannelID)
	if err != nil {
		return fmt.Errorf("gateway: resolve client %s: %w", cmd.ChannelID, err)
	}

	to, msg, err := mapper.BuildOutbound(sendCtx, client, cmd, fetchMediaURL)
	if err != nil {
		return g.publishSendFailure(ctx, cmd, providerID, fmt.Errorf("gateway: build outbound %s: %w", cmd.MessageID, err))
	}

	id, ts, err := g.manager.Send(sendCtx, cmd.ChannelID, to, msg, providerID)
	if err != nil {
		return g.publishSendFailure(ctx, cmd, providerID, fmt.Errorf("gateway: send %s: %w", cmd.MessageID, err))
	}

	g.logger.Info("gateway: message sent to whatsapp", "message_id", cmd.MessageID, "provider_message_id", id, "channel_id", cmd.ChannelID)

	if err := g.dedupe.MarkSent(ctx, dedupeKey); err != nil {
		return fmt.Errorf("gateway: mark sent %s: %w", dedupeKey, err)
	}

	if err := g.publisher.PublishStatus(ctx, mapper.StatusEvent{
		ProviderMessageID: id,
		OpaqueMessageID:   cmd.MessageID,
		Status:            "sent",
		Timestamp:         strconv.FormatInt(ts.Unix(), 10),
	}); err != nil {
		return fmt.Errorf("gateway: publish sent status %s: %w", cmd.MessageID, err)
	}

	return nil
}

func dedupeKeyFor(cmd amqp.GatewaySendCommand) string {
	switch cmd.Kind {
	case "revoke":
		return fmt.Sprintf("%s:revoke:%s", cmd.MessageID, cmd.TargetProviderMessageID)
	case "edit":
		h := fnv.New32a()
		_, _ = h.Write([]byte(cmd.Text))
		return fmt.Sprintf("%s:edit:%s:%08x", cmd.MessageID, cmd.TargetProviderMessageID, h.Sum32())
	case "reaction":
		h := fnv.New32a()
		_, _ = h.Write([]byte(cmd.Emoji))
		return fmt.Sprintf("%s:reaction:%s:%08x", cmd.MessageID, cmd.TargetProviderMessageID, h.Sum32())
	default:
		return cmd.MessageID
	}
}

func (g *gateway) publishSendFailure(ctx context.Context, cmd amqp.GatewaySendCommand, providerID string, cause error) error {
	g.logger.Error("gateway: send failed", "message_id", cmd.MessageID, "channel_id", cmd.ChannelID, "error", cause)

	if err := g.publisher.PublishStatus(ctx, mapper.StatusEvent{
		ProviderMessageID: providerID,
		OpaqueMessageID:   cmd.MessageID,
		Status:            "failed",
		Timestamp:         strconv.FormatInt(time.Now().Unix(), 10),
		Error: &mapper.StatusError{
			Code:   "gateway_send_error",
			Reason: cause.Error(),
		},
	}); err != nil {
		return fmt.Errorf("gateway: publish failed status %s: %w", cmd.MessageID, err)
	}
	return nil
}

func (g *gateway) waClientFor(channelID string) (session.WAClient, error) {
	return g.manager.Client(channelID)
}

// ensureChannelConnected gets a channel's socket up before a send. When the channel has
// no live session on this instance -- boot resume failed while the network was down, or
// the channel moved between instances -- it is resumed from its stored JID. Falling back
// to a brand-new device would leave the channel unpaired and waiting for a QR scan.
func (g *gateway) ensureChannelConnected(ctx context.Context, channelID string) error {
	err := g.manager.EnsureConnected(channelID)
	if !errors.Is(err, session.ErrNoSession) {
		return err
	}

	cs, found, lookupErr := g.registry.Get(ctx, channelID)
	if lookupErr != nil {
		return fmt.Errorf("gateway: lookup stored session %s: %w", channelID, lookupErr)
	}
	if !found {
		return fmt.Errorf("gateway: channel %s is not paired: %w", channelID, err)
	}

	jid, parseErr := types.ParseJID(cs.JID)
	if parseErr != nil {
		return fmt.Errorf("gateway: parse stored jid for %s: %w", channelID, parseErr)
	}

	g.logger.Info("gateway: resuming channel on demand", "channel_id", channelID, "jid", cs.JID)
	g.setTenant(channelID, cs.TenantID)

	if resumeErr := g.manager.Resume(ctx, channelID, jid); resumeErr != nil {
		return resumeErr
	}
	g.attachCalls(channelID)
	return g.manager.EnsureConnected(channelID)
}

func (g *gateway) handleSessionEvent(channelID string, evt any) {
	// Every event that really kills a channel's calls goes through one place,
	// so the difference between "the socket blipped" and "this session is
	// gone" is decided once rather than per case -- see callsAreDead.
	if reason, dead := callsAreDead(evt); dead {
		g.calls.AbortChannel(g.workCtx, channelID, reason)
	}

	switch e := evt.(type) {
	case *events.Message:
		g.handleInboundMessage(channelID, e)
	case *events.Receipt:
		g.handleReceipt(channelID, e)
	case *events.GroupInfo:
		if e.Name != nil {
			g.groups.Invalidate(channelID, e.JID)
		}
	case *events.LoggedOut:
		g.clearTenant(channelID)
		if err := g.registry.Delete(g.workCtx, channelID); err != nil {
			g.logger.Error("gateway: delete session on logout", "channel_id", channelID, "error", err)
		}
	case *events.CallOffer, *events.CallAccept, *events.CallPreAccept, *events.CallTerminate,
		*events.CallReject, *events.CallOfferNotice, *events.UnknownCallEvent:
		g.handleCallSignal(channelID, e)
	default:
		g.handleConnectionEvent(channelID, evt)
	}
}

// handleCallSignal logs whatsmeow's raw call signalling so the gateway is never blind to
// a call arriving, even when the calling library (meowcaller, subscribed to these same
// events independently) ignores or mishandles one. This is observability only: nothing
// here acts on a call, so it cannot change call behaviour.
func (g *gateway) handleCallSignal(channelID string, evt any) {
	switch e := evt.(type) {
	case *events.CallOffer:
		g.logger.Info("gateway: call offer received", "channel_id", channelID, "call_id", e.CallID, "from", e.From.String())
	case *events.CallAccept:
		g.logger.Info("gateway: call accept received", "channel_id", channelID, "call_id", e.CallID, "from", e.From.String())
	case *events.CallPreAccept:
		g.logger.Info("gateway: call pre-accept received", "channel_id", channelID, "call_id", e.CallID, "from", e.From.String())
	case *events.CallTerminate:
		g.logger.Info("gateway: call terminate received", "channel_id", channelID, "call_id", e.CallID, "from", e.From.String(), "reason", e.Reason)
	case *events.CallReject:
		g.logger.Info("gateway: call reject received", "channel_id", channelID, "call_id", e.CallID, "from", e.From.String())
	case *events.CallOfferNotice:
		g.logger.Info("gateway: call offer notice received", "channel_id", channelID, "call_id", e.CallID, "from", e.From.String(), "media", e.Media, "type", e.Type)
	case *events.UnknownCallEvent:
		g.logger.Info("gateway: unknown call event received", "channel_id", channelID)
	}
}

// callsAreDead reports whether a session event means the channel's live calls are
// genuinely over, and the reason to report them ending with. Ending a call publishes
// its `ended`, closes the operator's stream and finishes its recording, so getting this
// wrong in either direction is expensive.
//
// events.Disconnected is deliberately absent, and it is the whole reason this decision
// is written down rather than inlined. whatsmeow raises it on every socket drop -- a
// missed keepalive, a broker-side reset, a two-second blip -- and then reconnects on its
// own. A call's media does not ride that socket: it is a separate SRTP relay connection
// the calling library holds, which nothing tears down when the websocket flaps. Treating
// Disconnected as fatal truncated the recording and the CDR of calls that were still
// running, and left the live call behind with no registry entry for a hangup to reach.
//
// The four below are the ones whatsmeow does not come back from: a logout, a session
// taken over by another connection, a refused build, and a connect failure it gives up
// on rather than retrying.
func callsAreDead(evt any) (reason string, dead bool) {
	switch evt.(type) {
	case *events.LoggedOut:
		return "logged_out", true
	case *events.StreamReplaced:
		return "stream_replaced", true
	case *events.ClientOutdated:
		return "client_outdated", true
	case *events.ConnectFailure:
		return "connect_failure", true
	default:
		return "", false
	}
}

// handleConnectionEvent reports socket health. Everything here that is recoverable is
// already being retried by whatsmeow's auto-reconnect, so the gateway only observes and
// publishes status; the cases whatsmeow deliberately gives up on are published as errors
// because they need action outside the gateway (re-pair, unban, upgrade). Whether an
// event also ends the channel's live calls is decided by callsAreDead, before this runs.
func (g *gateway) handleConnectionEvent(channelID string, evt any) {
	switch e := evt.(type) {
	case *events.Connected:
		g.logger.Info("gateway: channel socket connected", "channel_id", channelID)
		g.publishChannelStatus(channelID, "connected", "")
	case *events.Disconnected:
		g.logger.Warn("gateway: channel socket disconnected, auto-reconnect running", "channel_id", channelID)
		g.publishChannelStatus(channelID, "disconnected", "socket disconnected")
	case *events.KeepAliveTimeout:
		g.logger.Warn("gateway: channel keepalive timed out",
			"channel_id", channelID,
			"error_count", e.ErrorCount,
			"last_success", e.LastSuccess)
	case *events.KeepAliveRestored:
		g.logger.Info("gateway: channel keepalive restored", "channel_id", channelID)
	case *events.StreamReplaced:
		g.logger.Error("gateway: channel session replaced by another connection", "channel_id", channelID)
		g.publishChannelStatus(channelID, "error", "session replaced by another connection")
	case *events.TemporaryBan:
		g.logger.Error("gateway: channel temporarily banned", "channel_id", channelID, "code", e.Code.String(), "expire", e.Expire)
		g.publishChannelStatus(channelID, "error", fmt.Sprintf("temporarily banned (%s), expires in %s", e.Code.String(), e.Expire))
	case *events.ClientOutdated:
		g.logger.Error("gateway: whatsmeow client outdated, whatsapp refused the connection", "channel_id", channelID)
		g.publishChannelStatus(channelID, "error", "whatsapp client version outdated")
	case *events.ConnectFailure:
		g.logger.Error("gateway: channel connect failure", "channel_id", channelID, "reason", e.Reason.String(), "message", e.Message)
		g.publishChannelStatus(channelID, "error", fmt.Sprintf("connect failure: %s %s", e.Reason.String(), e.Message))
	case *events.StreamError:
		g.logger.Error("gateway: channel stream error", "channel_id", channelID, "code", e.Code)
		g.publishChannelStatus(channelID, "error", fmt.Sprintf("stream error %s", e.Code))
	}
}

func (g *gateway) publishChannelStatus(channelID, status, reason string) {
	if err := g.publisher.PublishChannelStatus(g.workCtx, amqp.ChannelStatusEvent{
		TenantID:  g.tenantFor(channelID),
		ChannelID: channelID,
		Status:    status,
		Reason:    reason,
	}); err != nil {
		g.logger.Error("gateway: publish channel.status", "channel_id", channelID, "status", status, "error", err)
	}
}

func (g *gateway) handleInboundMessage(channelID string, evt *events.Message) {
	g.logger.Info("gateway: inbound event received",
		"channel_id", channelID,
		"message_id", evt.Info.ID,
		"from", evt.Info.Sender.String(),
		"push_name", evt.Info.PushName,
		"from_me", evt.Info.IsFromMe)

	client, err := g.waClientFor(channelID)
	if err != nil {
		g.logger.Error("gateway: resolve client for inbound event", "channel_id", channelID, "message_id", evt.Info.ID, "error", err)
		return
	}

	inbound, err := mapper.BuildInbound(g.workCtx, mapper.InboundDeps{
		Downloader: client,
		Resolver:   client,
		Media:      g.mediaStore,
		Avatars:    g.channelAvatars(channelID),
		Groups:     g.channelGroups(channelID),
		ChannelID:  channelID,
		TenantID:   g.tenantFor(channelID),
	}, evt)
	if err != nil {
		if errors.Is(err, mapper.ErrSkip) {
			g.logger.Info("gateway: skip non-content inbound event", "channel_id", channelID, "message_id", evt.Info.ID)
			return
		}
		g.logger.Error("gateway: build inbound event", "channel_id", channelID, "message_id", evt.Info.ID, "error", err)
		return
	}

	routingKey := amqp.InboundRoutingKey
	publish := g.publisher.PublishInbound
	if inbound.Group != nil {
		routingKey = amqp.GroupInboundRoutingKey
		publish = g.publisher.PublishGroupInbound
	}

	if err := publish(g.workCtx, inbound); err != nil {
		g.logger.Error("gateway: publish inbound event", "channel_id", channelID, "message_id", evt.Info.ID, "error", err)
		return
	}

	g.logger.Info("gateway: inbound event published to sender.events",
		"channel_id", channelID,
		"message_id", evt.Info.ID,
		"tenant_id", g.tenantFor(channelID),
		"routing_key", routingKey)
}

func (g *gateway) handleReceipt(channelID string, evt *events.Receipt) {
	deps := mapper.GroupStatusDeps{ChannelID: channelID, TenantID: g.tenantFor(channelID)}
	if group := mapper.BuildGroupStatus(deps, evt); group != nil {
		if err := g.publisher.PublishGroupStatus(g.workCtx, group); err != nil {
			g.logger.Error("gateway: publish group status event", "channel_id", channelID, "error", err)
		}
		return
	}

	for _, status := range mapper.BuildStatus(evt) {
		if err := g.publisher.PublishStatus(g.workCtx, status); err != nil {
			g.logger.Error("gateway: publish status event", "channel_id", channelID, "error", err)
		}
	}
}

func (g *gateway) setTenant(channelID, tenantID string) {
	g.tenantMu.Lock()
	g.tenantByChannel[channelID] = tenantID
	g.tenantMu.Unlock()
}

func (g *gateway) tenantFor(channelID string) string {
	g.tenantMu.RLock()
	defer g.tenantMu.RUnlock()
	return g.tenantByChannel[channelID]
}

func (g *gateway) clearTenant(channelID string) {
	g.tenantMu.Lock()
	delete(g.tenantByChannel, channelID)
	g.tenantMu.Unlock()
}

func NewWAClientFactory(container *sqlstore.Container, waLogger waLog.Logger, slogger *slog.Logger) session.ClientFactory {
	return func(channelID string, jid *types.JID) (session.WAClient, error) {
		device, err := store.DeviceFor(context.Background(), container, jid)
		if err != nil {
			return nil, fmt.Errorf("gateway: resolve device for channel %s: %w", channelID, err)
		}
		if device == nil {
			return nil, fmt.Errorf("gateway: no stored device for channel %s (jid %s)", channelID, jid)
		}
		return session.NewWAClient(channelID, device, waLogger, slogger), nil
	}
}

func fetchMediaURL(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("gateway: build media fetch request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gateway: fetch media %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gateway: fetch media %s: unexpected status %d", url, resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("gateway: read media %s: %w", url, err)
	}
	return data, nil
}
