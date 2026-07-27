package gateway

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/w3nder/whatsmeow-gateway/internal/dedupe"
	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
	"github.com/w3nder/whatsmeow-gateway/internal/ownership"
	"github.com/w3nder/whatsmeow-gateway/internal/registry"
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
	ShutdownDrainTimeout time.Duration
	Logger               *slog.Logger
}

type gateway struct {
	consumer             *amqp.Consumer
	publisher            *amqp.Publisher
	manager              *session.Manager
	ownership            *ownership.Store
	dedupe               *dedupe.Store
	registry             *registry.Store
	mediaStore           mapper.MediaStore
	instanceID           string
	shardLockTTL         time.Duration
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
		instanceID:           deps.InstanceID,
		shardLockTTL:         deps.ShardLockTTL,
		shutdownDrainTimeout: deps.ShutdownDrainTimeout,
		logger:               deps.Logger,
		workCtx:              context.WithoutCancel(ctx),
		tenantByChannel:      make(map[string]string),
	}

	return g.run(ctx)
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

	g.logger.Info("gateway started", "instance_id", g.instanceID)
	<-ctx.Done()
	g.logger.Info("gateway stopping")

	g.closeConsumerWithDrainDeadline()

	g.manager.DisconnectAll()

	if err := g.ownership.ReleaseAll(g.workCtx, g.instanceID); err != nil {
		g.logger.Error("gateway: release shards", "error", err)
		return fmt.Errorf("gateway: release shards: %w", err)
	}

	return nil
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

func (g *gateway) SendHandler(ctx context.Context, cmd amqp.GatewaySendCommand) error {
	g.setTenant(cmd.ChannelID, cmd.TenantID)

	providerID := dedupe.DeterministicProviderID(cmd.MessageID)

	alreadySent, existingProviderID, err := g.dedupe.Begin(ctx, cmd.MessageID, providerID)
	if err != nil {
		return fmt.Errorf("gateway: dedupe begin %s: %w", cmd.MessageID, err)
	}
	if alreadySent {
		if err := g.publisher.PublishStatus(ctx, mapper.StatusEvent{
			ProviderMessageID: existingProviderID,
			Status:            "sent",
			Timestamp:         strconv.FormatInt(time.Now().Unix(), 10),
		}); err != nil {
			return fmt.Errorf("gateway: publish sent status (dedup replay) %s: %w", cmd.MessageID, err)
		}
		return nil
	}

	if err := g.manager.EnsureConnected(cmd.ChannelID); err != nil {
		return fmt.Errorf("gateway: ensure connected %s: %w", cmd.ChannelID, err)
	}

	client, err := g.waClientFor(cmd.ChannelID)
	if err != nil {
		return fmt.Errorf("gateway: resolve client %s: %w", cmd.ChannelID, err)
	}

	to, msg, err := mapper.BuildOutbound(ctx, client, cmd, fetchMediaURL)
	if err != nil {
		return fmt.Errorf("gateway: build outbound %s: %w", cmd.MessageID, err)
	}

	id, ts, err := g.manager.Send(ctx, cmd.ChannelID, to, msg, providerID)
	if err != nil {
		return fmt.Errorf("gateway: send %s: %w", cmd.MessageID, err)
	}

	if err := g.dedupe.MarkSent(ctx, cmd.MessageID); err != nil {
		return fmt.Errorf("gateway: mark sent %s: %w", cmd.MessageID, err)
	}

	if err := g.publisher.PublishStatus(ctx, mapper.StatusEvent{
		ProviderMessageID: id,
		Status:            "sent",
		Timestamp:         strconv.FormatInt(ts.Unix(), 10),
	}); err != nil {
		return fmt.Errorf("gateway: publish sent status %s: %w", cmd.MessageID, err)
	}

	return nil
}

func (g *gateway) waClientFor(channelID string) (session.WAClient, error) {
	return g.manager.Client(channelID)
}

func (g *gateway) handleSessionEvent(channelID string, evt any) {
	switch e := evt.(type) {
	case *events.Message:
		g.handleInboundMessage(channelID, e)
	case *events.Receipt:
		g.handleReceipt(channelID, e)
	case *events.LoggedOut:
		g.clearTenant(channelID)
		if err := g.registry.Delete(g.workCtx, channelID); err != nil {
			g.logger.Error("gateway: delete session on logout", "channel_id", channelID, "error", err)
		}
	}
}

func (g *gateway) handleInboundMessage(channelID string, evt *events.Message) {
	client, err := g.waClientFor(channelID)
	if err != nil {
		g.logger.Error("gateway: resolve client for inbound event", "channel_id", channelID, "error", err)
		return
	}

	inbound, err := mapper.BuildInbound(g.workCtx, client, g.mediaStore, channelID, g.tenantFor(channelID), evt)
	if err != nil {
		if errors.Is(err, mapper.ErrSkip) {
			g.logger.Debug("gateway: skip non-content inbound event", "channel_id", channelID)
			return
		}
		g.logger.Error("gateway: build inbound event", "channel_id", channelID, "error", err)
		return
	}

	if err := g.publisher.PublishInbound(g.workCtx, inbound); err != nil {
		g.logger.Error("gateway: publish inbound event", "channel_id", channelID, "error", err)
	}
}

func (g *gateway) handleReceipt(channelID string, evt *events.Receipt) {
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

func NewWAClientFactory(container *sqlstore.Container, waLogger waLog.Logger) session.ClientFactory {
	return func(channelID string, jid *types.JID) (session.WAClient, error) {
		device, err := store.DeviceFor(context.Background(), container, jid)
		if err != nil {
			return nil, fmt.Errorf("gateway: resolve device for channel %s: %w", channelID, err)
		}
		if device == nil {
			return nil, fmt.Errorf("gateway: no stored device for channel %s (jid %s)", channelID, jid)
		}
		return session.NewWAClient(device, waLogger), nil
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
