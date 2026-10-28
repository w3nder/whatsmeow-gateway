package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/w3nder/whatsmeow-gateway/internal/deviceprops"
)

var ErrNoSession = errors.New("session: channel has no live session")

var ErrSocketDown = errors.New("session: socket still down")

var ErrInvalidPhoneNumber = errors.New("session: invalid phone number for a pairing code")

var ErrPairingCodeExpired = errors.New("session: pairing code expired before it was used")

var ErrPairingTimeout = errors.New("session: the QR code was not scanned in time")

const connectWait = 10 * time.Second

type PairUpdate struct {
	QR        string
	Code      string
	Connected bool
	Err       error
}

type ClientFactory func(channelID string, jid *types.JID) (WAClient, error)

type pairing struct {
	qr       string
	emitted  time.Time
	lifetime time.Duration

	phone string
	code  string

	importHistory bool
	stop          chan struct{}
	stopOnce      sync.Once
	done          chan struct{}
}

func newPairing(importHistory bool, phone string) *pairing {
	return &pairing{importHistory: importHistory, phone: phone, stop: make(chan struct{}), done: make(chan struct{})}
}

func (p *pairing) byCode() bool {
	return p.phone != ""
}

func (p *pairing) halt() {
	p.stopOnce.Do(func() { close(p.stop) })
}

func (p *pairing) validQR(now time.Time) (string, bool) {
	if p.qr == "" || !now.Before(p.emitted.Add(p.lifetime)) {
		return "", false
	}
	return p.qr, true
}

func (p *pairing) clear() {
	p.qr, p.emitted, p.lifetime, p.code = "", time.Time{}, 0, ""
}

func (p *pairing) retained(now time.Time) (PairUpdate, bool) {
	if p.byCode() {
		return PairUpdate{Code: p.code}, p.code != ""
	}
	code, valid := p.validQR(now)
	return PairUpdate{QR: code}, valid
}

type channelLock struct {
	mu      sync.Mutex
	holders int
}

type Manager struct {
	factory  ClientFactory
	slotWait time.Duration

	mu       sync.Mutex
	sessions map[string]WAClient
	pairings map[string]*pairing
	muted    map[string]*atomic.Bool
	locks    map[string]*channelLock

	handlersMu     sync.RWMutex
	handlers       []func(channelID string, evt any)
	unlinkHandlers []func(channelID string)
}

type Option func(*Manager)

func WithPairingSlotWait(wait time.Duration) Option {
	return func(m *Manager) { m.slotWait = wait }
}

func NewManager(factory ClientFactory, options ...Option) *Manager {
	m := &Manager{
		factory:  factory,
		slotWait: deviceprops.SlotWait,
		sessions: make(map[string]WAClient),
		pairings: make(map[string]*pairing),
		muted:    make(map[string]*atomic.Bool),
		locks:    make(map[string]*channelLock),
	}
	for _, option := range options {
		option(m)
	}
	return m
}

func (m *Manager) OnEvent(handler func(channelID string, evt any)) {
	m.handlersMu.Lock()
	defer m.handlersMu.Unlock()
	m.handlers = append(m.handlers[:len(m.handlers):len(m.handlers)], handler)
}

func (m *Manager) OnUnlink(handler func(channelID string)) {
	m.handlersMu.Lock()
	defer m.handlersMu.Unlock()
	m.unlinkHandlers = append(m.unlinkHandlers[:len(m.unlinkHandlers):len(m.unlinkHandlers)], handler)
}

func (m *Manager) Pair(ctx context.Context, channelID string, importHistory, relinkDevice bool) (<-chan PairUpdate, error) {
	return m.pair(ctx, channelID, importHistory, relinkDevice, "")
}

func (m *Manager) PairByCode(ctx context.Context, channelID, phone string, importHistory, relinkDevice bool) (<-chan PairUpdate, error) {
	return m.pair(ctx, channelID, importHistory, relinkDevice, phone)
}

func (m *Manager) pair(ctx context.Context, channelID string, importHistory, relinkDevice bool, phone string) (<-chan PairUpdate, error) {
	client, paired, err := m.pairingClient(ctx, channelID, importHistory && relinkDevice)
	if err != nil {
		return nil, err
	}
	if paired {
		updates := make(chan PairUpdate, 1)
		updates <- PairUpdate{Connected: true}
		close(updates)
		return updates, nil
	}

	p, joined, superseded := m.beginPairing(channelID, importHistory, phone)
	if superseded != nil {
		select {
		case <-superseded.done:
		case <-ctx.Done():
			return nil, fmt.Errorf("session: wait for the superseded pairing of %s to end: %w", channelID, ctx.Err())
		}
		return m.pair(ctx, channelID, importHistory, relinkDevice, phone)
	}
	if joined != nil {
		return joined, nil
	}

	qr, err := client.QRChannel(ctx)
	if err != nil {
		m.endPairing(channelID, p, client)
		close(p.done)
		return nil, fmt.Errorf("session: open qr channel for %s: %w", channelID, err)
	}
	if importHistory {
		client.TakeOverHistory()
	}
	release, err := deviceprops.Acquire(ctx, importHistory, m.slotWait)
	if err != nil {
		m.endPairing(channelID, p, client)
		close(p.done)
		return nil, fmt.Errorf("session: wait for the pairing slot for %s: %w", channelID, err)
	}
	if err := client.Connect(); err != nil {
		m.endPairing(channelID, p, client)
		release()
		close(p.done)
		return nil, fmt.Errorf("session: connect for pairing %s: %w", channelID, err)
	}

	updates := make(chan PairUpdate)
	go func() {
		defer close(p.done)
		defer close(updates)
		defer release()
		defer m.endPairing(channelID, p, client)

		for {
			select {
			case evt, ok := <-qr:
				if !ok {
					return
				}
				var update PairUpdate
				ends := false
				switch {
				case evt.Event == whatsmeow.QRChannelEventCode && p.byCode():
					if m.hasCode(channelID, p) {
						continue
					}
					code, err := client.PairPhone(ctx, p.phone)
					if err != nil {
						update, ends = PairUpdate{Err: pairPhoneError(channelID, err)}, true
						break
					}
					if !m.retainCode(channelID, p, code) {
						continue
					}
					update = PairUpdate{Code: code}
				case evt.Event == whatsmeow.QRChannelEventCode:
					if !m.retainQR(channelID, p, evt.Code, evt.Timeout) {
						continue
					}
					update = PairUpdate{QR: evt.Code}
				case evt.Event == whatsmeow.QRChannelSuccess.Event:
					m.clearQR(channelID, p)
					update = PairUpdate{Connected: true}
				default:
					m.clearQR(channelID, p)
					update = PairUpdate{Err: pairingEndError(p, evt)}
				}
				select {
				case updates <- update:
				case <-ctx.Done():
					return
				case <-p.stop:
					return
				}
				if ends {
					return
				}
			case <-ctx.Done():
				return
			case <-p.stop:
				return
			}
		}
	}()

	return updates, nil
}

func (m *Manager) pairingClient(ctx context.Context, channelID string, relink bool) (WAClient, bool, error) {
	unlock := m.lockChannel(channelID)
	defer unlock()

	client, err := m.client(channelID)
	if err != nil {
		return nil, false, err
	}
	if client.DeviceJID() == nil {
		return client, false, nil
	}
	if err := ensureUp(client); err != nil {
		return nil, false, err
	}
	if !relink {
		return client, true, nil
	}
	if err := m.unlink(ctx, channelID, client); err != nil {
		return nil, false, err
	}
	fresh, err := m.client(channelID)
	if err != nil {
		return nil, false, err
	}
	return fresh, false, nil
}

func (m *Manager) lockChannel(channelID string) func() {
	m.mu.Lock()
	lock, ok := m.locks[channelID]
	if !ok {
		lock = &channelLock{}
		m.locks[channelID] = lock
	}
	lock.holders++
	m.mu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		m.mu.Lock()
		lock.holders--
		if lock.holders == 0 {
			delete(m.locks, channelID)
		}
		m.mu.Unlock()
	}
}

func (m *Manager) beginPairing(channelID string, importHistory bool, phone string) (*pairing, <-chan PairUpdate, *pairing) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if live, ok := m.pairings[channelID]; ok {
		if importHistory != live.importHistory || phone != live.phone {
			live.halt()
			return nil, nil, live
		}
		updates := make(chan PairUpdate, 1)
		if update, valid := live.retained(time.Now()); valid {
			updates <- update
		}
		close(updates)
		return nil, updates, nil
	}

	p := newPairing(importHistory, phone)
	m.pairings[channelID] = p
	return p, nil, nil
}

func (m *Manager) endPairing(channelID string, p *pairing, client WAClient) {
	m.mu.Lock()
	if m.pairings[channelID] == p {
		delete(m.pairings, channelID)
	}
	m.mu.Unlock()

	if client.DeviceJID() == nil {
		m.drop(channelID, client)
	}
}

func (m *Manager) retainQR(channelID string, p *pairing, code string, timeout time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.pairings[channelID] != p {
		return false
	}

	if code == "" || timeout <= 0 {
		p.clear()
		return code != ""
	}

	now := time.Now()
	emitted := now
	if scheduled := p.emitted.Add(p.lifetime); !p.emitted.IsZero() && scheduled.Before(now) {
		emitted = scheduled
	}

	p.qr, p.emitted, p.lifetime = code, emitted, timeout
	return now.Before(emitted.Add(timeout))
}

func (m *Manager) hasCode(channelID string, p *pairing) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.pairings[channelID] == p && p.code != ""
}

func (m *Manager) retainCode(channelID string, p *pairing, code string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.pairings[channelID] != p || code == "" {
		return false
	}
	p.code = code
	return true
}

func pairPhoneError(channelID string, err error) error {
	if errors.Is(err, whatsmeow.ErrPhoneNumberTooShort) || errors.Is(err, whatsmeow.ErrPhoneNumberIsNotInternational) {
		return fmt.Errorf("%w: %w", ErrInvalidPhoneNumber, err)
	}
	return fmt.Errorf("session: ask a pairing code for %s: %w", channelID, err)
}

func pairingEndError(p *pairing, evt whatsmeow.QRChannelItem) error {
	if evt.Error != nil {
		return evt.Error
	}
	if evt.Event == whatsmeow.QRChannelTimeout.Event {
		if p.byCode() {
			return ErrPairingCodeExpired
		}
		return ErrPairingTimeout
	}
	return fmt.Errorf("whatsmeow-gateway: pairing ended with event %q", evt.Event)
}

func (m *Manager) clearQR(channelID string, p *pairing) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.pairings[channelID] != p {
		return
	}
	p.clear()
}

func (m *Manager) EnsureConnected(channelID string) error {
	client, err := m.session(channelID)
	if err != nil {
		return err
	}
	return ensureUp(client)
}

func ensureUp(client WAClient) error {
	if client.IsConnected() && client.IsLoggedIn() {
		return nil
	}
	if err := client.Connect(); err != nil && !errors.Is(err, whatsmeow.ErrAlreadyConnected) {
		return err
	}
	if client.WaitForConnection(connectWait) {
		return nil
	}
	return fmt.Errorf("%w after %s, auto-reconnect still running", ErrSocketDown, connectWait)
}

func (m *Manager) Send(ctx context.Context, channelID string, to types.JID, msg *waE2E.Message, id string, nodes []waBinary.Node) (string, time.Time, error) {
	client, err := m.session(channelID)
	if err != nil {
		return "", time.Time{}, err
	}
	resp, err := client.SendMessage(ctx, to, msg, id, nodes)
	if err != nil {
		return "", time.Time{}, err
	}
	return resp.ID, resp.Timestamp, nil
}

func (m *Manager) Client(channelID string) (WAClient, error) {
	return m.session(channelID)
}

func (m *Manager) DisconnectAll() {
	m.mu.Lock()
	sessions := make([]WAClient, 0, len(m.sessions))
	for _, c := range m.sessions {
		sessions = append(sessions, c)
	}
	m.sessions = make(map[string]WAClient)
	m.muted = make(map[string]*atomic.Bool)
	m.pairings = make(map[string]*pairing)
	m.mu.Unlock()

	for _, c := range sessions {
		c.Disconnect()
	}
}

func (m *Manager) session(channelID string) (WAClient, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	c, ok := m.sessions[channelID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNoSession, channelID)
	}
	return c, nil
}

func (m *Manager) client(channelID string) (WAClient, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if c, ok := m.sessions[channelID]; ok {
		return c, nil
	}

	c, err := m.factory(channelID, nil)
	if err != nil {
		return nil, err
	}

	m.register(channelID, c)
	return c, nil
}

func (m *Manager) Resume(ctx context.Context, channelID string, jid types.JID, importingHistory bool) error {
	m.mu.Lock()
	if _, ok := m.sessions[channelID]; ok {
		m.mu.Unlock()
		return nil
	}

	c, err := m.factory(channelID, &jid)
	if err != nil {
		m.mu.Unlock()
		return fmt.Errorf("session: resume device for channel %s: %w", channelID, err)
	}

	m.register(channelID, c)
	m.mu.Unlock()

	if importingHistory {
		c.TakeOverHistory()
	}
	if err := c.Connect(); err != nil {
		m.drop(channelID, c)
		return fmt.Errorf("session: connect resumed channel %s: %w", channelID, err)
	}

	return nil
}

func (m *Manager) register(channelID string, c WAClient) {
	muted := &atomic.Bool{}
	c.AddEventHandler(func(evt any) {
		if muted.Load() {
			return
		}
		if _, ok := evt.(*events.LoggedOut); ok {
			go m.drop(channelID, c)
		}
		m.dispatch(channelID, evt)
	})

	m.sessions[channelID] = c
	m.muted[channelID] = muted
}

func (m *Manager) unlink(ctx context.Context, channelID string, c WAClient) error {
	muted := m.mutedFlag(channelID, c)
	muted.Store(true)
	if err := c.Logout(ctx); err != nil {
		muted.Store(false)
		return fmt.Errorf("session: unlink the paired device of %s to pair it again with the history: %w", channelID, err)
	}
	m.drop(channelID, c)

	m.handlersMu.RLock()
	handlers := m.unlinkHandlers
	m.handlersMu.RUnlock()
	for _, h := range handlers {
		h(channelID)
	}
	return nil
}

func (m *Manager) mutedFlag(channelID string, c WAClient) *atomic.Bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.sessions[channelID] == c {
		return m.muted[channelID]
	}
	return &atomic.Bool{}
}

func (m *Manager) drop(channelID string, c WAClient) {
	m.mu.Lock()
	if m.sessions[channelID] == c {
		delete(m.sessions, channelID)
		delete(m.muted, channelID)
	}
	m.mu.Unlock()

	c.Disconnect()
}

func (m *Manager) dispatch(channelID string, evt any) {
	m.handlersMu.RLock()
	handlers := m.handlers
	m.handlersMu.RUnlock()

	for _, h := range handlers {
		h(channelID, evt)
	}
}
