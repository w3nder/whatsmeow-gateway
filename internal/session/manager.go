package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

var ErrNoSession = errors.New("session: channel has no live session")

const connectWait = 10 * time.Second

type PairUpdate struct {
	QR        string
	Connected bool
	Err       error
}

type ClientFactory func(channelID string, jid *types.JID) (WAClient, error)

type pairing struct {
	qr       string
	emitted  time.Time
	lifetime time.Duration
}

func (p *pairing) validQR(now time.Time) (string, bool) {
	if p.qr == "" || !now.Before(p.emitted.Add(p.lifetime)) {
		return "", false
	}
	return p.qr, true
}

func (p *pairing) clear() {
	p.qr, p.emitted, p.lifetime = "", time.Time{}, 0
}

type Manager struct {
	factory ClientFactory

	mu       sync.Mutex
	sessions map[string]WAClient
	pairings map[string]*pairing

	handlersMu sync.RWMutex
	handlers   []func(channelID string, evt any)
}

func NewManager(factory ClientFactory) *Manager {
	return &Manager{
		factory:  factory,
		sessions: make(map[string]WAClient),
		pairings: make(map[string]*pairing),
	}
}

func (m *Manager) OnEvent(handler func(channelID string, evt any)) {
	m.handlersMu.Lock()
	defer m.handlersMu.Unlock()
	m.handlers = append(m.handlers[:len(m.handlers):len(m.handlers)], handler)
}

func (m *Manager) Pair(ctx context.Context, channelID string) (<-chan PairUpdate, error) {
	client, err := m.client(channelID)
	if err != nil {
		return nil, err
	}

	if client.DeviceJID() != nil {
		if err := ensureUp(client); err != nil {
			return nil, err
		}
		updates := make(chan PairUpdate, 1)
		updates <- PairUpdate{Connected: true}
		close(updates)
		return updates, nil
	}

	p, joined := m.beginPairing(channelID)
	if joined != nil {
		return joined, nil
	}

	qr, err := client.QRChannel(ctx)
	if err != nil {
		m.endPairing(channelID, p, client)
		return nil, fmt.Errorf("session: open qr channel for %s: %w", channelID, err)
	}
	if err := client.Connect(); err != nil {
		m.endPairing(channelID, p, client)
		return nil, fmt.Errorf("session: connect for pairing %s: %w", channelID, err)
	}

	updates := make(chan PairUpdate)
	go func() {
		defer close(updates)
		defer m.endPairing(channelID, p, client)

		for {
			select {
			case evt, ok := <-qr:
				if !ok {
					return
				}
				var update PairUpdate
				switch evt.Event {
				case "code":
					if !m.retainQR(channelID, p, evt.Code, evt.Timeout) {
						continue
					}
					update = PairUpdate{QR: evt.Code}
				case "success":
					m.clearQR(channelID, p)
					update = PairUpdate{Connected: true}
				default:
					m.clearQR(channelID, p)
					pairErr := evt.Error
					if pairErr == nil {
						pairErr = fmt.Errorf("whatsmeow-gateway: pairing ended with event %q", evt.Event)
					}
					update = PairUpdate{Err: pairErr}
				}
				select {
				case updates <- update:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return updates, nil
}

func (m *Manager) beginPairing(channelID string) (*pairing, <-chan PairUpdate) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if live, ok := m.pairings[channelID]; ok {
		updates := make(chan PairUpdate, 1)
		if code, valid := live.validQR(time.Now()); valid {
			updates <- PairUpdate{QR: code}
		}
		close(updates)
		return nil, updates
	}

	p := &pairing{}
	m.pairings[channelID] = p
	return p, nil
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
	return fmt.Errorf("session: socket still down after %s, auto-reconnect still running", connectWait)
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

func (m *Manager) Resume(ctx context.Context, channelID string, jid types.JID) error {
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

	if err := c.Connect(); err != nil {
		m.drop(channelID, c)
		return fmt.Errorf("session: connect resumed channel %s: %w", channelID, err)
	}

	return nil
}

func (m *Manager) register(channelID string, c WAClient) {
	c.AddEventHandler(func(evt any) {
		if _, ok := evt.(*events.LoggedOut); ok {
			go m.drop(channelID, c)
		}
		m.dispatch(channelID, evt)
	})

	m.sessions[channelID] = c
}

func (m *Manager) drop(channelID string, c WAClient) {
	m.mu.Lock()
	if m.sessions[channelID] == c {
		delete(m.sessions, channelID)
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
