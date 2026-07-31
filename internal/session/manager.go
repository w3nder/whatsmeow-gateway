package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// ErrNoSession means the channel has no live session on this instance. Callers must
// resume it from its stored JID: building a device from scratch would produce an
// unpaired one and force the user through the QR flow again.
var ErrNoSession = errors.New("session: channel has no live session")

// connectWait bounds how long a caller blocks waiting for a socket to come back up.
// whatsmeow keeps auto-reconnecting past this deadline, so exceeding it fails only the
// message in flight, never the channel.
const connectWait = 10 * time.Second

type PairUpdate struct {
	QR        string
	Connected bool
	Err       error
}

type ClientFactory func(channelID string, jid *types.JID) (WAClient, error)

type Manager struct {
	factory ClientFactory

	mu       sync.Mutex
	sessions map[string]WAClient

	handlersMu sync.RWMutex
	handlers   []func(channelID string, evt any)
}

func NewManager(factory ClientFactory) *Manager {
	return &Manager{
		factory:  factory,
		sessions: make(map[string]WAClient),
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

	qr, err := client.QRChannel(ctx)
	if err != nil {
		return nil, err
	}
	if err := client.Connect(); err != nil {
		return nil, err
	}

	updates := make(chan PairUpdate)
	go func() {
		defer close(updates)
		for {
			select {
			case evt, ok := <-qr:
				if !ok {
					return
				}
				var update PairUpdate
				switch evt.Event {
				case "code":
					update = PairUpdate{QR: evt.Code}
				case "success":
					update = PairUpdate{Connected: true}
				default:
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

func (m *Manager) EnsureConnected(channelID string) error {
	client, err := m.session(channelID)
	if err != nil {
		return err
	}
	return ensureUp(client)
}

// ensureUp brings a session's socket back up.
//
// IsLoggedIn is not enough to decide: whatsmeow only clears that flag on a stream
// error, so a socket killed by the network still reports the device as logged in.
// IsConnected is the one that reflects the socket. Racing whatsmeow's own
// auto-reconnect is expected and surfaces as ErrAlreadyConnected, which means the
// socket is up, not that the call failed.
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

func (m *Manager) Send(ctx context.Context, channelID string, to types.JID, msg *waE2E.Message, id string) (string, time.Time, error) {
	client, err := m.session(channelID)
	if err != nil {
		return "", time.Time{}, err
	}
	resp, err := client.SendMessage(ctx, to, msg, id)
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
	m.mu.Unlock()

	for _, c := range sessions {
		c.Disconnect()
	}
}

// session returns the live session for a channel. It never builds a device: only
// pairing and resuming know which device a channel belongs to.
func (m *Manager) session(channelID string) (WAClient, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	c, ok := m.sessions[channelID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNoSession, channelID)
	}
	return c, nil
}

// client returns the live session for a channel, building a fresh (unpaired) device
// when there is none. Only the pairing flow may use it.
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

	// Register before connecting so connection events emitted during the handshake
	// reach the gateway, and connect outside the lock because the handler takes it.
	m.register(channelID, c)
	m.mu.Unlock()

	if err := c.Connect(); err != nil {
		m.drop(channelID, c)
		return fmt.Errorf("session: connect resumed channel %s: %w", channelID, err)
	}

	return nil
}

// register wires a client's events to the manager and stores it. m.mu must be held.
func (m *Manager) register(channelID string, c WAClient) {
	c.AddEventHandler(func(evt any) {
		if _, ok := evt.(*events.LoggedOut); ok {
			// LoggedOut is the one disconnect that must not be retried: the device is
			// gone from the phone and the channel has to be paired again.
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
