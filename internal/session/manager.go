package session

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type PairUpdate struct {
	QR        string
	Connected bool
	Err       error
}

type ClientFactory func(channelID string) (WAClient, error)

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
		if err := client.Connect(); err != nil {
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
		for evt := range qr {
			switch evt.Event {
			case "code":
				updates <- PairUpdate{QR: evt.Code}
			case "success":
				updates <- PairUpdate{Connected: true}
			default:
				pairErr := evt.Error
				if pairErr == nil {
					pairErr = fmt.Errorf("whatsmeow-gateway: pairing ended with event %q", evt.Event)
				}
				updates <- PairUpdate{Err: pairErr}
			}
		}
	}()

	return updates, nil
}

func (m *Manager) EnsureConnected(ctx context.Context, channelID string) error {
	client, err := m.client(channelID)
	if err != nil {
		return err
	}
	if client.IsLoggedIn() {
		return nil
	}
	return client.Connect()
}

func (m *Manager) Send(ctx context.Context, channelID string, to types.JID, msg *waE2E.Message) (string, time.Time, error) {
	client, err := m.client(channelID)
	if err != nil {
		return "", time.Time{}, err
	}
	resp, err := client.SendMessage(ctx, to, msg)
	if err != nil {
		return "", time.Time{}, err
	}
	return resp.ID, resp.Timestamp, nil
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

func (m *Manager) client(channelID string) (WAClient, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if c, ok := m.sessions[channelID]; ok {
		return c, nil
	}

	c, err := m.factory(channelID)
	if err != nil {
		return nil, err
	}

	c.AddEventHandler(func(evt any) {
		if _, ok := evt.(*events.LoggedOut); ok {
			m.mu.Lock()
			delete(m.sessions, channelID)
			m.mu.Unlock()
		}
		m.dispatch(channelID, evt)
	})

	m.sessions[channelID] = c
	return c, nil
}

func (m *Manager) dispatch(channelID string, evt any) {
	m.handlersMu.RLock()
	handlers := m.handlers
	m.handlersMu.RUnlock()

	for _, h := range handlers {
		h(channelID, evt)
	}
}
