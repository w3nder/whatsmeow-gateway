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

// pairing is one channel's live QR loop, from the moment Pair opens the QR channel
// until that loop ends. It carries the code whatsmeow emitted last so a pair command
// that arrives mid-flight can be answered with the code that is on the operator's
// screen right now instead of the one twenty seconds away.
type pairing struct {
	qr string
	// emitted is when whatsmeow released the code, which is not when this loop read
	// it, and lifetime is the window whatsmeow gave that one code. Together they are
	// the only honest answer to whether the code still stands -- see retainQR.
	emitted  time.Time
	lifetime time.Duration
}

// validQR returns the retained code only while the phone would still accept it.
// Replaying an expired code is worse than replaying nothing: the operator scans it,
// WhatsApp refuses it, and nothing on screen explains why.
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
	// pairings holds only the channels currently pairing, so a retained code dies
	// with its loop and can never be replayed to another channel or another tenant.
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

// Pair drives a channel through the QR flow, reporting every step on the returned
// channel, which closes once the pairing ends.
//
// Exactly one QR loop ever runs per channel. whatsmeow refuses to hand out a second
// QR channel for a client whose socket is up (ErrQRAlreadyConnected), so a second pair
// command -- the operator closing the pairing dialog and opening it again -- could not
// run its own loop even if we let it. It joins the live one instead: it is handed the
// retained code immediately when that code is still valid and then closes, leaving the
// running loop as the only publisher of the rotations and of the outcome. Nothing is
// published twice, and the operator stops staring at an empty box until the next
// rotation twenty seconds later.
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
		// One cleanup for every way this loop ends -- a terminal event, whatsmeow
		// closing the channel, or the caller's context going away.
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
					// Retained before it is forwarded, because the whole point is to
					// answer a pair command that arrives while nobody is reading this
					// channel -- including while this very send is blocked.
					if !m.retainQR(channelID, p, evt.Code, evt.Timeout) {
						// Dead before it was read: it waited in whatsmeow's buffer
						// while this loop was busy. Sending it on would put a code in
						// front of the operator that the phone refuses, and the one
						// that replaced it is already queued behind it.
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

// beginPairing claims the channel's single QR loop for the caller. A caller that finds
// the loop already claimed gets no pairing to drive and, instead, a closed channel
// carrying the retained code when it is still valid -- see Pair.
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

// endPairing takes the channel out of the pairing state and, when the device never got
// paired, throws the client away.
//
// A client that reached a terminal QR event unpaired is finished: whatsmeow has already
// removed its QR event handler and, on the timeout path, disconnected it. Left in
// m.sessions it still looks like a live session, and it poisons both paths that come
// next -- EnsureConnected would try to bring an unpaired device up rather than reporting
// ErrNoSession, and the next pair command would reuse the same client, whose QRChannel
// answers ErrQRAlreadyConnected on the outcomes where whatsmeow leaves the socket up.
// Dropping it here is what lets the retry start from a fresh device and show a QR at
// once.
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

// retainQR records the code whatsmeow just emitted and reports whether it is still one
// the phone would accept.
//
// When a code is read says nothing about how much of its life is left. whatsmeow's
// emitter pushes a session's six codes into a channel buffered at eight, on a timer of
// its own, whether or not anyone is reading; a loop held up publishing one code comes
// back to find the next already emitted and part spent -- or wholly spent. So a code is
// dated from when the emitter released it, not from when it was read: the first when it
// is read, since nothing can have been buffered ahead of it, and every later one exactly
// one previous lifetime after the code before it, which is the emitter's own schedule.
func (m *Manager) retainQR(channelID string, p *pairing, code string, timeout time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.pairings[channelID] != p {
		return false
	}

	// A code with no stated lifetime cannot be dated or aged, so it is passed on this
	// once and never retained: there would be no way to tell later whether it still
	// stands.
	if code == "" || timeout <= 0 {
		p.clear()
		return code != ""
	}

	now := time.Now()
	emitted := now
	// Never later than now, whatever the clock did: the code is already in our hands.
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
	// Every QR loop dies with its socket, so no code may survive into whatever
	// pairs next.
	m.pairings = make(map[string]*pairing)
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
