package channelsettings

import "sync"

type Map struct {
	mu        sync.RWMutex
	byChannel map[string]Settings
}

func NewMap() *Map {
	return &Map{byChannel: make(map[string]Settings)}
}

func (m *Map) Set(channelID string, s Settings) {
	m.mu.Lock()
	m.byChannel[channelID] = s
	m.mu.Unlock()
}

func (m *Map) For(channelID string) Settings {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if s, ok := m.byChannel[channelID]; ok {
		return s
	}
	return Defaults()
}

func (m *Map) Clear(channelID string) {
	m.mu.Lock()
	delete(m.byChannel, channelID)
	m.mu.Unlock()
}
