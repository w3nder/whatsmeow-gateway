package avatar

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestEvictIdleSkipsAnEntryThatIsMidResolve(t *testing.T) {
	c := New(nil, nil, Options{}, discardLogger())
	const key = "ch1|5511999998888@s.whatsapp.net"

	e := &entry{expiresAt: c.now().Add(-2 * idleBeforeEviction)}
	c.entries[key] = e

	e.mu.Lock()

	c.mu.Lock()
	c.evictIdle()
	_, stillPresent := c.entries[key]
	c.mu.Unlock()

	e.mu.Unlock()

	if !stillPresent {
		t.Fatal("evictIdle dropped an entry whose lock was held by an in-flight resolve")
	}

	c.mu.Lock()
	c.evictIdle()
	_, presentAfterRelease := c.entries[key]
	c.mu.Unlock()

	if presentAfterRelease {
		t.Fatal("evictIdle kept an expired entry once its resolve finished")
	}
}

func TestEvictIdleReadsExpiresAtUnderTheEntryLock(t *testing.T) {
	c := New(nil, nil, Options{}, discardLogger())

	for i := 0; i < 64; i++ {
		c.entries[string(rune('a'+i%26))+string(rune('a'+i/26))] = &entry{expiresAt: c.now()}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for _, e := range c.entries {
		wg.Add(1)
		go func(e *entry) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				e.mu.Lock()
				e.expiresAt = time.Now()
				e.mu.Unlock()
			}
		}(e)
	}

	for i := 0; i < 200; i++ {
		c.mu.Lock()
		c.evictIdle()
		c.mu.Unlock()
	}

	close(stop)
	wg.Wait()
}
