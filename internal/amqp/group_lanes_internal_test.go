package amqp

import (
	"sync"
	"testing"

	rabbitmq "github.com/rabbitmq/amqp091-go"
)

type recordingAcknowledger struct {
	mu    sync.Mutex
	nacks []uint64
}

func (r *recordingAcknowledger) Ack(uint64, bool) error { return nil }
func (r *recordingAcknowledger) Reject(uint64, bool) error {
	return nil
}
func (r *recordingAcknowledger) Nack(tag uint64, _ bool, requeue bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if requeue {
		r.nacks = append(r.nacks, tag)
	}
	return nil
}

func TestRequeueInOrderNacksByDeliveryTagNotByFinishOrder(t *testing.T) {
	ack := &recordingAcknowledger{}
	finished := []rabbitmq.Delivery{{Acknowledger: ack, DeliveryTag: 7}, {Acknowledger: ack, DeliveryTag: 3}, {Acknowledger: ack, DeliveryTag: 5}}
	requeueInOrder(finished)
	if got := ack.nacks; len(got) != 3 || got[0] != 3 || got[1] != 5 || got[2] != 7 {
		t.Fatalf("requeue must follow the original delivery order, got %v", got)
	}
}

func TestLanesRefuseAJobBeyondTheBacklog(t *testing.T) {
	l := newLanes(2)
	release := make(chan struct{})
	started := make(chan struct{})
	if !l.run("a", func() { close(started); <-release }) {
		t.Fatal("the first job must run")
	}
	<-started
	for i := range 2 {
		if !l.run("a", func() {}) {
			t.Fatalf("waiting job %d is within the backlog", i+1)
		}
	}
	if l.run("a", func() {}) {
		t.Fatal("a job beyond the backlog of its channel must be refused")
	}
	if !l.run("b", func() {}) {
		t.Fatal("another channel must still be accepted while one channel is full")
	}
	close(release)
	l.wait()
}

func overflowFor(t *testing.T) *overflow {
	t.Helper()
	return &overflow{next: map[string]int64{}, expected: map[string]int64{}, queue: make(chan overflowItem, 16)}
}

func copyOf(seq int64) rabbitmq.Delivery {
	return rabbitmq.Delivery{Headers: rabbitmq.Table{overflowHeader: seq}}
}

func TestOverflowKeepsTheOrderOfOneChannel(t *testing.T) {
	o := overflowFor(t)
	if !o.admits("a", rabbitmq.Delivery{}) {
		t.Fatal("a channel with nothing spilled admits new commands")
	}
	o.send("a", rabbitmq.Delivery{})
	o.send("a", rabbitmq.Delivery{})
	first, second := <-o.queue, <-o.queue
	if first.seq != 1 || second.seq != 2 {
		t.Fatalf("spilled commands are numbered in arrival order, got %d and %d", first.seq, second.seq)
	}
	if o.admits("a", rabbitmq.Delivery{}) {
		t.Fatal("a new command must queue behind the spilled ones of its channel")
	}
	if !o.admits("b", rabbitmq.Delivery{}) {
		t.Fatal("another channel is never held by the spill of the first")
	}
	if o.admits("a", copyOf(2)) {
		t.Fatal("the second copy must not overtake the first")
	}
	o.send("a", copyOf(2))
	if again := <-o.queue; again.seq != 2 {
		t.Fatalf("a copy sent back keeps its number, got %d", again.seq)
	}
	if !o.admits("a", copyOf(1)) {
		t.Fatal("the oldest copy is admitted")
	}
	o.admitted("a", copyOf(1))
	if !o.admits("a", copyOf(2)) {
		t.Fatal("then the next one")
	}
	o.admitted("a", copyOf(2))
	if !o.admits("a", rabbitmq.Delivery{}) {
		t.Fatal("once every copy is back, new commands flow again")
	}
	if len(o.next) != 0 || len(o.expected) != 0 {
		t.Fatalf("a drained channel leaves no state behind, next=%v expected=%v", o.next, o.expected)
	}
}
