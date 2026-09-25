package amqp

import (
	"errors"
	"sync"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"
)

func spillOnly(nonce string) *overflow {
	return &overflow{nonce: nonce, now: time.Now, channels: map[string]*channelSpill{}, queue: make(chan overflowItem, 16)}
}

func copyFrom(nonce string, seq int64) rabbitmq.Delivery {
	return rabbitmq.Delivery{Headers: rabbitmq.Table{overflowSeqHeader: seq, overflowNonceHeader: nonce}}
}

func TestOverflowKeepsTheOrderOfOneChannel(t *testing.T) {
	o := spillOnly("a")
	if !o.admits("x", rabbitmq.Delivery{}) {
		t.Fatal("a channel with nothing spilled admits new commands")
	}
	o.send("x", rabbitmq.Delivery{})
	o.send("x", rabbitmq.Delivery{})
	first, second := <-o.queue, <-o.queue
	if first.seq != 1 || second.seq != 2 {
		t.Fatalf("spilled commands are numbered in arrival order, got %d and %d", first.seq, second.seq)
	}
	if o.admits("x", rabbitmq.Delivery{}) {
		t.Fatal("a new command must queue behind the spilled ones of its channel")
	}
	if !o.admits("y", rabbitmq.Delivery{}) {
		t.Fatal("another channel is never held by the spill of the first")
	}
	if o.admits("x", copyFrom("a", 2)) {
		t.Fatal("the second copy must not overtake the first")
	}
	o.send("x", copyFrom("a", 2))
	if again := <-o.queue; again.seq != 2 {
		t.Fatalf("a copy sent back keeps its number, got %d", again.seq)
	}
	for _, seq := range []int64{1, 2} {
		if !o.admits("x", copyFrom("a", seq)) {
			t.Fatalf("copy %d is next and must be admitted", seq)
		}
		o.admitted("x", copyFrom("a", seq))
	}
	if !o.admits("x", rabbitmq.Delivery{}) || len(o.channels) != 0 {
		t.Fatalf("once every copy is back new commands flow and no state is left, state %v", o.channels)
	}
}

func TestOverflowAdmitsAStaleCopyAsAReplay(t *testing.T) {
	o := spillOnly("a")
	for range 3 {
		o.send("x", rabbitmq.Delivery{})
	}
	o.admitted("x", copyFrom("a", 1))
	if !o.admits("x", copyFrom("a", 1)) {
		t.Fatal("a duplicate of a copy already admitted is a ledger replay and must be admitted")
	}
	o.admitted("x", copyFrom("a", 1))
	if o.channels["x"].expected != 2 {
		t.Fatalf("a duplicate must not move the expected number, got %d", o.channels["x"].expected)
	}
}

func TestOverflowOfTwoInstancesNeverWedgesOnForeignCopies(t *testing.T) {
	a, b := spillOnly("instance-a"), spillOnly("instance-b")
	a.send("x", rabbitmq.Delivery{})
	fromA := copyFrom("instance-a", (<-a.queue).seq)

	if !b.admits("x", fromA) {
		t.Fatal("a copy numbered by another instance is a new command where nothing is spilled")
	}
	b.send("x", rabbitmq.Delivery{})
	<-b.queue
	if b.admits("x", fromA) {
		t.Fatal("a foreign copy must queue behind this instance's own spill of the channel")
	}
	b.send("x", fromA)
	if renumbered := <-b.queue; renumbered.seq != 2 {
		t.Fatalf("a foreign copy takes this instance's next number, got %d", renumbered.seq)
	}
	for _, seq := range []int64{1, 2} {
		if !b.admits("x", copyFrom("instance-b", seq)) {
			t.Fatalf("own copy %d must be admitted in order", seq)
		}
		b.admitted("x", copyFrom("instance-b", seq))
	}
	if !b.admits("x", rabbitmq.Delivery{}) {
		t.Fatal("instance b must drain the channel")
	}
}

func TestOverflowClearsAChannelThatStalled(t *testing.T) {
	o := spillOnly("a")
	clock := time.Now()
	o.now = func() time.Time { return clock }
	o.send("x", rabbitmq.Delivery{})
	<-o.queue
	if o.admits("x", rabbitmq.Delivery{}) {
		t.Fatal("while the copy is out, new commands wait")
	}
	clock = clock.Add(overflowStallAfter + time.Second)
	if !o.admits("x", rabbitmq.Delivery{}) {
		t.Fatal("a channel whose copy never came back must stop holding its commands")
	}
	if _, tracked := o.channels["x"]; tracked {
		t.Fatal("the stalled channel's tracking must be cleared")
	}
}

type flakyTail struct {
	mu        sync.Mutex
	failures  int
	published []int64
}

func (f *flakyTail) publish(_ rabbitmq.Delivery, headers rabbitmq.Table) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failures > 0 {
		f.failures--
		return errors.New("confirm timed out")
	}
	f.published = append(f.published, headers[overflowSeqHeader].(int64))
	return nil
}

func TestOverflowRetriesAFailedCopyKeepingItsNumber(t *testing.T) {
	tail := &flakyTail{failures: 2}
	ack := &recordingAcknowledger{}
	o := &overflow{
		nonce: "a", publish: tail.publish, hold: 0, retryFirst: time.Millisecond, now: time.Now,
		channels: map[string]*channelSpill{}, queue: make(chan overflowItem, 4), stop: make(chan struct{}), done: make(chan struct{}),
	}
	go o.run()
	o.send("x", rabbitmq.Delivery{Acknowledger: ack, DeliveryTag: 1})
	o.send("x", rabbitmq.Delivery{Acknowledger: ack, DeliveryTag: 2})
	for {
		tail.mu.Lock()
		n := len(tail.published)
		tail.mu.Unlock()
		if n == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	o.close()

	if tail.published[0] != 1 || tail.published[1] != 2 {
		t.Fatalf("a copy that failed to publish must keep its number and its place, published %v", tail.published)
	}
	if len(ack.nacks) != 0 || len(ack.acks) != 2 || ack.acks[0] != 1 {
		t.Fatalf("each original is acked once its copy is confirmed, acks %v nacks %v", ack.acks, ack.nacks)
	}
	for _, seq := range tail.published {
		if !o.admits("x", copyFrom("a", seq)) {
			t.Fatalf("copy %d must be admitted when it comes back", seq)
		}
		o.admitted("x", copyFrom("a", seq))
	}
	if len(o.channels) != 0 {
		t.Fatalf("the channel must drain after the retried copies come back, state %v", o.channels)
	}
}
