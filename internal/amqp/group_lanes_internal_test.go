package amqp

import (
	"sync"
	"testing"

	rabbitmq "github.com/rabbitmq/amqp091-go"
)

type recordingAcknowledger struct {
	mu    sync.Mutex
	acks  []uint64
	nacks []uint64
}

func (r *recordingAcknowledger) Ack(tag uint64, _ bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.acks = append(r.acks, tag)
	return nil
}
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
