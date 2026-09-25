package amqp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync/atomic"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"
)

const (
	overflowSeqHeader   = "x-gateway-overflow"
	overflowNonceHeader = "x-gateway-overflow-nonce"
	overflowHold        = time.Second
	overflowStallAfter  = 30 * time.Second
	overflowRetryFirst  = 200 * time.Millisecond
	overflowRetryMax    = 5 * time.Second
	overflowPublishIn   = 5 * time.Second
)

var errCopyReturned = errors.New("amqp: broker returned the overflow copy as unroutable")

type tailPublisher func(d rabbitmq.Delivery, headers rabbitmq.Table) error

type queueTail struct {
	ch      *rabbitmq.Channel
	queue   string
	returns chan rabbitmq.Return
}

func newQueueTail(ch *rabbitmq.Channel, queue string) tailPublisher {
	t := &queueTail{ch: ch, queue: queue, returns: ch.NotifyReturn(make(chan rabbitmq.Return, 1))}
	return t.publish
}

func drainReturns(returns <-chan rabbitmq.Return) {
	for {
		select {
		case _, ok := <-returns:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

func copyReturned(returns <-chan rabbitmq.Return) bool {
	select {
	case _, ok := <-returns:
		return ok
	default:
		return false
	}
}

func (t *queueTail) publish(d rabbitmq.Delivery, headers rabbitmq.Table) error {
	drainReturns(t.returns)
	ctx, cancel := context.WithTimeout(context.Background(), overflowPublishIn)
	defer cancel()
	confirm, err := t.ch.PublishWithDeferredConfirmWithContext(ctx, "", t.queue, true, false, rabbitmq.Publishing{
		Headers:      headers,
		ContentType:  d.ContentType,
		DeliveryMode: rabbitmq.Persistent,
		MessageId:    d.MessageId,
		Timestamp:    d.Timestamp,
		Body:         d.Body,
	})
	if err != nil {
		return err
	}
	acked, err := confirm.WaitContext(ctx)
	if err != nil {
		return err
	}
	if copyReturned(t.returns) {
		return errCopyReturned
	}
	if !acked {
		return errors.New("amqp: broker refused the overflow copy")
	}
	return nil
}

type overflowItem struct {
	delivery rabbitmq.Delivery
	seq      int64
	at       time.Time
	spill    *channelSpill
}

type channelSpill struct {
	next         int64
	expected     int64
	lastProgress time.Time
	unpublished  atomic.Int64
}

type overflow struct {
	nonce      string
	publish    tailPublisher
	hold       time.Duration
	retryFirst time.Duration
	now        func() time.Time
	channels   map[string]*channelSpill
	queue      chan overflowItem
	stop       chan struct{}
	done       chan struct{}
}

func newOverflow(publish tailPublisher, capacity int) *overflow {
	o := &overflow{
		nonce:      newNonce(),
		publish:    publish,
		hold:       overflowHold,
		retryFirst: overflowRetryFirst,
		now:        time.Now,
		channels:   make(map[string]*channelSpill),
		queue:      make(chan overflowItem, capacity),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	go o.run()
	return o
}

func newNonce() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (o *overflow) copyNumber(d rabbitmq.Delivery) (int64, bool) {
	if nonce, _ := d.Headers[overflowNonceHeader].(string); nonce != o.nonce {
		return 0, false
	}
	seq, ok := d.Headers[overflowSeqHeader].(int64)
	return seq, ok
}

func (o *overflow) admits(channelID string, d rabbitmq.Delivery) bool {
	spill := o.channels[channelID]
	if spill == nil {
		return true
	}
	if spill.unpublished.Load() == 0 && o.now().Sub(spill.lastProgress) > overflowStallAfter {
		delete(o.channels, channelID)
		return true
	}
	seq, copied := o.copyNumber(d)
	return copied && seq <= spill.expected
}

func (o *overflow) admitted(channelID string, d rabbitmq.Delivery) {
	spill := o.channels[channelID]
	seq, copied := o.copyNumber(d)
	if spill == nil || !copied {
		return
	}
	spill.lastProgress = o.now()
	if seq != spill.expected {
		return
	}
	spill.expected++
	if spill.expected == spill.next {
		delete(o.channels, channelID)
	}
}

func (o *overflow) send(channelID string, d rabbitmq.Delivery) {
	seq, copied := o.copyNumber(d)
	spill := o.channels[channelID]
	if spill == nil {
		spill = &channelSpill{next: 1, expected: 1, lastProgress: o.now()}
		o.channels[channelID] = spill
		copied = false
	}
	if !copied {
		seq = spill.next
		spill.next++
	}
	spill.unpublished.Add(1)
	o.queue <- overflowItem{delivery: d, seq: seq, at: o.now(), spill: spill}
}

func (o *overflow) close() {
	close(o.stop)
	close(o.queue)
	<-o.done
}

func (o *overflow) run() {
	defer close(o.done)
	for item := range o.queue {
		o.moveToTail(item)
		item.spill.unpublished.Add(-1)
	}
}

func (o *overflow) stopped() bool {
	select {
	case <-o.stop:
		return true
	default:
		return false
	}
}

func (o *overflow) waitOrStop(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-o.stop:
		return false
	case <-timer.C:
		return true
	}
}

func (o *overflow) moveToTail(item overflowItem) {
	if !o.waitOrStop(time.Until(item.at.Add(o.hold))) {
		_ = item.delivery.Nack(false, true)
		return
	}
	headers := rabbitmq.Table{}
	for k, v := range item.delivery.Headers {
		headers[k] = v
	}
	headers[overflowSeqHeader] = item.seq
	headers[overflowNonceHeader] = o.nonce
	wait := o.retryFirst
	for {
		if o.stopped() {
			_ = item.delivery.Nack(false, true)
			return
		}
		err := o.publish(item.delivery, headers)
		if err == nil {
			_ = item.delivery.Ack(false)
			return
		}
		if errors.Is(err, rabbitmq.ErrClosed) {
			_ = item.delivery.Nack(false, true)
			return
		}
		if !o.waitOrStop(wait) {
			_ = item.delivery.Nack(false, true)
			return
		}
		wait = min(wait*2, overflowRetryMax)
	}
}
