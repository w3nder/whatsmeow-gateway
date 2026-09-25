package amqp

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"
)

const (
	DefaultGroupPrefetch    = 64
	DefaultGroupLaneBacklog = 16

	overflowHeader    = "x-gateway-overflow"
	overflowHold      = time.Second
	overflowPublishIn = 5 * time.Second
)

func orDefault(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

type laneJob struct {
	channelID string
	handle    func() error
}

func (c *Consumer) consumePerChannel(queue string, deliveries <-chan rabbitmq.Delivery, decode func(rabbitmq.Delivery) (laneJob, error)) {
	defer c.wg.Done()
	var (
		mu      sync.Mutex
		requeue []rabbitmq.Delivery
		lanes   = newLanes(c.groupLaneBacklog)
		spill   = newOverflow(c.groupCh, c.groupPrefetch)
	)
	for d := range deliveries {
		job, err := decode(d)
		if err != nil {
			settle(d, err)
			continue
		}
		handle := func() {
			err := job.handle()
			if errors.Is(err, ErrRequeue) {
				mu.Lock()
				requeue = append(requeue, d)
				mu.Unlock()
				return
			}
			settle(d, err)
		}
		if spill.admits(job.channelID, d) && lanes.run(job.channelID, handle) {
			spill.admitted(job.channelID, d)
			continue
		}
		spill.send(job.channelID, d)
	}
	lanes.wait()
	spill.close()
	requeueInOrder(requeue)
	c.reportFailure(fmt.Errorf("amqp: %s consumer stopped: broker closed the delivery channel", queue))
}

func requeueInOrder(deliveries []rabbitmq.Delivery) {
	slices.SortFunc(deliveries, func(a, b rabbitmq.Delivery) int {
		switch {
		case a.DeliveryTag < b.DeliveryTag:
			return -1
		case a.DeliveryTag > b.DeliveryTag:
			return 1
		default:
			return 0
		}
	})
	for _, d := range deliveries {
		_ = d.Nack(false, true)
	}
}

type lanes struct {
	backlog int
	mu      sync.Mutex
	pending map[string][]func()
	workers sync.WaitGroup
}

func newLanes(backlog int) *lanes {
	return &lanes{backlog: backlog, pending: make(map[string][]func())}
}

func (l *lanes) run(key string, job func()) bool {
	l.mu.Lock()
	queued, busy := l.pending[key]
	if len(queued) >= l.backlog {
		l.mu.Unlock()
		return false
	}
	l.pending[key] = append(queued, job)
	l.mu.Unlock()
	if !busy {
		l.workers.Add(1)
		go l.drain(key)
	}
	return true
}

func (l *lanes) drain(key string) {
	defer l.workers.Done()
	for {
		l.mu.Lock()
		queued := l.pending[key]
		if len(queued) == 0 {
			delete(l.pending, key)
			l.mu.Unlock()
			return
		}
		job := queued[0]
		l.pending[key] = queued[1:]
		l.mu.Unlock()
		job()
	}
}

func (l *lanes) wait() {
	l.workers.Wait()
}

type overflowItem struct {
	delivery rabbitmq.Delivery
	seq      int64
	at       time.Time
}

type overflow struct {
	ch       *rabbitmq.Channel
	next     map[string]int64
	expected map[string]int64
	queue    chan overflowItem
	done     chan struct{}
}

func newOverflow(ch *rabbitmq.Channel, capacity int) *overflow {
	o := &overflow{ch: ch, next: make(map[string]int64), expected: make(map[string]int64), queue: make(chan overflowItem, capacity), done: make(chan struct{})}
	go o.run()
	return o
}

func overflowSeq(d rabbitmq.Delivery) (int64, bool) {
	seq, copied := d.Headers[overflowHeader].(int64)
	return seq, copied
}

func (o *overflow) admits(channelID string, d rabbitmq.Delivery) bool {
	_, tracked := o.next[channelID]
	if !tracked {
		return true
	}
	seq, copied := overflowSeq(d)
	return copied && seq == o.expected[channelID]
}

func (o *overflow) admitted(channelID string, d rabbitmq.Delivery) {
	seq, copied := overflowSeq(d)
	if _, tracked := o.next[channelID]; !copied || !tracked || seq != o.expected[channelID] {
		return
	}
	o.expected[channelID]++
	if o.expected[channelID] == o.next[channelID] {
		delete(o.expected, channelID)
		delete(o.next, channelID)
	}
}

func (o *overflow) send(channelID string, d rabbitmq.Delivery) {
	seq, copied := overflowSeq(d)
	if _, tracked := o.next[channelID]; !tracked {
		o.expected[channelID] = 1
		o.next[channelID] = 1
		copied = false
	}
	if !copied {
		seq = o.next[channelID]
		o.next[channelID]++
	}
	o.queue <- overflowItem{delivery: d, seq: seq, at: time.Now()}
}

func (o *overflow) close() {
	close(o.queue)
	<-o.done
}

func (o *overflow) run() {
	defer close(o.done)
	for item := range o.queue {
		time.Sleep(time.Until(item.at.Add(overflowHold)))
		if err := o.toTail(item.delivery, item.seq); err != nil {
			_ = item.delivery.Nack(false, true)
			continue
		}
		_ = item.delivery.Ack(false)
	}
}

func (o *overflow) toTail(d rabbitmq.Delivery, seq int64) error {
	headers := rabbitmq.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers[overflowHeader] = seq
	ctx, cancel := context.WithTimeout(context.Background(), overflowPublishIn)
	defer cancel()
	confirm, err := o.ch.PublishWithDeferredConfirmWithContext(ctx, d.Exchange, d.RoutingKey, false, false, rabbitmq.Publishing{
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
	if !acked {
		return errors.New("amqp: broker refused the overflow copy")
	}
	return nil
}
