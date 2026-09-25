package amqp

import (
	"errors"
	"fmt"
	"slices"
	"sync"

	rabbitmq "github.com/rabbitmq/amqp091-go"
)

const (
	DefaultGroupPrefetch    = 64
	DefaultGroupLaneBacklog = 16
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
		spill   = newOverflow(newQueueTail(c.groupCh, GatewayGroupQueue), c.groupPrefetch)
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
