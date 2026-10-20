// Package queue defines the event queue and an in-memory implementation.
package queue

import (
	"context"
	"sync"

	"github.com/guuhto/webhook-dispatcher/internal/event"
)

// Queue is the queue of events to dispatch.
type Queue interface {
	// Enqueue adds the event; returns an error if ctx is already cancelled.
	Enqueue(ctx context.Context, e event.Event) error
	// Dequeue blocks until an item is available or ctx is cancelled.
	Dequeue(ctx context.Context) (event.Event, error)
}

// Memory is an unbounded in-memory FIFO queue, safe for concurrent use.
type Memory struct {
	mu     sync.Mutex
	items  []event.Event
	signal chan struct{} // closed and recreated on every Enqueue to wake up waiters
}

var _ Queue = (*Memory)(nil)

// NewMemory creates an empty in-memory queue.
func NewMemory() *Memory {
	return &Memory{signal: make(chan struct{})}
}

func (q *Memory) Enqueue(ctx context.Context, e event.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	q.items = append(q.items, e)
	close(q.signal)
	q.signal = make(chan struct{})
	q.mu.Unlock()
	return nil
}

func (q *Memory) Dequeue(ctx context.Context) (event.Event, error) {
	for {
		if err := ctx.Err(); err != nil {
			return event.Event{}, err
		}
		q.mu.Lock()
		if len(q.items) > 0 {
			e := q.items[0]
			q.items[0] = event.Event{}
			q.items = q.items[1:]
			q.mu.Unlock()
			return e, nil
		}
		sig := q.signal
		q.mu.Unlock()

		select {
		case <-sig:
		case <-ctx.Done():
			return event.Event{}, ctx.Err()
		}
	}
}
