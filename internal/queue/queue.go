// Package queue define a fila de eventos e uma implementação em memória.
package queue

import (
	"context"
	"sync"

	"github.com/guuhto/webhook-dispatcher/internal/event"
)

// Queue é a fila de eventos a despachar.
type Queue interface {
	// Enqueue adiciona o evento; retorna erro se ctx já estiver cancelado.
	Enqueue(ctx context.Context, e event.Event) error
	// Dequeue bloqueia até haver um item ou ctx ser cancelado.
	Dequeue(ctx context.Context) (event.Event, error)
}

// Memory é uma fila FIFO ilimitada em memória, segura para concorrência.
type Memory struct {
	mu     sync.Mutex
	items  []event.Event
	signal chan struct{} // fechado e recriado a cada Enqueue para acordar quem espera
}

var _ Queue = (*Memory)(nil)

// NewMemory cria uma fila em memória vazia.
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
