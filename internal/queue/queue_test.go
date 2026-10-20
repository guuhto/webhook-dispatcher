package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/guuhto/webhook-dispatcher/internal/event"
)

func TestFIFO(t *testing.T) {
	q := NewMemory()
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := q.Enqueue(ctx, event.Event{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		e, err := q.Dequeue(ctx)
		if err != nil || e.ID != fmt.Sprint(i) {
			t.Fatalf("got %v, %v; want ID %d", e, err, i)
		}
	}
}

func TestDequeueBlocksUntilEnqueue(t *testing.T) {
	q := NewMemory()
	got := make(chan event.Event)
	go func() {
		e, _ := q.Dequeue(context.Background())
		got <- e
	}()
	select {
	case <-got:
		t.Fatal("Dequeue did not block")
	case <-time.After(50 * time.Millisecond):
	}
	_ = q.Enqueue(context.Background(), event.Event{ID: "a"})
	select {
	case e := <-got:
		if e.ID != "a" {
			t.Fatalf("ID = %q", e.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("Dequeue did not wake up")
	}
}

func TestDequeueReturnsCtxErrOnCancel(t *testing.T) {
	q := NewMemory()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error)
	go func() {
		_, err := q.Dequeue(ctx)
		errc <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Dequeue did not return")
	}
}

func TestEnqueueFailsOnCanceledCtx(t *testing.T) {
	q := NewMemory()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := q.Enqueue(ctx, event.Event{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestConcurrentProducersAndConsumers(t *testing.T) {
	q := NewMemory()
	ctx := context.Background()
	const producers, perProducer, consumers = 8, 100, 4
	total := producers * perProducer

	var pw sync.WaitGroup
	for p := 0; p < producers; p++ {
		pw.Add(1)
		go func(p int) {
			defer pw.Done()
			for i := 0; i < perProducer; i++ {
				_ = q.Enqueue(ctx, event.Event{ID: fmt.Sprintf("%d-%d", p, i)})
			}
		}(p)
	}

	var mu sync.Mutex
	seen := map[string]int{}
	cctx, cancel := context.WithCancel(ctx)
	var cw sync.WaitGroup
	for c := 0; c < consumers; c++ {
		cw.Add(1)
		go func() {
			defer cw.Done()
			for {
				e, err := q.Dequeue(cctx)
				if err != nil {
					return
				}
				mu.Lock()
				seen[e.ID]++
				done := len(seen) == total
				mu.Unlock()
				if done {
					cancel()
				}
			}
		}()
	}
	pw.Wait()
	cw.Wait()
	cancel()

	if len(seen) != total {
		t.Fatalf("received %d unique, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("%s delivered %d times", id, n)
		}
	}
}

func TestDequeueReturnsCtxErrOnDeadline(t *testing.T) {
	q := NewMemory()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := q.Dequeue(ctx)
	if err != ctx.Err() || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, ctx.Err() = %v", err, ctx.Err())
	}
}

func TestDequeueCanceledCtxDoesNotConsumeItem(t *testing.T) {
	q := NewMemory()
	_ = q.Enqueue(context.Background(), event.Event{ID: "x"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := q.Dequeue(ctx); err != ctx.Err() {
		t.Fatalf("err = %v, want %v", err, ctx.Err())
	}
	e, err := q.Dequeue(context.Background())
	if err != nil || e.ID != "x" {
		t.Fatalf("item lost: %v, %v", e, err)
	}
}
