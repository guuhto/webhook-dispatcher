package dispatcher

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guuhto/webhook-dispatcher/internal/event"
	"github.com/guuhto/webhook-dispatcher/internal/queue"
)

type fakeSleep struct {
	mu     sync.Mutex
	delays []time.Duration
	err    error
}

func (f *fakeSleep) Sleep(ctx context.Context, d time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delays = append(f.delays, d)
	return f.err
}

// statusServer responds with the listed statuses (repeating the last) and counts calls.
func statusServer(t *testing.T, statuses ...int) (*httptest.Server, *int32) {
	t.Helper()
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(atomic.AddInt32(&n, 1)) - 1
		if i >= len(statuses) {
			i = len(statuses) - 1
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q", ct)
		}
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		b, _ := io.ReadAll(r.Body)
		if string(b) != `{"a":1}` {
			t.Errorf("body = %s", b)
		}
		w.WriteHeader(statuses[i])
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

func newTestDispatcher(fs *fakeSleep, max int) *Dispatcher {
	return New(queue.NewMemory(), Config{
		BaseDelay: time.Second, MaxDelay: 5 * time.Second, MaxAttempts: max, Sleep: fs.Sleep,
	})
}

func newTestEvent(url string) event.Event {
	return event.Event{ID: "1", URL: url, Payload: []byte(`{"a":1}`)}
}

func TestSucceedsOnFirstAttempt(t *testing.T) {
	srv, n := statusServer(t, 200)
	fs := &fakeSleep{}
	if err := newTestDispatcher(fs, 3).Deliver(context.Background(), newTestEvent(srv.URL)); err != nil {
		t.Fatal(err)
	}
	if *n != 1 || len(fs.delays) != 0 {
		t.Fatalf("calls=%d sleeps=%v", *n, fs.delays)
	}
}

func TestSucceedsAfterFailures(t *testing.T) {
	srv, n := statusServer(t, 500, 429, 204)
	fs := &fakeSleep{}
	if err := newTestDispatcher(fs, 5).Deliver(context.Background(), newTestEvent(srv.URL)); err != nil {
		t.Fatal(err)
	}
	if *n != 3 {
		t.Fatalf("calls=%d", *n)
	}
	want := []time.Duration{time.Second, 2 * time.Second}
	if len(fs.delays) != 2 || fs.delays[0] != want[0] || fs.delays[1] != want[1] {
		t.Fatalf("sleeps=%v", fs.delays)
	}
}

func TestExhaustsAttempts(t *testing.T) {
	srv, n := statusServer(t, 503)
	fs := &fakeSleep{}
	err := newTestDispatcher(fs, 3).Deliver(context.Background(), newTestEvent(srv.URL))
	if !errors.Is(err, ErrMaxAttempts) {
		t.Fatalf("err=%v", err)
	}
	if *n != 3 || len(fs.delays) != 2 {
		t.Fatalf("calls=%d sleeps=%v", *n, fs.delays)
	}
}

func TestNetworkErrorRetries(t *testing.T) {
	srv, _ := statusServer(t, 200)
	url := srv.URL
	srv.Close()
	fs := &fakeSleep{}
	err := newTestDispatcher(fs, 2).Deliver(context.Background(), newTestEvent(url))
	if !errors.Is(err, ErrMaxAttempts) || len(fs.delays) != 1 {
		t.Fatalf("err=%v sleeps=%v", err, fs.delays)
	}
}

func TestClientErrorsDoNotRetry(t *testing.T) {
	for _, code := range []int{400, 404, 410} {
		srv, n := statusServer(t, code)
		fs := &fakeSleep{}
		err := newTestDispatcher(fs, 5).Deliver(context.Background(), newTestEvent(srv.URL))
		if err == nil || errors.Is(err, ErrMaxAttempts) {
			t.Fatalf("%d: err=%v", code, err)
		}
		if *n != 1 || len(fs.delays) != 0 {
			t.Fatalf("%d: calls=%d sleeps=%v", code, *n, fs.delays)
		}
	}
}

func TestBackoffAndCap(t *testing.T) {
	d := New(queue.NewMemory(), Config{BaseDelay: time.Second, MaxDelay: 10 * time.Second})
	want := []time.Duration{1, 2, 4, 8, 10, 10}
	for i, w := range want {
		if got := d.Backoff(i); got != w*time.Second {
			t.Errorf("Backoff(%d)=%v, want %v", i, got, w*time.Second)
		}
	}
	if got := d.Backoff(10000); got != 10*time.Second {
		t.Errorf("large Backoff = %v", got)
	}
}

func TestCancelDuringSleep(t *testing.T) {
	srv, n := statusServer(t, 500)
	fs := &fakeSleep{err: context.Canceled}
	err := newTestDispatcher(fs, 5).Deliver(context.Background(), newTestEvent(srv.URL))
	if !errors.Is(err, context.Canceled) || *n != 1 {
		t.Fatalf("err=%v calls=%d", err, *n)
	}
}

func TestRunStopsOnCtxCancel(t *testing.T) {
	srv, _ := statusServer(t, 200)
	q := queue.NewMemory()
	results := make(chan error, 1)
	d := New(q, Config{Sleep: (&fakeSleep{}).Sleep, OnResult: func(_ event.Event, err error) { results <- err }})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()

	if err := q.Enqueue(ctx, newTestEvent(srv.URL)); err != nil {
		t.Fatal(err)
	}
	if err := <-results; err != nil {
		t.Fatalf("delivery: %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run=%v", err)
	}
}
