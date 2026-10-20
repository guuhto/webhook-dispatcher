// Package dispatcher consumes the queue and delivers webhooks via HTTP POST,
// with retry and exponential backoff.
package dispatcher

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/guuhto/webhook-dispatcher/internal/event"
	"github.com/guuhto/webhook-dispatcher/internal/queue"
)

// Config configures the Dispatcher. Zero-valued fields get defaults.
type Config struct {
	BaseDelay   time.Duration // initial backoff delay (default 30s)
	Multiplier  float64       // backoff growth factor, must be >= 1 (default 2)
	MaxDelay    time.Duration // delay cap (default 1h)
	MaxAttempts int           // total attempts per event (default 8)
	HTTPClient  *http.Client  // default: client with a 10s timeout
	// Sleep waits for d or returns the ctx error; injectable for tests.
	Sleep func(ctx context.Context, d time.Duration) error
	// OnResult, if non-nil, is called after each event is processed
	// (nil err = delivered).
	OnResult func(e event.Event, err error)
	// OnDead, if non-nil, is called once when an event exhausts its retries
	// (ErrMaxAttempts only; not for permanent 4xx or ctx cancellation).
	// e.Attempts is the number of attempts made.
	OnDead func(e event.Event, err error)
}

// Dispatcher delivers events taken from a queue.
type Dispatcher struct {
	q   queue.Queue
	cfg Config
}

// ErrMaxAttempts indicates that all attempts were exhausted.
var ErrMaxAttempts = errors.New("dispatcher: attempts exhausted")

// New creates a Dispatcher, applying the configuration defaults.
func New(q queue.Queue, cfg Config) *Dispatcher {
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = 30 * time.Second
	}
	if cfg.Multiplier < 1 {
		cfg.Multiplier = 2
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = time.Hour
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 8
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	if cfg.Sleep == nil {
		cfg.Sleep = realSleep
	}
	return &Dispatcher{q: q, cfg: cfg}
}

func realSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Backoff returns the delay after the failure at index attempt (0 = first):
// BaseDelay * Multiplier^attempt, capped at MaxDelay.
func (d *Dispatcher) Backoff(attempt int) time.Duration {
	maxF := float64(d.cfg.MaxDelay)
	delay := float64(d.cfg.BaseDelay)
	for i := 0; i < attempt; i++ {
		if delay >= maxF || d.cfg.Multiplier == 1 {
			break
		}
		delay *= d.cfg.Multiplier // float math cannot overflow int64; capped below
	}
	if delay >= maxF {
		return d.cfg.MaxDelay
	}
	return time.Duration(delay)
}

// Run loops on Dequeue until ctx is cancelled; it returns the ctx error.
func (d *Dispatcher) Run(ctx context.Context) error {
	for {
		e, err := d.q.Dequeue(ctx)
		if err != nil {
			return err
		}
		err = d.Deliver(ctx, e)
		if d.cfg.OnResult != nil {
			d.cfg.OnResult(e, err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// Deliver delivers the event, retrying with backoff on transient failures
// (network error, 5xx, 429). Other 4xx responses are not retried.
func (d *Dispatcher) Deliver(ctx context.Context, e event.Event) error {
	var lastErr error
	for attempt := 0; attempt < d.cfg.MaxAttempts; attempt++ {
		e.Attempts = attempt + 1
		retry, err := d.post(ctx, e)
		if err == nil {
			return nil
		}
		lastErr = err
		if !retry || ctx.Err() != nil {
			return err
		}
		if attempt == d.cfg.MaxAttempts-1 {
			break
		}
		if serr := d.cfg.Sleep(ctx, d.Backoff(attempt)); serr != nil {
			return serr
		}
	}
	err := fmt.Errorf("%w (%d): %v", ErrMaxAttempts, d.cfg.MaxAttempts, lastErr)
	if d.cfg.OnDead != nil {
		d.cfg.OnDead(e, err)
	}
	return err
}

// post makes one attempt; retry says whether trying again is worthwhile.
func (d *Dispatcher) post(ctx context.Context, e event.Event) (retry bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.URL, bytes.NewReader(e.Payload))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.cfg.HTTPClient.Do(req)
	if err != nil {
		return true, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return false, nil
	case resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests:
		return true, fmt.Errorf("status %d", resp.StatusCode)
	default:
		return false, fmt.Errorf("status %d", resp.StatusCode)
	}
}
