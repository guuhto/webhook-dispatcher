// Package dispatcher consome a fila e entrega os webhooks via HTTP POST,
// com retry e backoff exponencial.
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

// Config configura o Dispatcher. Campos zerados recebem valores padrão.
type Config struct {
	BaseDelay   time.Duration // atraso inicial do backoff (padrão 1s)
	MaxDelay    time.Duration // teto do atraso (padrão 1min)
	MaxAttempts int           // total de tentativas por evento (padrão 5)
	HTTPClient  *http.Client  // padrão: cliente com timeout de 10s
	// Sleep espera d ou retorna o erro de ctx; injetável para testes.
	Sleep func(ctx context.Context, d time.Duration) error
	// OnResult, se não nulo, é chamado ao fim do processamento de cada evento
	// (err nil = entregue).
	OnResult func(e event.Event, err error)
}

// Dispatcher entrega eventos retirados de uma fila.
type Dispatcher struct {
	q   queue.Queue
	cfg Config
}

// ErrMaxAttempts indica que as tentativas se esgotaram.
var ErrMaxAttempts = errors.New("dispatcher: tentativas esgotadas")

// New cria um Dispatcher aplicando os padrões da configuração.
func New(q queue.Queue, cfg Config) *Dispatcher {
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = time.Second
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = time.Minute
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
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

// Backoff retorna o atraso após a falha de índice attempt (0 = primeira):
// BaseDelay * 2^attempt, limitado a MaxDelay.
func (d *Dispatcher) Backoff(attempt int) time.Duration {
	delay := d.cfg.BaseDelay
	for i := 0; i < attempt; i++ {
		if delay >= d.cfg.MaxDelay/2 { // próximo dobro passaria do teto (e evita overflow)
			return d.cfg.MaxDelay
		}
		delay *= 2
	}
	if delay > d.cfg.MaxDelay {
		return d.cfg.MaxDelay
	}
	return delay
}

// Run faz loop em Dequeue até ctx ser cancelado; retorna o erro de ctx.
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

// Deliver entrega o evento, repetindo com backoff em falhas transitórias
// (erro de rede, 5xx, 429). Outros 4xx não são repetidos.
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
	return fmt.Errorf("%w (%d): %v", ErrMaxAttempts, d.cfg.MaxAttempts, lastErr)
}

// post faz uma tentativa; retry indica se vale tentar de novo.
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
