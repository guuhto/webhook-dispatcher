// Command dispatcher receives webhooks over HTTP and delivers them in the background.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/guuhto/webhook-dispatcher/internal/config"
	"github.com/guuhto/webhook-dispatcher/internal/deadletter"
	"github.com/guuhto/webhook-dispatcher/internal/dispatcher"
	"github.com/guuhto/webhook-dispatcher/internal/event"
	"github.com/guuhto/webhook-dispatcher/internal/queue"
	"github.com/guuhto/webhook-dispatcher/internal/server"
)

func main() {
	def := ":8080"
	if v := os.Getenv("PORT_ADDR"); v != "" {
		def = v
	}
	addr := flag.String("addr", def, "listen address (env PORT_ADDR)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("config: backoff base=%v multiplier=%v max=%v, max attempts=%d",
		cfg.BackoffBase, cfg.BackoffMultiplier, cfg.BackoffMax, cfg.MaxAttempts)

	handler, d := build(cfg, nil)
	srv := &http.Server{Addr: *addr, Handler: handler}

	// Delivery worker: runs until the signal context is cancelled.
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		if err := d.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("worker exited with error: %v", err)
		}
	}()

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Printf("listening on %s", *addr)

	select {
	case err := <-errCh:
		log.Fatalf("server failed: %v", err)
	case <-ctx.Done():
	}

	log.Print("shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("shutdown error: %v", err)
	}
	// Wait for the worker to finish its in-flight delivery.
	<-workerDone
}

// build wires the queue, dead letter store, HTTP handler and dispatcher.
// sleep may be nil (real sleeping); tests inject a no-op.
func build(cfg config.Config, sleep func(context.Context, time.Duration) error) (http.Handler, *dispatcher.Dispatcher) {
	q := queue.NewMemory()
	store := deadletter.NewStore()
	d := dispatcher.New(q, dispatcher.Config{
		BaseDelay:   cfg.BackoffBase,
		Multiplier:  cfg.BackoffMultiplier,
		MaxDelay:    cfg.BackoffMax,
		MaxAttempts: cfg.MaxAttempts,
		Sleep:       sleep,
		OnResult: func(e event.Event, err error) {
			if err != nil {
				log.Printf("delivery of event %s failed: %v", e.ID, err)
				return
			}
			log.Printf("event %s delivered", e.ID)
		},
		OnDead: func(e event.Event, err error) {
			store.Add(deadletter.Entry{Event: e, Error: err.Error(), FailedAt: time.Now()})
		},
	})
	return server.New(q, store), d
}
