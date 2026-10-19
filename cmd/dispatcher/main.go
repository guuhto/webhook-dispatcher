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
	addr := flag.String("addr", def, "endereço de escuta (env PORT_ADDR)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	q := queue.NewMemory()
	srv := &http.Server{Addr: *addr, Handler: server.New(q)}

	// Delivery worker: runs until the signal context is cancelled.
	d := dispatcher.New(q, dispatcher.Config{OnResult: func(e event.Event, err error) {
		if err != nil {
			log.Printf("entrega do evento %s falhou: %v", e.ID, err)
			return
		}
		log.Printf("evento %s entregue", e.ID)
	}})
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		if err := d.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("worker encerrou com erro: %v", err)
		}
	}()

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Printf("escutando em %s", *addr)

	select {
	case err := <-errCh:
		log.Fatalf("servidor falhou: %v", err)
	case <-ctx.Done():
	}

	log.Print("encerrando...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("erro no shutdown: %v", err)
	}
	// Wait for the worker to finish its in-flight delivery.
	<-workerDone
}
