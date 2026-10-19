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

	// TODO: start the internal/dispatcher worker consuming q once t2 is merged.

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
}
