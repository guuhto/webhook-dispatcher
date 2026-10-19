package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/guuhto/webhook-dispatcher/internal/queue"
)

func postWebhook(h http.Handler, method, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/webhooks", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAcceptsValidWebhook(t *testing.T) {
	q := queue.NewMemory()
	rec := postWebhook(New(q), http.MethodPost, `{"url":"https://exemplo.com/hook","payload":{"a":1}}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, quer 202", rec.Code)
	}
	var resp response
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp.ID == "" {
		t.Fatalf("resposta sem ID: %q (%v)", rec.Body.String(), err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	e, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if e.ID != resp.ID || e.URL != "https://exemplo.com/hook" || string(e.Payload) != `{"a":1}` {
		t.Fatalf("evento inesperado: %+v", e)
	}
}

func TestRejectsInvalidInput(t *testing.T) {
	cases := map[string]string{
		"json quebrado":  `{`,
		"vazio":          ``,
		"sem url":        `{"payload":{}}`,
		"esquema ftp":    `{"url":"ftp://x.com","payload":{}}`,
		"url sem host":   `{"url":"http://","payload":{}}`,
		"url relativa":   `{"url":"/hook","payload":{}}`,
		"sem payload":    `{"url":"http://x.com"}`,
		"url não string": `{"url":1,"payload":{}}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			q := queue.NewMemory()
			if rec := postWebhook(New(q), http.MethodPost, body); rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, quer 400", rec.Code)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			if _, err := q.Dequeue(ctx); err == nil {
				t.Fatal("evento inválido foi enfileirado")
			}
		})
	}
}

func TestMethodNotAllowed(t *testing.T) {
	for _, m := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		rec := postWebhook(New(queue.NewMemory()), m, "")
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s: status = %d, quer 405", m, rec.Code)
		}
	}
}
