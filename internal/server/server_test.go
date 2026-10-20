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
	rec := postWebhook(New(q), http.MethodPost, `{"url":"https://example.com/hook","payload":{"a":1}}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	var resp response
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp.ID == "" {
		t.Fatalf("response without ID: %q (%v)", rec.Body.String(), err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	e, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if e.ID != resp.ID || e.URL != "https://example.com/hook" || string(e.Payload) != `{"a":1}` {
		t.Fatalf("unexpected event: %+v", e)
	}
}

func TestRejectsInvalidInput(t *testing.T) {
	cases := map[string]string{
		"malformed json":   `{`,
		"empty":            ``,
		"missing url":      `{"payload":{}}`,
		"ftp scheme":       `{"url":"ftp://x.com","payload":{}}`,
		"url without host": `{"url":"http://","payload":{}}`,
		"relative url":     `{"url":"/hook","payload":{}}`,
		"missing payload":  `{"url":"http://x.com"}`,
		"non-string url":   `{"url":1,"payload":{}}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			q := queue.NewMemory()
			if rec := postWebhook(New(q), http.MethodPost, body); rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			if _, err := q.Dequeue(ctx); err == nil {
				t.Fatal("invalid event was enqueued")
			}
		})
	}
}

func TestMethodNotAllowed(t *testing.T) {
	for _, m := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		rec := postWebhook(New(queue.NewMemory()), m, "")
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s: status = %d, want 405", m, rec.Code)
		}
	}
}

func TestIgnoresClientSuppliedIDAndAttempts(t *testing.T) {
	q := queue.NewMemory()
	body := `{"id":"client-id","attempts":5,"url":"https://example.com/hook","payload":{"a":1}}`
	rec := postWebhook(New(q), http.MethodPost, body)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	var resp response
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp.ID == "" {
		t.Fatalf("response without ID: %q (%v)", rec.Body.String(), err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	e, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if e.Attempts != 0 {
		t.Fatalf("Attempts = %d, want 0", e.Attempts)
	}
	if e.ID == "client-id" || e.ID != resp.ID {
		t.Fatalf("ID = %q, want server-generated %q", e.ID, resp.ID)
	}
}
