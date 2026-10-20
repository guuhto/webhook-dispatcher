// Package server exposes the HTTP API that receives webhooks and enqueues them.
package server

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/guuhto/webhook-dispatcher/internal/deadletter"
	"github.com/guuhto/webhook-dispatcher/internal/event"
	"github.com/guuhto/webhook-dispatcher/internal/queue"
)

const maxBodyBytes = 1 << 20 // 1 MiB

// request is the public API contract: clients may only set url and payload.
// Any other field (such as id or attempts) is ignored by the decoder; the
// server builds the internal event.Event itself.
type request struct {
	URL     string          `json:"url"`
	Payload json.RawMessage `json:"payload"`
}

// response is returned to the client once the webhook is accepted.
type response struct {
	ID string `json:"id"`
}

// errorResponse is the body returned on failures.
type errorResponse struct {
	Error string `json:"error"`
}

// DeadLetters is the read side of the dead letter store.
type DeadLetters interface {
	List() []deadletter.Entry
}

// New returns the HTTP handler serving POST /webhooks and GET /webhooks/dead.
func New(q queue.Queue, dead DeadLetters) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/webhooks", func(w http.ResponseWriter, r *http.Request) {
		handleWebhook(q, w, r)
	})
	mux.HandleFunc("/webhooks/dead", func(w http.ResponseWriter, r *http.Request) {
		handleDead(dead, w, r)
	})
	return mux
}

func handleDead(dead DeadLetters, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{"method not allowed"})
		return
	}
	entries := dead.List()
	if entries == nil {
		entries = []deadletter.Entry{} // encode as [] rather than null
	}
	writeJSON(w, http.StatusOK, entries)
}

func handleWebhook(q queue.Queue, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{"method not allowed"})
		return
	}

	var req request
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{"invalid JSON"})
		return
	}
	if !validURL(req.URL) {
		writeJSON(w, http.StatusBadRequest, errorResponse{"url must be http or https"})
		return
	}
	if len(req.Payload) == 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{"payload is required"})
		return
	}

	id, err := newID()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{"failed to generate ID"})
		return
	}
	// The event is built server-side: the ID is generated here and Attempts
	// always starts at 0, regardless of what the client sent.
	e := event.Event{ID: id, URL: req.URL, Payload: req.Payload, Attempts: 0}
	if err := q.Enqueue(r.Context(), e); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{"could not enqueue"})
		return
	}
	writeJSON(w, http.StatusAccepted, response{ID: id})
}

func validURL(s string) bool {
	u, err := url.Parse(s)
	if err != nil || u.Host == "" {
		return false
	}
	return u.Scheme == "http" || u.Scheme == "https"
}

func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
