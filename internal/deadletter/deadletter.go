// Package deadletter keeps events whose delivery attempts were exhausted.
package deadletter

import (
	"sync"
	"time"

	"github.com/guuhto/webhook-dispatcher/internal/event"
)

// Entry is a dead-lettered event together with why and when it failed.
type Entry struct {
	Event    event.Event `json:"event"`
	Error    string      `json:"error"`
	FailedAt time.Time   `json:"failed_at"`
}

// Store is a thread-safe in-memory dead letter store.
type Store struct {
	mu      sync.Mutex
	entries []Entry
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{}
}

// Add appends an entry to the store.
func (s *Store) Add(e Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, e)
}

// List returns a copy of the stored entries in insertion order.
func (s *Store) List() []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Entry, len(s.entries))
	copy(out, s.entries)
	return out
}
