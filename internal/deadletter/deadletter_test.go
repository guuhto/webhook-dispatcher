package deadletter

import (
	"sync"
	"testing"
	"time"

	"github.com/guuhto/webhook-dispatcher/internal/event"
)

func TestAddAndList(t *testing.T) {
	s := NewStore()
	if got := s.List(); len(got) != 0 {
		t.Fatalf("new store has %d entries", len(got))
	}
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.Add(Entry{Event: event.Event{ID: "a"}, Error: "boom", FailedAt: at})
	s.Add(Entry{Event: event.Event{ID: "b"}})
	got := s.List()
	if len(got) != 2 || got[0].Event.ID != "a" || got[0].Error != "boom" || !got[0].FailedAt.Equal(at) || got[1].Event.ID != "b" {
		t.Fatalf("unexpected list: %+v", got)
	}
}

func TestListReturnsCopy(t *testing.T) {
	s := NewStore()
	s.Add(Entry{Event: event.Event{ID: "a"}})
	got := s.List()
	got[0].Event.ID = "mutated"
	_ = append(got, Entry{})
	if s.List()[0].Event.ID != "a" || len(s.List()) != 1 {
		t.Fatal("store was affected by changes to the returned slice")
	}
}

func TestConcurrentAddList(t *testing.T) {
	s := NewStore()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); s.Add(Entry{Error: "x"}) }()
		go func() { defer wg.Done(); _ = s.List() }()
	}
	wg.Wait()
	if n := len(s.List()); n != 50 {
		t.Fatalf("len = %d, want 50", n)
	}
}
