package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guuhto/webhook-dispatcher/internal/config"
)

func TestExhaustedEventAppearsInDeadLetters(t *testing.T) {
	var hits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer target.Close()

	var sleeps atomic.Int32
	sleep := func(context.Context, time.Duration) error { sleeps.Add(1); return nil }
	cfg := config.Config{BackoffBase: time.Minute, BackoffMultiplier: 2, BackoffMax: time.Hour, MaxAttempts: 3}
	handler, d := build(cfg, sleep)
	api := httptest.NewServer(handler)
	defer api.Close()

	body := `{"url":"` + target.URL + `","payload":{"a":1}}`
	resp, err := http.Post(api.URL+"/webhooks", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx)

	deadline := time.Now().Add(5 * time.Second)
	for {
		r, err := http.Get(api.URL + "/webhooks/dead")
		if err != nil {
			t.Fatal(err)
		}
		var entries []struct {
			Event struct {
				URL      string `json:"url"`
				Attempts int    `json:"attempts"`
			} `json:"event"`
			Error    string `json:"error"`
			FailedAt string `json:"failed_at"`
		}
		err = json.NewDecoder(r.Body).Decode(&entries)
		r.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 1 {
			e := entries[0]
			if e.Event.URL != target.URL || e.Event.Attempts != 3 || e.Error == "" || e.FailedAt == "" {
				t.Fatalf("unexpected entry: %+v", e)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("event never reached the dead letter list")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if hits.Load() != 3 || sleeps.Load() != 2 {
		t.Fatalf("hits=%d sleeps=%d, want 3 and 2", hits.Load(), sleeps.Load())
	}
}
