package config

import (
	"testing"
	"time"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoad(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		want    Config
		wantErr bool
	}{
		{"defaults", nil, Config{30 * time.Second, 2, time.Hour, 8}, false},
		{"all set", map[string]string{"BACKOFF_BASE": "1m", "BACKOFF_MULTIPLIER": "1.5", "BACKOFF_MAX": "2h", "MAX_ATTEMPTS": "3"}, Config{time.Minute, 1.5, 2 * time.Hour, 3}, false},
		{"bad duration", map[string]string{"BACKOFF_BASE": "abc"}, Config{}, true},
		{"no unit", map[string]string{"BACKOFF_BASE": "30"}, Config{}, true},
		{"zero base", map[string]string{"BACKOFF_BASE": "0s"}, Config{}, true},
		{"negative max", map[string]string{"BACKOFF_MAX": "-1h"}, Config{}, true},
		{"mult below 1", map[string]string{"BACKOFF_MULTIPLIER": "0.5"}, Config{}, true},
		{"mult nan", map[string]string{"BACKOFF_MULTIPLIER": "NaN"}, Config{}, true},
		{"mult junk", map[string]string{"BACKOFF_MULTIPLIER": "x"}, Config{}, true},
		{"attempts zero", map[string]string{"MAX_ATTEMPTS": "0"}, Config{}, true},
		{"attempts junk", map[string]string{"MAX_ATTEMPTS": "1.5"}, Config{}, true},
		{"max below base", map[string]string{"BACKOFF_BASE": "2h"}, Config{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Load(env(tt.env))
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}
