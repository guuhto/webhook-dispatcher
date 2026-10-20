// Package config loads the dispatcher configuration from environment variables.
package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Config holds the retry/backoff settings.
type Config struct {
	BackoffBase       time.Duration
	BackoffMultiplier float64
	BackoffMax        time.Duration
	MaxAttempts       int
}

// Defaults for unset variables.
const (
	DefaultBase        = 30 * time.Second
	DefaultMultiplier  = 2.0
	DefaultMax         = time.Hour
	DefaultMaxAttempts = 8
)

// Load reads BACKOFF_BASE, BACKOFF_MULTIPLIER, BACKOFF_MAX and MAX_ATTEMPTS
// using getenv. Unset or empty variables take their defaults.
func Load(getenv func(string) string) (Config, error) {
	c := Config{
		BackoffBase:       DefaultBase,
		BackoffMultiplier: DefaultMultiplier,
		BackoffMax:        DefaultMax,
		MaxAttempts:       DefaultMaxAttempts,
	}
	var err error
	if c.BackoffBase, err = duration(getenv, "BACKOFF_BASE", c.BackoffBase); err != nil {
		return Config{}, err
	}
	if c.BackoffMax, err = duration(getenv, "BACKOFF_MAX", c.BackoffMax); err != nil {
		return Config{}, err
	}
	if v := strings.TrimSpace(getenv("BACKOFF_MULTIPLIER")); v != "" {
		f, perr := strconv.ParseFloat(v, 64)
		if perr != nil || f != f || f < 1 || f > 1e9 {
			return Config{}, fmt.Errorf("config: BACKOFF_MULTIPLIER=%q: must be a number >= 1", v)
		}
		c.BackoffMultiplier = f
	}
	if v := strings.TrimSpace(getenv("MAX_ATTEMPTS")); v != "" {
		n, perr := strconv.Atoi(v)
		if perr != nil || n <= 0 {
			return Config{}, fmt.Errorf("config: MAX_ATTEMPTS=%q: must be a positive integer", v)
		}
		c.MaxAttempts = n
	}
	if c.BackoffMax < c.BackoffBase {
		return Config{}, fmt.Errorf("config: BACKOFF_MAX (%v) must be >= BACKOFF_BASE (%v)", c.BackoffMax, c.BackoffBase)
	}
	return c, nil
}

func duration(getenv func(string) string, key string, def time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(getenv(key))
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q: invalid duration (e.g. 30s, 1h)", key, v)
	}
	if d <= 0 {
		return 0, fmt.Errorf("config: %s=%q: must be positive", key, v)
	}
	return d, nil
}
