// Package event defines the webhook event that travels through the queue.
package event

import "encoding/json"

// Event represents a webhook to be delivered.
type Event struct {
	ID       string          `json:"id"`
	URL      string          `json:"url"`
	Payload  json.RawMessage `json:"payload"`
	Attempts int             `json:"attempts"`
}
