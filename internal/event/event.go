// Package event define o evento de webhook que trafega pela fila.
package event

import "encoding/json"

// Event representa um webhook a ser entregue.
type Event struct {
	ID       string          `json:"id"`
	URL      string          `json:"url"`
	Payload  json.RawMessage `json:"payload"`
	Attempts int             `json:"attempts"`
}
