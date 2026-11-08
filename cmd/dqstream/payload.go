package main

import (
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/Kuwar-Vishnudatt/dqstream/internal/event"
)

func marshalEnvelope(env event.Envelope) ([]byte, error) {
	return json.Marshal(env)
}

// syntheticPayload generates a plausible "order" event. Once injecting is
// true, it occasionally mutates the shape of the event to exercise each
// check: a missing required field, a type change, a new unexpected field,
// or a null spike on "status".
func syntheticPayload(rng *rand.Rand, n int, injecting bool) json.RawMessage {
	fields := map[string]any{
		"order_id": fmt.Sprintf("ord-%d", n),
		"status":   []string{"placed", "shipped", "delivered"}[rng.Intn(3)],
		"amount":   float64(rng.Intn(10000)) / 100,
	}

	if injecting {
		switch r := rng.Float64(); {
		case r < 0.1:
			delete(fields, "amount") // missing required field
		case r < 0.2:
			fields["amount"] = fmt.Sprintf("%.2f", rng.Float64()*100) // type change: number -> string
		case r < 0.3:
			fields["discount_code"] = "FLASH25" // new field
		case r < 0.45:
			fields["status"] = nil // null-rate spike
		}
	}

	data, _ := json.Marshal(fields)
	return data
}
