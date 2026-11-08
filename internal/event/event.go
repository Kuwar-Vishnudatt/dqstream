// Package event defines the wire envelope the monitor consumes from Kafka
// and the decoded in-memory representation used by every check.
package event

import (
	"encoding/json"
	"fmt"
	"time"
)

// Envelope is the on-the-wire JSON shape producers publish. EventTime is the
// producer's claim of when the event occurred (distinct from Kafka's own
// message timestamp, which is closer to ingest/send time) — freshness checks
// use this field specifically because it's the one most likely to lag
// silently when an upstream producer gets stuck, retried, or backfilled.
type Envelope struct {
	EventTime time.Time       `json:"event_time"`
	Payload   json.RawMessage `json:"payload"`
}

// Record is a decoded event ready for the check pipeline.
type Record struct {
	EventTime time.Time
	Fields    map[string]any
}

// Decode parses a raw Kafka message value into a Record. The payload must be
// a JSON object (not an array or scalar) since every check keys off field
// names.
func Decode(raw []byte) (Record, error) {
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return Record{}, fmt.Errorf("decode envelope: %w", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(env.Payload, &fields); err != nil {
		return Record{}, fmt.Errorf("decode payload (must be a JSON object): %w", err)
	}
	return Record{EventTime: env.EventTime, Fields: fields}, nil
}
