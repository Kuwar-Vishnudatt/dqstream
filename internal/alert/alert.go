// Package alert defines the alert shape, pluggable delivery sinks, and a
// cooldown wrapper that deduplicates repeated breaches of the same check so
// a sustained data-quality incident produces one alert per cooldown period,
// not one per window.
package alert

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Severity classifies how urgent an alert is.
type Severity string

const (
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// Alert is one data-quality finding ready for delivery.
type Alert struct {
	Time     time.Time      `json:"time"`
	Topic    string         `json:"topic"`
	Check    string         `json:"check"` // e.g. "freshness", "volume", "schema", "null_rate"
	Key      string         `json:"key"`   // dedup key, e.g. check+field
	Severity Severity       `json:"severity"`
	Message  string         `json:"message"`
	Details  map[string]any `json:"details,omitempty"`
}

// Sink delivers an alert somewhere. Implementations must be safe for
// concurrent use.
type Sink interface {
	Send(a Alert) error
}

// ConsoleSink writes one JSON line per alert to the given writer (typically
// os.Stdout). Useful for local running and for tests that want to assert on
// output.
type ConsoleSink struct {
	mu  sync.Mutex
	out *os.File
}

func NewConsoleSink(out *os.File) *ConsoleSink {
	return &ConsoleSink{out: out}
}

func (s *ConsoleSink) Send(a Alert) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	enc := json.NewEncoder(s.out)
	return enc.Encode(a)
}

// MemorySink collects alerts in memory — used by tests.
type MemorySink struct {
	mu     sync.Mutex
	alerts []Alert
}

func NewMemorySink() *MemorySink {
	return &MemorySink{}
}

func (s *MemorySink) Send(a Alert) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.alerts = append(s.alerts, a)
	return nil
}

func (s *MemorySink) Alerts() []Alert {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Alert, len(s.alerts))
	copy(out, s.alerts)
	return out
}

// Deduper wraps a Sink and suppresses repeat alerts that share the same
// (Topic, Check, Key) within a cooldown window, so a check that breaches on
// every window during a sustained incident still only pages a human once per
// cooldown — not once per window. The first alert for a given key after it
// has been quiet for a cooldown period always goes through.
type Deduper struct {
	inner    Sink
	cooldown time.Duration
	now      func() time.Time

	mu       sync.Mutex
	lastSent map[string]time.Time
}

func NewDeduper(inner Sink, cooldown time.Duration) *Deduper {
	return &Deduper{inner: inner, cooldown: cooldown, now: time.Now, lastSent: make(map[string]time.Time)}
}

func dedupKey(a Alert) string {
	return fmt.Sprintf("%s|%s|%s", a.Topic, a.Check, a.Key)
}

func (d *Deduper) Send(a Alert) error {
	d.mu.Lock()
	key := dedupKey(a)
	now := d.now()
	if last, ok := d.lastSent[key]; ok && now.Sub(last) < d.cooldown {
		d.mu.Unlock()
		return nil
	}
	d.lastSent[key] = now
	d.mu.Unlock()
	return d.inner.Send(a)
}
