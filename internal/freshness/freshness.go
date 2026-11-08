// Package freshness tracks the lag between an event's claimed event time and
// the wall-clock time it was processed, flagging when that lag exceeds a
// configured threshold — the signature symptom of a stuck, backlogged, or
// silently-stalled upstream producer.
package freshness

import "time"

// Monitor tracks the maximum observed lag within the current window.
//
// The caller supplies "now" explicitly on every Observe call rather than the
// monitor reading a clock internally — this is deliberate: a monitor with
// its own private clock field is easy to forget to override in a test (or,
// worse, in a larger pipeline with its own injected clock), silently
// falling back to real wall-clock time and making threshold comparisons
// against a fabricated "current" time meaningless.
type Monitor struct {
	threshold time.Duration

	maxLag time.Duration
	count  int
}

// New returns a freshness monitor that flags lag beyond threshold.
func New(threshold time.Duration) *Monitor {
	return &Monitor{threshold: threshold}
}

// Observe records one event's claimed event time against the given current
// time.
func (m *Monitor) Observe(eventTime, now time.Time) {
	lag := now.Sub(eventTime)
	if lag < 0 {
		// Event claims to be from the future (clock skew, bad producer
		// clock). Not a freshness problem — don't let it suppress a
		// genuine max-lag reading by counting as a (negative) max.
		lag = 0
	}
	if lag > m.maxLag {
		m.maxLag = lag
	}
	m.count++
}

// WindowResult summarizes one window's worth of Observe calls.
type WindowResult struct {
	MaxLag   time.Duration
	Count    int
	Breached bool
}

// Flush returns the window's result and resets internal state for the next
// window.
func (m *Monitor) Flush() WindowResult {
	r := WindowResult{MaxLag: m.maxLag, Count: m.count, Breached: m.maxLag > m.threshold}
	m.maxLag = 0
	m.count = 0
	return r
}
