package freshness

import (
	"testing"
	"time"
)

func TestBreachesWhenLagExceedsThreshold(t *testing.T) {
	fixedNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	m := New(5 * time.Second)

	m.Observe(fixedNow.Add(-10*time.Second), fixedNow)
	r := m.Flush()
	if !r.Breached {
		t.Fatal("expected breach for 10s lag against 5s threshold")
	}
	if r.MaxLag != 10*time.Second {
		t.Fatalf("expected max lag 10s, got %v", r.MaxLag)
	}
}

func TestNoBreachWithinThreshold(t *testing.T) {
	fixedNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	m := New(5 * time.Second)

	m.Observe(fixedNow.Add(-1*time.Second), fixedNow)
	r := m.Flush()
	if r.Breached {
		t.Fatal("expected no breach for 1s lag against 5s threshold")
	}
}

func TestFutureEventTimeDoesNotProduceNegativeLag(t *testing.T) {
	fixedNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	m := New(5 * time.Second)

	m.Observe(fixedNow.Add(2*time.Second), fixedNow) // "from the future"
	r := m.Flush()
	if r.MaxLag != 0 {
		t.Fatalf("expected clamped lag of 0, got %v", r.MaxLag)
	}
	if r.Breached {
		t.Fatal("future-dated event must not trigger a freshness breach")
	}
}

func TestFlushResetsState(t *testing.T) {
	fixedNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	m := New(5 * time.Second)

	m.Observe(fixedNow.Add(-10*time.Second), fixedNow)
	m.Flush()
	r2 := m.Flush()
	if r2.MaxLag != 0 || r2.Count != 0 || r2.Breached {
		t.Fatalf("expected clean state after flush, got %+v", r2)
	}
}

func TestMaxLagTracksWorstEventInWindow(t *testing.T) {
	fixedNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	m := New(5 * time.Second)

	m.Observe(fixedNow.Add(-1*time.Second), fixedNow)
	m.Observe(fixedNow.Add(-20*time.Second), fixedNow)
	m.Observe(fixedNow.Add(-3*time.Second), fixedNow)
	r := m.Flush()
	if r.MaxLag != 20*time.Second {
		t.Fatalf("expected max lag 20s, got %v", r.MaxLag)
	}
	if r.Count != 3 {
		t.Fatalf("expected count 3, got %d", r.Count)
	}
}
