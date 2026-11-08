package monitor

import (
	"testing"
	"time"

	"github.com/Kuwar-Vishnudatt/dqstream/internal/alert"
	"github.com/Kuwar-Vishnudatt/dqstream/internal/event"
)

func rec(now time.Time, lag time.Duration, fields map[string]any) event.Record {
	return event.Record{EventTime: now.Add(-lag), Fields: fields}
}

func baseFields(status string) map[string]any {
	return map[string]any{"id": "x", "status": status, "amount": float64(10)}
}

func TestEndToEndDetectsFreshnessVolumeSchemaAndNullRate(t *testing.T) {
	mem := alert.NewMemorySink()
	cfg := Config{
		Topic:               "orders",
		FreshnessThreshold:  5 * time.Second,
		VolumeMinWarmup:     3,
		VolumeZThreshold:    3.0,
		SchemaWarmupEvents:  20,
		NullRateMinWarmup:   3,
		NullRateDeltaThresh: 0.2,
	}
	m := New(cfg, mem)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return now }

	// --- Window 0..2: establish a clean baseline (schema warmup needs 20
	// events, so spread across a few windows of 10 "normal" events each). ---
	for w := 0; w < 3; w++ {
		for i := 0; i < 10; i++ {
			m.Observe(rec(now, time.Second, baseFields("ok")))
		}
		m.Flush()
	}
	if len(mem.Alerts()) != 0 {
		t.Fatalf("expected no alerts during clean baseline windows, got %v", mem.Alerts())
	}

	// --- Window 3: a volume spike (10x normal count). ---
	for i := 0; i < 100; i++ {
		m.Observe(rec(now, time.Second, baseFields("ok")))
	}
	m.Flush()
	if !hasCheck(mem.Alerts(), "volume") {
		t.Fatalf("expected a volume alert after a 10x spike, got %v", mem.Alerts())
	}

	// --- Window 4: a freshness breach (event 10s stale vs 5s threshold). ---
	for i := 0; i < 9; i++ {
		m.Observe(rec(now, time.Second, baseFields("ok")))
	}
	m.Observe(rec(now, 10*time.Second, baseFields("ok")))
	m.Flush()
	if !hasCheck(mem.Alerts(), "freshness") {
		t.Fatalf("expected a freshness alert after a stale event, got %v", mem.Alerts())
	}

	// --- Window 5: a new field appears (schema drift). ---
	for i := 0; i < 9; i++ {
		m.Observe(rec(now, time.Second, baseFields("ok")))
	}
	drifted := baseFields("ok")
	drifted["coupon_code"] = "SAVE10"
	m.Observe(rec(now, time.Second, drifted))
	m.Flush()
	if !hasCheck(mem.Alerts(), "schema") {
		t.Fatalf("expected a schema alert after a new field appeared, got %v", mem.Alerts())
	}

	// --- Windows 6..8: null rate on "status" spikes well above baseline. ---
	for w := 0; w < 3; w++ {
		for i := 0; i < 10; i++ {
			f := baseFields("ok")
			if w == 2 {
				f["status"] = nil // sudden spike to 100% null in the last window
			}
			m.Observe(rec(now, time.Second, f))
		}
		m.Flush()
	}
	if !hasCheck(mem.Alerts(), "null_rate") {
		t.Fatalf("expected a null_rate alert after status went null, got %v", mem.Alerts())
	}
}

func TestNoFalsePositivesOnPerfectlyStableStream(t *testing.T) {
	mem := alert.NewMemorySink()
	cfg := Config{Topic: "orders", FreshnessThreshold: 5 * time.Second, SchemaWarmupEvents: 10, VolumeMinWarmup: 3, NullRateMinWarmup: 3}
	m := New(cfg, mem)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return now }

	for w := 0; w < 10; w++ {
		for i := 0; i < 20; i++ {
			m.Observe(rec(now, time.Second, baseFields("ok")))
		}
		m.Flush()
	}
	if alerts := mem.Alerts(); len(alerts) != 0 {
		t.Fatalf("expected zero alerts on a perfectly stable stream, got %v", alerts)
	}
}

func TestMissingRequiredFieldIsCritical(t *testing.T) {
	mem := alert.NewMemorySink()
	cfg := Config{Topic: "orders", SchemaWarmupEvents: 5, VolumeMinWarmup: 3, NullRateMinWarmup: 3}
	m := New(cfg, mem)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return now }

	for i := 0; i < 5; i++ {
		m.Observe(rec(now, 0, baseFields("ok")))
	}
	m.Observe(rec(now, 0, map[string]any{"id": "x"})) // status, amount missing
	m.Flush()

	var found bool
	for _, a := range mem.Alerts() {
		if a.Check == "schema" && a.Severity == alert.SeverityCritical {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a critical schema alert for missing required fields, got %v", mem.Alerts())
	}
}

func hasCheck(alerts []alert.Alert, check string) bool {
	for _, a := range alerts {
		if a.Check == check {
			return true
		}
	}
	return false
}
