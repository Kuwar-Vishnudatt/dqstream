package nullrate

import "testing"

func observeWindow(m *Monitor, field string, total, nulls int) {
	for i := 0; i < total; i++ {
		m.Observe(field, i < nulls)
	}
}

func TestNoDriftResultsDuringWarmup(t *testing.T) {
	m := New(0.3, 3, 0.2)
	for w := 0; w < 3; w++ {
		observeWindow(m, "email", 100, 5) // stable 5% null rate
		results := m.Flush()
		if len(results) != 0 {
			t.Fatalf("window %d: expected no results during warmup, got %v", w, results)
		}
	}
}

func TestStableRateIsNotDrift(t *testing.T) {
	m := New(0.3, 3, 0.2)
	for w := 0; w < 3; w++ {
		observeWindow(m, "email", 100, 5)
		m.Flush()
	}
	observeWindow(m, "email", 100, 6) // 6% vs baseline ~5%: well within threshold
	results := m.Flush()
	if len(results) != 1 {
		t.Fatalf("expected one result for email, got %v", results)
	}
	if results[0].Drifted {
		t.Fatalf("expected no drift for a small rate fluctuation, got %+v", results[0])
	}
}

func TestSpikeInNullRateIsDrift(t *testing.T) {
	m := New(0.3, 3, 0.2)
	for w := 0; w < 3; w++ {
		observeWindow(m, "email", 100, 5) // baseline ~5% null
		m.Flush()
	}
	observeWindow(m, "email", 100, 60) // jumps to 60% null
	results := m.Flush()
	if len(results) != 1 || !results[0].Drifted {
		t.Fatalf("expected drift for email null-rate spike, got %v", results)
	}
}

func TestFieldsTrackedIndependently(t *testing.T) {
	m := New(0.3, 2, 0.2)
	for w := 0; w < 2; w++ {
		observeWindow(m, "email", 100, 5)
		observeWindow(m, "phone", 100, 50)
		m.Flush()
	}
	observeWindow(m, "email", 100, 70) // email spikes
	observeWindow(m, "phone", 100, 52) // phone stays roughly stable
	results := m.Flush()

	byField := map[string]FieldResult{}
	for _, r := range results {
		byField[r.Field] = r
	}
	if !byField["email"].Drifted {
		t.Fatalf("expected email to drift, got %+v", byField["email"])
	}
	if byField["phone"].Drifted {
		t.Fatalf("expected phone to NOT drift, got %+v", byField["phone"])
	}
}

func TestEmptyWindowDoesNotResetBaseline(t *testing.T) {
	m := New(0.3, 2, 0.2)
	observeWindow(m, "email", 100, 5)
	m.Flush()
	observeWindow(m, "email", 100, 5)
	m.Flush()
	// A window with zero events for this field (e.g. a quiet period) must
	// not corrupt the baseline or be reported as a spurious 0%-vs-baseline
	// drift.
	results := m.Flush()
	if len(results) != 0 {
		t.Fatalf("expected no result for a field with zero events this window, got %v", results)
	}
}
