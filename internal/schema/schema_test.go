package schema

import "testing"

func obj(pairs ...any) map[string]any {
	m := make(map[string]any, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		m[pairs[i].(string)] = pairs[i+1]
	}
	return m
}

func TestNoDriftDuringWarmup(t *testing.T) {
	s := New(5)
	for i := 0; i < 5; i++ {
		if d := s.Observe(obj("a", "x", "b", float64(1))); d != nil {
			t.Fatalf("expected no drift during warmup, got %v", d)
		}
	}
	if !s.Learned() {
		t.Fatal("expected warmup to have closed after 5 events")
	}
}

func TestNewFieldDetectedAfterWarmup(t *testing.T) {
	s := New(3)
	for i := 0; i < 3; i++ {
		s.Observe(obj("a", "x"))
	}
	drifts := s.Observe(obj("a", "x", "b", float64(2)))
	if len(drifts) != 1 || drifts[0].Kind != DriftNewField || drifts[0].Field != "b" {
		t.Fatalf("expected one new_field drift for b, got %v", drifts)
	}
}

func TestTypeChangeDetected(t *testing.T) {
	s := New(3)
	for i := 0; i < 3; i++ {
		s.Observe(obj("amount", float64(10)))
	}
	drifts := s.Observe(obj("amount", "ten"))
	if len(drifts) != 1 || drifts[0].Kind != DriftTypeChange || drifts[0].OldType != TypeNumber || drifts[0].NewType != TypeString {
		t.Fatalf("expected type_change number->string for amount, got %v", drifts)
	}
}

func TestMissingRequiredFieldDetected(t *testing.T) {
	s := New(4)
	for i := 0; i < 4; i++ {
		s.Observe(obj("id", "1", "status", "ok"))
	}
	drifts := s.Observe(obj("id", "2"))
	if len(drifts) != 1 || drifts[0].Kind != DriftMissingField || drifts[0].Field != "status" {
		t.Fatalf("expected missing_field drift for status, got %v", drifts)
	}
}

func TestOptionalFieldOmissionIsNotDrift(t *testing.T) {
	// "note" present in only 1 of 10 warmup events: below the required
	// threshold, so its later absence must NOT be reported as drift.
	s := New(10)
	for i := 0; i < 10; i++ {
		if i == 0 {
			s.Observe(obj("id", "1", "note", "hi"))
		} else {
			s.Observe(obj("id", "1"))
		}
	}
	drifts := s.Observe(obj("id", "2"))
	if len(drifts) != 0 {
		t.Fatalf("expected no drift for optional field omission, got %v", drifts)
	}
}

func TestNullNeverCountsAsTypeChangeOrDrift(t *testing.T) {
	s := New(3)
	for i := 0; i < 3; i++ {
		s.Observe(obj("amount", float64(10)))
	}
	// amount is null this time: must not be flagged as type change, and
	// must not be flagged as missing (field key is present, just null).
	drifts := s.Observe(obj("amount", nil))
	if len(drifts) != 0 {
		t.Fatalf("expected no drift from a null value, got %v", drifts)
	}
}

func TestFieldsReturnsCopy(t *testing.T) {
	s := New(1)
	s.Observe(obj("a", "x"))
	f := s.Fields()
	f["a"] = TypeNumber
	if s.fieldTypes["a"] != TypeString {
		t.Fatal("Fields() must return a copy, mutation leaked into schema")
	}
}
