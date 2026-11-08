package alert

import (
	"testing"
	"time"
)

func TestDeduperSuppressesWithinCooldown(t *testing.T) {
	mem := NewMemorySink()
	d := NewDeduper(mem, time.Minute)
	fixedNow := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d.now = func() time.Time { return fixedNow }

	a := Alert{Topic: "orders", Check: "freshness", Key: "lag"}
	for i := 0; i < 5; i++ {
		if err := d.Send(a); err != nil {
			t.Fatal(err)
		}
	}
	if len(mem.Alerts()) != 1 {
		t.Fatalf("expected exactly 1 alert delivered within cooldown, got %d", len(mem.Alerts()))
	}
}

func TestDeduperAllowsAfterCooldownExpires(t *testing.T) {
	mem := NewMemorySink()
	d := NewDeduper(mem, time.Minute)
	cur := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d.now = func() time.Time { return cur }

	a := Alert{Topic: "orders", Check: "freshness", Key: "lag"}
	d.Send(a)
	cur = cur.Add(2 * time.Minute)
	d.Send(a)

	if len(mem.Alerts()) != 2 {
		t.Fatalf("expected 2 alerts after cooldown expired, got %d", len(mem.Alerts()))
	}
}

func TestDeduperKeysAreIndependent(t *testing.T) {
	mem := NewMemorySink()
	d := NewDeduper(mem, time.Minute)
	fixedNow := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d.now = func() time.Time { return fixedNow }

	d.Send(Alert{Topic: "orders", Check: "freshness", Key: "lag"})
	d.Send(Alert{Topic: "orders", Check: "schema", Key: "new_field:foo"})
	d.Send(Alert{Topic: "payments", Check: "freshness", Key: "lag"})

	if len(mem.Alerts()) != 3 {
		t.Fatalf("expected 3 distinct keys to all be delivered, got %d", len(mem.Alerts()))
	}
}

func TestMemorySinkAlertsReturnsCopy(t *testing.T) {
	mem := NewMemorySink()
	mem.Send(Alert{Topic: "orders"})
	got := mem.Alerts()
	got[0].Topic = "mutated"
	if mem.Alerts()[0].Topic != "orders" {
		t.Fatal("Alerts() must return a copy, mutation leaked into sink")
	}
}
