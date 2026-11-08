package volume

import (
	"math"
	"testing"
)

func TestNoAnomalyOnStableCounts(t *testing.T) {
	m := New(0.3, 3.0, 5)
	counts := []int{100, 98, 101, 99, 100, 102, 97, 100, 99, 101}
	for i, c := range counts {
		r := m.Observe(c)
		if r.Anomalous {
			t.Fatalf("window %d: unexpected anomaly on stable count %d (z=%v)", i, c, r.ZScore)
		}
	}
}

func TestSpikeDetectedAfterWarmup(t *testing.T) {
	m := New(0.3, 3.0, 5)
	for _, c := range []int{100, 98, 101, 99, 100, 102, 97} {
		m.Observe(c)
	}
	r := m.Observe(1000) // a 10x spike
	if !r.Anomalous {
		t.Fatalf("expected spike to 1000 to be flagged anomalous, got z=%v", r.ZScore)
	}
}

func TestDropToZeroDetected(t *testing.T) {
	m := New(0.3, 3.0, 5)
	for _, c := range []int{100, 98, 101, 99, 100, 102, 97} {
		m.Observe(c)
	}
	r := m.Observe(0)
	if !r.Anomalous {
		t.Fatalf("expected drop to 0 to be flagged anomalous, got z=%v", r.ZScore)
	}
}

func TestNoAnomalyFlaggedDuringWarmup(t *testing.T) {
	m := New(0.3, 3.0, 5)
	// Wildly different counts during warmup must never be flagged — there
	// isn't yet a baseline to compare against.
	for _, c := range []int{10, 10000, 1, 5000, 0} {
		r := m.Observe(c)
		if r.Anomalous {
			t.Fatalf("expected no anomaly during warmup, got one for count %d", c)
		}
	}
}

func TestZScoreSignMatchesDirection(t *testing.T) {
	m := New(0.3, 3.0, 5)
	for _, c := range []int{100, 100, 100, 100, 100} {
		m.Observe(c)
	}
	rUp := m.Observe(500)
	if rUp.ZScore <= 0 {
		t.Fatalf("expected positive z-score for a spike, got %v", rUp.ZScore)
	}

	m2 := New(0.3, 3.0, 5)
	for _, c := range []int{100, 100, 100, 100, 100} {
		m2.Observe(c)
	}
	rDown := m2.Observe(10)
	if rDown.ZScore >= 0 {
		t.Fatalf("expected negative z-score for a drop, got %v", rDown.ZScore)
	}
}

// TestBurstDuringWarmupDoesNotMaskLaterBurst reproduces a real bug found by
// running the demo CLI against a live Kafka broker: a volume burst that
// happened to land inside the warmup window inflated the learned baseline
// so much (via plain EWMA seeded from whatever sample it saw first) that an
// equally-sized SECOND burst immediately afterward no longer crossed the
// z-score threshold. A robust (median/MAD) baseline must not have this
// failure mode.
func TestBurstDuringWarmupDoesNotMaskLaterBurst(t *testing.T) {
	m := New(0.3, 3.0, 5)
	// 4 normal windows, then a burst AS PART OF warmup (the 5th sample).
	for _, c := range []int{90, 92, 88, 91, 240} {
		m.Observe(c)
	}
	// A second, comparably-sized burst right after warmup closes must still
	// be flagged.
	r := m.Observe(235)
	if !r.Anomalous {
		t.Fatalf("expected a burst immediately after warmup to be flagged despite an earlier burst landing inside warmup, got z=%v mean=%v stddev=%v", r.ZScore, r.Mean, r.StdDev)
	}
}

func TestRobustBaselineIgnoresMinorityOfOutliersInWarmup(t *testing.T) {
	// 1 outlier out of 5 warmup samples: median/MAD should still land close
	// to the true ~90 baseline, not be dragged toward the outlier.
	m := New(0.3, 3.0, 5)
	for _, c := range []int{90, 91, 89, 90, 500} {
		m.Observe(c)
	}
	r := m.Observe(90)
	if r.Anomalous {
		t.Fatalf("expected a normal count to NOT be anomalous against a robust baseline, got mean=%v stddev=%v z=%v", r.Mean, r.StdDev, r.ZScore)
	}
	if math.Abs(r.Mean-90) > 5 {
		t.Fatalf("expected robust baseline mean near 90 despite one outlier, got %v", r.Mean)
	}
}

func TestMeanTracksConstantSequenceExactly(t *testing.T) {
	m := New(0.3, 3.0, 2)
	for i := 0; i < 20; i++ {
		m.Observe(50)
	}
	r := m.Observe(50)
	if math.Abs(r.Mean-50) > 0.001 {
		t.Fatalf("expected mean to converge to 50 on a constant sequence, got %v", r.Mean)
	}
}
