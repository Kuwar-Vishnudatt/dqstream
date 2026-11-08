// Package volume detects abnormal per-window event counts (spikes or drops)
// using an exponentially-weighted moving average and variance, rather than a
// fixed threshold — so "normal" adapts to each stream's actual traffic
// pattern instead of requiring a hand-tuned count per topic.
package volume

import (
	"math"
	"sort"
)

// Monitor tracks an EWMA mean and variance of per-window event counts and
// flags a window whose count deviates from that baseline by more than
// zThreshold standard deviations.
type Monitor struct {
	alpha      float64
	zThreshold float64
	minWarmup  int

	warmupSamples []float64
	learned       bool

	windows  int
	mean     float64
	variance float64
}

// New returns a volume anomaly monitor. alpha is the EWMA smoothing factor
// (0 < alpha <= 1; higher reacts faster to recent windows). zThreshold is
// the number of standard deviations from the EWMA mean that counts as
// anomalous. minWarmup is how many windows must be observed before the
// monitor will flag anything — with too few windows the variance estimate
// itself is noise, and flagging against it produces false positives.
func New(alpha, zThreshold float64, minWarmup int) *Monitor {
	if minWarmup < 2 {
		minWarmup = 2
	}
	return &Monitor{alpha: alpha, zThreshold: zThreshold, minWarmup: minWarmup}
}

// Result summarizes one window's count against the baseline as it stood
// BEFORE this window was folded in.
type Result struct {
	Count     int
	Mean      float64
	StdDev    float64
	ZScore    float64
	Anomalous bool
}

// Observe folds one window's count into the baseline and returns whether
// that count was anomalous relative to the baseline BEFORE this update.
// During warmup, nothing is ever flagged — see closeWarmup for why the
// baseline itself is computed robustly rather than via plain EWMA seeded
// from the very first sample.
func (m *Monitor) Observe(count int) Result {
	x := float64(count)

	if !m.learned {
		m.warmupSamples = append(m.warmupSamples, x)
		if len(m.warmupSamples) >= m.minWarmup {
			m.closeWarmup()
		}
		return Result{Count: count}
	}

	const epsilon = 1e-9
	res := Result{Count: count, Mean: m.mean, StdDev: math.Sqrt(m.variance)}
	switch {
	case res.StdDev > epsilon:
		res.ZScore = (x - m.mean) / res.StdDev
		res.Anomalous = math.Abs(res.ZScore) > m.zThreshold
	case math.Abs(x-m.mean) > epsilon:
		// Zero-variance baseline (every warmup window had the identical
		// count) and this window differs at all: that's anomalous by
		// definition even though a z-score isn't well-defined. Report a
		// signed infinity so callers still see directionality, rather than
		// silently treating it as normal just because division by a zero
		// stddev is undefined.
		if x > m.mean {
			res.ZScore = math.Inf(1)
		} else {
			res.ZScore = math.Inf(-1)
		}
		res.Anomalous = true
	}

	// EWMA update using the mean as it stood BEFORE this observation (not
	// after) — using the post-update mean would bias the variance estimate
	// downward, since the new point would always look closer to a mean that
	// was just pulled toward it.
	prevMean := m.mean
	diff := x - prevMean
	m.mean = prevMean + m.alpha*diff
	m.variance = (1 - m.alpha) * (m.variance + m.alpha*diff*diff)
	m.windows++

	return res
}

// closeWarmup computes the initial baseline from every buffered warmup
// sample using the median and the median absolute deviation (MAD), not a
// plain mean/EWMA seeded from the first sample.
//
// This is deliberate, found via actually running the demo CLI against real
// Kafka: a volume burst that happens to land inside the warmup window
// otherwise poisons a naive EWMA baseline on the very first sample it ever
// sees, inflating both the learned mean and variance so much that a SECOND,
// comparably-sized burst shortly afterward no longer crosses the z-score
// threshold — the monitor silently "learns" the anomaly as normal traffic.
// The median and MAD are robust statistics: up to roughly half the warmup
// samples can be outliers without moving either estimate much, which is
// exactly the property a baseline needs when it can't assume its own
// training data is clean.
func (m *Monitor) closeWarmup() {
	samples := append([]float64(nil), m.warmupSamples...)
	sort.Float64s(samples)
	median := percentile50(samples)

	absDevs := make([]float64, len(samples))
	for i, s := range samples {
		absDevs[i] = math.Abs(s - median)
	}
	sort.Float64s(absDevs)
	mad := percentile50(absDevs)

	// 1.4826 is the standard consistency constant that makes MAD an
	// unbiased estimator of the standard deviation for normally-distributed
	// data.
	robustStdDev := mad * 1.4826

	m.mean = median
	m.variance = robustStdDev * robustStdDev
	m.learned = true
	m.warmupSamples = nil
}

// percentile50 returns the median of an already-sorted slice.
func percentile50(sorted []float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}
