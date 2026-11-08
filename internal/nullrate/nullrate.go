// Package nullrate tracks the per-field null rate within each window and
// flags a field whose null rate jumps well above its learned baseline — a
// common, easy-to-miss real-world data-quality regression (an upstream
// change silently starts leaving a field empty far more often, without
// ever violating the schema itself).
package nullrate

// fieldStats is the learned baseline for one field plus its current
// window's counters.
type fieldStats struct {
	baselineRate float64 // EWMA of null rate, established over warmup windows
	learned      bool
	windowsSeen  int

	windowTotal int
	windowNull  int
}

// Monitor tracks per-field null rates across windows.
type Monitor struct {
	alpha       float64
	minWarmup   int
	deltaThresh float64 // absolute rate increase over baseline that counts as drift

	fields map[string]*fieldStats
}

// New returns a null-rate monitor. alpha is the EWMA smoothing factor for
// the baseline rate. minWarmup is how many windows a field must be observed
// in before its baseline is trusted enough to detect drift against.
// deltaThresh is the minimum absolute increase in null rate (e.g. 0.2 for
// +20 percentage points) over the learned baseline that counts as drift —
// an absolute, not relative, threshold, since a baseline near zero would
// make any relative threshold fire on a single stray null.
func New(alpha float64, minWarmup int, deltaThresh float64) *Monitor {
	if minWarmup < 1 {
		minWarmup = 1
	}
	return &Monitor{alpha: alpha, minWarmup: minWarmup, deltaThresh: deltaThresh, fields: make(map[string]*fieldStats)}
}

// Observe records whether one field was null on one event. Fields are
// tracked lazily — a field need not be pre-registered.
func (m *Monitor) Observe(field string, isNull bool) {
	fs, ok := m.fields[field]
	if !ok {
		fs = &fieldStats{}
		m.fields[field] = fs
	}
	fs.windowTotal++
	if isNull {
		fs.windowNull++
	}
}

// FieldResult summarizes one field's null-rate drift status for a window.
type FieldResult struct {
	Field        string
	Rate         float64
	BaselineRate float64
	Drifted      bool
}

// Flush closes out the current window for every tracked field, updates
// baselines, and returns drift results only for fields whose baseline is
// already established (fields still in warmup return no result this
// window, since there's nothing yet to compare against).
func (m *Monitor) Flush() []FieldResult {
	var results []FieldResult

	for name, fs := range m.fields {
		if fs.windowTotal == 0 {
			continue
		}
		rate := float64(fs.windowNull) / float64(fs.windowTotal)

		if fs.learned {
			drifted := rate-fs.baselineRate > m.deltaThresh
			results = append(results, FieldResult{Field: name, Rate: rate, BaselineRate: fs.baselineRate, Drifted: drifted})
		}

		if !fs.learned {
			if fs.windowsSeen == 0 {
				fs.baselineRate = rate
			} else {
				fs.baselineRate = fs.baselineRate + m.alpha*(rate-fs.baselineRate)
			}
			fs.windowsSeen++
			if fs.windowsSeen >= m.minWarmup {
				fs.learned = true
			}
		} else {
			fs.baselineRate = fs.baselineRate + m.alpha*(rate-fs.baselineRate)
		}

		fs.windowTotal = 0
		fs.windowNull = 0
	}

	return results
}
