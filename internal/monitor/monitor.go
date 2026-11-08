// Package monitor wires the individual checks (freshness, volume, schema,
// null-rate) into one per-topic pipeline: every decoded event is fed to the
// per-event checks immediately, and a periodic Flush closes out the current
// window for the per-window checks.
package monitor

import (
	"time"

	"github.com/Kuwar-Vishnudatt/dqstream/internal/alert"
	"github.com/Kuwar-Vishnudatt/dqstream/internal/event"
	"github.com/Kuwar-Vishnudatt/dqstream/internal/freshness"
	"github.com/Kuwar-Vishnudatt/dqstream/internal/nullrate"
	"github.com/Kuwar-Vishnudatt/dqstream/internal/schema"
	"github.com/Kuwar-Vishnudatt/dqstream/internal/volume"
)

// Config tunes every check. See each package's doc comments for what each
// parameter means; defaults here are reasonable for a moderate-traffic
// topic with a window on the order of seconds.
type Config struct {
	Topic string

	FreshnessThreshold time.Duration

	VolumeAlpha      float64
	VolumeZThreshold float64
	VolumeMinWarmup  int

	SchemaWarmupEvents int

	NullRateAlpha       float64
	NullRateMinWarmup   int
	NullRateDeltaThresh float64
}

// WithDefaults fills in zero-valued fields with reasonable defaults.
func (c Config) WithDefaults() Config {
	if c.FreshnessThreshold == 0 {
		c.FreshnessThreshold = 30 * time.Second
	}
	if c.VolumeAlpha == 0 {
		c.VolumeAlpha = 0.3
	}
	if c.VolumeZThreshold == 0 {
		c.VolumeZThreshold = 3.0
	}
	if c.VolumeMinWarmup == 0 {
		c.VolumeMinWarmup = 5
	}
	if c.SchemaWarmupEvents == 0 {
		c.SchemaWarmupEvents = 50
	}
	if c.NullRateAlpha == 0 {
		c.NullRateAlpha = 0.3
	}
	if c.NullRateMinWarmup == 0 {
		c.NullRateMinWarmup = 5
	}
	if c.NullRateDeltaThresh == 0 {
		c.NullRateDeltaThresh = 0.2
	}
	return c
}

// Monitor is a single topic's check pipeline.
type Monitor struct {
	cfg  Config
	sink alert.Sink
	now  func() time.Time

	schema    *schema.Schema
	freshness *freshness.Monitor
	volume    *volume.Monitor
	nullrate  *nullrate.Monitor

	windowCount int
}

// New builds a Monitor for one topic, delivering alerts to sink.
func New(cfg Config, sink alert.Sink) *Monitor {
	cfg = cfg.WithDefaults()
	return &Monitor{
		cfg:       cfg,
		sink:      sink,
		now:       time.Now,
		schema:    schema.New(cfg.SchemaWarmupEvents),
		freshness: freshness.New(cfg.FreshnessThreshold),
		volume:    volume.New(cfg.VolumeAlpha, cfg.VolumeZThreshold, cfg.VolumeMinWarmup),
		nullrate:  nullrate.New(cfg.NullRateAlpha, cfg.NullRateMinWarmup, cfg.NullRateDeltaThresh),
	}
}

// Observe feeds one decoded event through the per-event checks: freshness
// (event-time lag) and schema (structural drift). Null-rate accounting for
// every currently-known field is also recorded here, since whether a field
// was null is a per-event fact even though drift on it is only reported at
// window boundaries.
func (m *Monitor) Observe(rec event.Record) {
	m.windowCount++
	m.freshness.Observe(rec.EventTime, m.now())

	drifts := m.schema.Observe(rec.Fields)
	for _, d := range drifts {
		m.emitSchemaDrift(d)
	}

	for field := range m.schema.Fields() {
		v, present := rec.Fields[field]
		isNull := !present || v == nil
		m.nullrate.Observe(field, isNull)
	}
}

// Flush closes out the current window: evaluates the per-window checks
// (volume, null-rate) and the freshness max-lag for the window just ended,
// emitting an alert for anything that breached.
func (m *Monitor) Flush() {
	fr := m.freshness.Flush()
	if fr.Breached {
		sev := alert.SeverityWarning
		if fr.MaxLag > 3*m.cfg.FreshnessThreshold {
			sev = alert.SeverityCritical
		}
		m.send(alert.Alert{
			Check:    "freshness",
			Key:      "lag",
			Severity: sev,
			Message:  "event freshness lag exceeded threshold",
			Details: map[string]any{
				"max_lag_seconds":   fr.MaxLag.Seconds(),
				"threshold_seconds": m.cfg.FreshnessThreshold.Seconds(),
				"window_events":     fr.Count,
			},
		})
	}

	vr := m.volume.Observe(m.windowCount)
	m.windowCount = 0
	if vr.Anomalous {
		sev := alert.SeverityWarning
		if vr.ZScore > 2*m.cfg.VolumeZThreshold || vr.ZScore < -2*m.cfg.VolumeZThreshold {
			sev = alert.SeverityCritical
		}
		m.send(alert.Alert{
			Check:    "volume",
			Key:      "count",
			Severity: sev,
			Message:  "event volume deviated from learned baseline",
			Details: map[string]any{
				"count":   vr.Count,
				"mean":    vr.Mean,
				"std_dev": vr.StdDev,
				"z_score": vr.ZScore,
			},
		})
	}

	for _, nr := range m.nullrate.Flush() {
		if !nr.Drifted {
			continue
		}
		m.send(alert.Alert{
			Check:    "null_rate",
			Key:      "field:" + nr.Field,
			Severity: alert.SeverityWarning,
			Message:  "null rate for field rose well above its learned baseline",
			Details: map[string]any{
				"field":         nr.Field,
				"rate":          nr.Rate,
				"baseline_rate": nr.BaselineRate,
			},
		})
	}
}

func (m *Monitor) emitSchemaDrift(d schema.Drift) {
	sev := alert.SeverityWarning
	msg := ""
	details := map[string]any{"field": d.Field}
	switch d.Kind {
	case schema.DriftNewField:
		msg = "new field appeared that was not present during schema learning"
		details["new_type"] = string(d.NewType)
	case schema.DriftMissingField:
		sev = alert.SeverityCritical
		msg = "previously-required field is now missing"
		details["expected_type"] = string(d.OldType)
	case schema.DriftTypeChange:
		sev = alert.SeverityCritical
		msg = "field's type changed from its learned baseline type"
		details["old_type"] = string(d.OldType)
		details["new_type"] = string(d.NewType)
	}
	m.send(alert.Alert{
		Check:    "schema",
		Key:      string(d.Kind) + ":" + d.Field,
		Severity: sev,
		Message:  msg,
		Details:  details,
	})
}

func (m *Monitor) send(a alert.Alert) {
	a.Time = m.now()
	a.Topic = m.cfg.Topic
	_ = m.sink.Send(a)
}
