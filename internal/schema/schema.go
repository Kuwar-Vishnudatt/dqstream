// Package schema infers a lightweight structural schema from decoded JSON
// payloads and detects drift against a learned baseline: new fields,
// missing REQUIRED fields, and type changes on a field that previously had
// a stable type.
package schema

import "fmt"

// FieldType is a coarse JSON type classification. "null" is tracked
// separately from the other types specifically so a field that is merely
// null in one event doesn't get misclassified as a type change — see
// Schema.Observe.
type FieldType string

const (
	TypeString FieldType = "string"
	TypeNumber FieldType = "number"
	TypeBool   FieldType = "bool"
	TypeObject FieldType = "object"
	TypeArray  FieldType = "array"
	TypeNull   FieldType = "null"
)

func classify(v any) FieldType {
	switch v.(type) {
	case string:
		return TypeString
	case float64:
		return TypeNumber
	case bool:
		return TypeBool
	case map[string]any:
		return TypeObject
	case []any:
		return TypeArray
	case nil:
		return TypeNull
	default:
		return FieldType(fmt.Sprintf("%T", v))
	}
}

// DriftKind identifies the category of a single detected change.
type DriftKind string

const (
	DriftNewField     DriftKind = "new_field"
	DriftMissingField DriftKind = "missing_field"
	DriftTypeChange   DriftKind = "type_change"
)

// Drift describes one detected schema change for one field.
type Drift struct {
	Field   string
	Kind    DriftKind
	OldType FieldType // set for type_change, missing_field
	NewType FieldType // set for new_field, type_change
}

// requiredThreshold: a field observed in at least this fraction of warmup
// events is treated as "required" — its later absence is drift. Below this,
// it's an optional field and never triggers missing_field. This matters
// because real-world JSON events routinely omit optional fields outright
// (rather than sending them as null); without a presence threshold, every
// event missing an optional field would falsely report drift.
const requiredThreshold = 0.95

// Schema learns an observed structural schema over a warmup window, then
// detects drift against what it learned. It is NOT safe for concurrent use;
// callers running concurrent consumers should serialize access (e.g. the
// monitor's single check goroutine per partition).
type Schema struct {
	warmupSize int
	warmupSeen int
	presence   map[string]int // field -> count of warmup events it appeared in

	fieldTypes map[string]FieldType
	required   map[string]bool
	learned    bool
}

// New returns a schema that learns its baseline from the first warmupEvents
// events it observes (minimum 1).
func New(warmupEvents int) *Schema {
	if warmupEvents < 1 {
		warmupEvents = 1
	}
	return &Schema{
		warmupSize: warmupEvents,
		presence:   make(map[string]int),
		fieldTypes: make(map[string]FieldType),
		required:   make(map[string]bool),
	}
}

// Fields returns the currently known field->type map, for inspection or
// persisting as a baseline. The returned map is a copy.
func (s *Schema) Fields() map[string]FieldType {
	out := make(map[string]FieldType, len(s.fieldTypes))
	for k, v := range s.fieldTypes {
		out[k] = v
	}
	return out
}

// Learned reports whether the warmup window has closed and the schema is
// now actively detecting drift (vs. still learning its baseline).
func (s *Schema) Learned() bool {
	return s.learned
}

// Observe records one event's fields against the schema and, once the
// warmup window has closed, returns any drift detected on THIS event
// relative to the learned baseline. During warmup it returns nothing — it's
// still building the baseline, not yet comparing against it.
func (s *Schema) Observe(fields map[string]any) []Drift {
	if !s.learned {
		for name, raw := range fields {
			t := classify(raw)
			if t == TypeNull {
				continue
			}
			s.presence[name]++
			if _, known := s.fieldTypes[name]; !known {
				s.fieldTypes[name] = t
			}
		}
		s.warmupSeen++
		if s.warmupSeen >= s.warmupSize {
			s.closeWarmup()
		}
		return nil
	}

	return s.detectDrift(fields)
}

func (s *Schema) closeWarmup() {
	for name, count := range s.presence {
		if float64(count)/float64(s.warmupSeen) >= requiredThreshold {
			s.required[name] = true
		}
	}
	s.learned = true
}

func (s *Schema) detectDrift(fields map[string]any) []Drift {
	var drifts []Drift

	for name, raw := range fields {
		t := classify(raw)
		if t == TypeNull {
			continue
		}
		prev, known := s.fieldTypes[name]
		switch {
		case !known:
			s.fieldTypes[name] = t
			drifts = append(drifts, Drift{Field: name, Kind: DriftNewField, NewType: t})
		case t != prev:
			s.fieldTypes[name] = t
			drifts = append(drifts, Drift{Field: name, Kind: DriftTypeChange, OldType: prev, NewType: t})
		}
	}

	for name := range s.required {
		if _, present := fields[name]; !present {
			drifts = append(drifts, Drift{Field: name, Kind: DriftMissingField, OldType: s.fieldTypes[name]})
		}
	}

	return drifts
}
