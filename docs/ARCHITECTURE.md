# Architecture

## What problem this solves

Most "is my data pipeline healthy" monitoring stops at infrastructure
signals: consumer lag, error rates, whether the process is alive. Those
catch crashes. They don't catch a producer that's still running but has
quietly stopped setting a field, or started sending half the volume it used
to, or renamed a field without telling anyone downstream. Those failures are
*data-shaped*, not *infrastructure-shaped*, and they're exactly the kind the
2026 Data Engineer hiring research flagged as the differentiator: idempotent
pipelines and schema contracts are now expected table-stakes, but someone
who can reason about silent data-quality regressions — not just pipeline
uptime — is the more valuable signal. This project is that monitor.

## Why four checks, each with a learned baseline

Every check here solves the same underlying problem from a different angle:
how do you flag "this is abnormal" without requiring a human to pre-set a
threshold for every field of every topic, which doesn't scale and goes stale
the moment traffic patterns shift?

- **Freshness** (`internal/freshness`) tracks event-time lag per window and
  compares it to a single configured threshold — the one check that's
  intentionally NOT self-learned, because "how stale is too stale" is a
  business SLA decision, not a statistical property of the stream.
- **Volume** (`internal/volume`) learns a baseline mean/variance of
  per-window counts and flags a z-score outside a configurable number of
  standard deviations — adapting automatically as a topic's normal traffic
  level changes over the life of the monitor.
- **Schema** (`internal/schema`) learns the field set and types over a
  warmup window, then flags new fields, type changes, and — critically,
  only for fields observed in nearly every warmup event — missing fields.
  Real JSON events routinely omit genuinely optional fields; a schema
  monitor that doesn't distinguish "optional, sometimes absent" from
  "required, now missing" would be too noisy to run in production.
- **Null-rate** (`internal/nullrate`) learns a per-field baseline null rate
  and flags an *absolute* increase past a threshold (not a relative one,
  deliberately — see below) — catching the regression that schema drift
  structurally cannot: a field that's still present, still the right type,
  just empty far more often than before.

## Four real bugs found while building this, in the order they were found

### 1. Schema drift: pre-populating a field's type before the drift check ever ran

`schema.Observe` originally had one loop at the top that ran unconditionally
(during warmup AND after), and which set `fieldTypes[name] = t` the first
time it saw any field it didn't already know about — including a new field
appearing for the first time *after* warmup had closed. By the time
`detectDrift` ran afterward and checked `if _, known := s.fieldTypes[name];
!known`, the field was already marked known (by the loop that had just run
moments earlier in the same call), so `new_field` drift silently never
fired. `TestNewFieldDetectedAfterWarmup` caught this immediately — it
expected one `new_field` drift and got none. Fixed by splitting the method
cleanly: during warmup, only learn (populate `presence`/`fieldTypes`,
never check for drift); after warmup, only call `detectDrift` (which does
its own, now-correctly-ordered, known-field check before recording the new
type). No code path does both anymore.

### 2. Volume: an undefined z-score let a zero-variance baseline never flag anything

A topic with an exactly constant per-window count (every warmup window saw
precisely the same number of events) produces a learned variance of
precisely zero. Dividing by a zero standard deviation to get a z-score is
undefined, so the original code guarded with `if stdDev > 0` before
computing it — which meant that for a perfectly stable baseline, *no count
could ever be flagged*, no matter how extreme, because the guard just
silently skipped the anomaly check entirely.
`TestZScoreSignMatchesDirection` caught this (a baseline of constant 100s,
then a jump to 500, expected a positive z-score and got exactly 0). Fixed by
treating a near-zero stddev as its own case: if the count differs from the
mean at all under those conditions, it's anomalous by definition (report a
signed infinity for directionality) rather than silently treating "no
variance to divide by" as "nothing is ever wrong."

### 3. Freshness: a private clock the outer monitor's injected clock never reached

`monitor.Monitor` has a `now func() time.Time` field specifically so tests
can inject a fixed clock. The embedded `freshness.Monitor` originally had
its *own*, separate `now` field defaulting to the real `time.Now` — and
nothing wired the outer monitor's overridden clock into it. The first
full-pipeline monitor test set `m.now` to a fixed date in the past and
expected no freshness alerts on "fresh" events; instead every single
baseline event got flagged, with a reported lag of roughly 27 million
seconds — the freshness check was comparing a fixed test timestamp against
the *actual* real-world current time, not the fake clock the test thought it
controlled. This is the same category of bug as a context that silently
doesn't propagate a cancellation — a sub-component's own notion of "now"
diverging from its caller's. Fixed by removing the internal clock field
from `freshness.Monitor` entirely: `Observe(eventTime, now time.Time)` now
takes the current time as an explicit parameter from the caller, so there's
no private clock left to forget to override — the compiler enforces that
every caller supplies one.

### 4. Volume (found via the live CLI demo, not a unit test): a burst during warmup poisons the baseline

This was the one unit tests didn't catch, because the unit tests never
happened to put an anomalous sample inside the warmup window itself. Running
`dqstream generate` (which periodically injects a 5x volume burst once
anomaly injection starts) against `dqstream monitor` on a real local Kafka
broker, the FIRST burst landed by chance inside the monitor's 5-window
warmup period. The monitor's plain EWMA baseline — seeded directly from
whatever its very first sample happened to be, then updated via ordinary
exponential smoothing — absorbed that burst as if it were normal traffic:
the learned mean jumped from ~90 to ~124 and the stddev from near-zero to
~52. The SECOND burst, comparably sized to the first, produced a z-score of
only ~2.2 — below the default threshold of 3.0 — and was never flagged.
Diagnosed by rerunning the same scenario with the monitor's JSON alert log
in hand and noticing "volume" alerts simply never appeared despite two
clearly-logged bursts in the generator's own output; re-deriving the EWMA
math by hand against the logged counts confirmed the baseline had been
corrupted by the first burst rather than the detection logic being wrong
per se. Fixed by replacing the warmup-phase baseline estimator: instead of
seeding and updating an EWMA from the first sample onward (which gives a
single outlier, especially an early one, outsized leverage), the monitor now
buffers every warmup-window sample and computes the initial baseline from
the **median** and **median absolute deviation (MAD × 1.4826)** — robust
statistics that tolerate a meaningful fraction of outliers in their own
input without being dragged toward them. Only after warmup closes does the
monitor switch to the cheaper EWMA update for ongoing tracking, by which
point the baseline it's updating from is already trustworthy.
`TestBurstDuringWarmupDoesNotMaskLaterBurst` reproduces the exact scenario
(a burst as the final warmup sample, followed by a second comparable burst)
and asserts the second burst is still flagged; re-running the CLI demo
against live Kafka afterward confirmed both bursts now trigger a `volume`
alert.

## Null-rate drift: why an absolute, not relative, threshold

A field with a true baseline null rate near zero (e.g. 0.5%) would trip a
*relative* threshold (e.g. "null rate increased by more than 2x") on almost
any single stray null, since 1% is already "2x" of 0.5%. `nullrate.Monitor`
instead flags only an *absolute* increase past a fixed number of percentage
points over the learned baseline (default 20pp) — a field going from 0.5%
null to 3% null is real movement but not an incident; from 0.5% to 60% is.

## Why a real Kafka broker for integration tests, not a mock

`test/integration/harness.go` formats and boots a genuine single-node
KRaft-mode Kafka broker (Homebrew's `kafka` formula, no ZooKeeper, no
Docker) per test, on randomized ports in a temp directory, and tears it down
in `t.Cleanup`. This is the same choice `pgcdc` made for Postgres logical
replication: the properties actually worth testing here — consumer-group
offset commit and resume semantics, whether a topic needs to exist before
the first produce, whether reading actually blocks correctly — are
precisely the things a mocked client can't get wrong in the same ways the
real protocol can. `TestConsumerGroupResumesFromCommittedOffsetAfterRestart`
specifically proves offset commit/resume against the real broker: it reads
half of 20 published messages, closes the reader (simulating a restart),
opens a second reader in the same consumer group, and asserts it resumes
exactly at the message the first reader left off at — not from the
beginning, not skipping ahead.

## Testing strategy

- `internal/schema`, `internal/freshness`, `internal/volume`,
  `internal/nullrate`, `internal/alert`: pure unit tests, no I/O, each
  covering both the intended behavior and the edge case that produced one
  of the bugs above (zero-variance baseline, optional-field omission,
  burst-during-warmup, cooldown-key independence, etc.).
- `internal/monitor`: in-process end-to-end tests that drive the full
  per-event/per-window pipeline with a fixed injected clock and assert on
  the resulting alerts — including a dedicated "perfectly stable stream
  produces zero alerts" test, since a monitor that's merely sensitive isn't
  useful if it's also noisy.
- `test/integration`: the same pipeline, but through a real Kafka producer,
  a real consumer-group reader, and a real broker — proving the wiring
  itself (decode → Observe → Flush) works end to end over the actual wire
  protocol, and that consumer-group offset semantics behave as the monitor
  depends on them to.
