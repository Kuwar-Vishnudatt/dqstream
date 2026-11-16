# dqstream

**A streaming data-quality monitor that watches whether your data is quietly going wrong, not just whether your pipeline is up.**

dqstream is a small Go program that sits on a Kafka topic and continuously checks the *shape* of the events flowing through it — their timing, their volume, their structure, and how often their fields are empty — against a baseline it learns from the stream itself. It runs four independent checks per topic (freshness, volume, schema drift, and null-rate drift), and three of the four learn what "normal" looks like automatically instead of relying on a human to hand-pick a threshold. It consumes from a real local Kafka broker — Homebrew's `kafka` package running in KRaft mode, which means no ZooKeeper and no Docker are needed to try it — and prints one JSON line per alert.

This README walks through the full system: why it exists, exactly how data flows through it from Kafka to an alert, a tour of every package, and the full story of four real bugs that were found (and fixed) while building it — including one that only showed up when the thing was actually run against a live broker, not in a unit test.

## Why this exists

Most "is my data pipeline healthy" tooling answers one question: **is it up?** Is the consumer lagging, did the process crash, is the broker reachable, did deserialization throw an exception. Those are real failures and worth catching, but they all share one property — they're loud. Something visibly breaks, and whatever you're already using to watch infrastructure will tell you about it.

The failures that actually hurt in production are usually the quiet ones:

- A producer is still running, still sending *something*, but has silently stalled upstream of itself — timestamps keep drifting further into the past while nothing crashes.
- A field that used to always be populated starts being `null` on a growing fraction of events, with no error anywhere, because some upstream service changed behavior and nobody updated the schema.
- A field gets renamed, or its type quietly changes from a number to a string, and nothing downstream throws — it just silently starts getting the wrong data, or Decimal-parses a string that used to be a number.
- Traffic from a producer quietly drops to a third of normal (or triples), and no "is it up" check notices, because the process is still alive and still producing *some* events.

None of these trip a consumer-lag alert or a crash dashboard. They are **data-shaped** failures, not **infrastructure-shaped** ones, and by the time a human notices (usually because a downstream report looks wrong), the bad data has often been flowing for hours or days. dqstream exists to catch exactly this second, harder category — continuously, automatically, and without needing someone to pre-configure a magic threshold for every field of every topic before it's useful.

## How it works

### 1. Events flow in from Kafka

The `monitor` command opens a Kafka **consumer-group reader** on one topic (a reader that's part of a named group, so Kafka tracks which messages it has already processed and lets it resume from the same point after a restart instead of starting over or skipping ahead). Every message is expected to be a small JSON envelope:

```json
{"event_time": "2026-11-16T10:03:21Z", "payload": {"order_id": "ord-42", "status": "placed", "amount": 19.99}}
```

`event_time` is the *producer's own claim* about when the event happened — deliberately not Kafka's own message timestamp (which only reflects when the broker received it) — because the producer's claimed time is exactly the field that goes stale first when a producer gets stuck, retried, or is backfilling old data. `payload` is decoded into a generic field-name → value map so every check can inspect it without knowing the event's business schema ahead of time.

Each decoded event is handed to the monitor twice, in two different rhythms:

- **Per event, immediately** — the freshness check and the schema-drift check each look at the single event as it arrives.
- **Per window, on a timer** — every few seconds (configurable, default 5s), a **tumbling window** closes: a fixed-size, non-overlapping time slice (window 1 covers seconds 0–5, window 2 covers seconds 5–10, and so on, with no overlap and nothing counted twice). The volume check and the null-rate check only evaluate once a window closes, because "how many events arrived" and "what fraction were null" are both window-level facts, not single-event facts.

### 2. What each of the four checks does

**Freshness** (`internal/freshness`) tracks, within the current window, the largest lag it has seen between an event's claimed `event_time` and the current time. If that maximum lag exceeds a configured threshold (default 30 seconds) when the window closes, it fires. This is the one check that is **deliberately not self-learned** — "how stale is too stale" is a business SLA decision (what does *your* system consider too old to still be useful), not a statistical property dqstream can infer from the data itself. Every other check below, by contrast, learns its own baseline.

**Volume** (`internal/volume`) counts how many events arrived in each window and compares that count to a baseline it has learned for this topic, flagging unusually high *or* low counts. The comparison is a **z-score**: how many standard deviations the current count is from the learned average (a z-score of 3 means "3 standard deviations above normal," which is already a big, rare deviation for most real distributions). The baseline itself is an **EWMA** — an exponentially-weighted moving average, which is just a running average that weights recent windows more heavily than old ones, so it adapts as a topic's normal traffic level genuinely shifts over time, rather than staying anchored to however things looked on day one. Before any of that EWMA tracking starts, though, the monitor spends a **warmup period** (the first several windows) building its *initial* baseline — and, as explained in bug #4 below, it deliberately does this using **robust statistics** (the median and MAD, explained there) rather than a plain average, specifically because a plain average turned out to be dangerously easy to corrupt.

**Schema drift** (`internal/schema`) learns the expected set of fields and each field's JSON type (string, number, bool, object, array) during a warmup window (the first 50 events, by default). Once that warmup closes, every subsequent event is checked against what was learned, and three kinds of drift are flagged: a **new field** that never appeared during warmup, a **type change** on a field whose type was previously stable (e.g. `amount` going from a number to a string), and a **missing field** — but only for fields that were treated as "required." A field only earns "required" status if it was present in at least 95% of warmup events; this **presence threshold** exists because real-world JSON events routinely omit genuinely optional fields outright rather than sending them as `null`, and without this threshold, every single event that happened to skip an optional field would falsely report drift.

**Null-rate drift** (`internal/nullrate`) tracks, per field, what fraction of events in each window had that field `null` (or missing), and learns a baseline null rate for each field the same way volume learns its baseline — an EWMA, built up over a warmup period. Once a field's baseline is established, a window's null rate is compared to it, and this is the one place where the *kind* of threshold matters: the comparison is **absolute**, not relative. A field going from a 0.5% baseline null rate to 3% is a 6x relative increase but barely an incident; a fixed absolute threshold (default: 20 percentage points above baseline) catches the field that goes from 0.5% null to 60% null while deliberately not screaming every time a single stray null nudges a near-zero baseline. This check exists because it catches a regression schema drift structurally cannot: a field that is still present, and still the right type, just empty far more often than it used to be.

### 3. How "learning a baseline" actually works, in one sentence per check

Every self-learning check follows the same shape: collect data during a warmup period with the alarm turned off, derive a starting baseline from that data once warmup closes, and then keep that baseline updated going forward (via EWMA) as real traffic continues — so a human never has to type in "the right" count, rate, or field type by hand, and the baseline itself keeps adapting as the topic's own normal behavior evolves.

### 4. What happens when something looks wrong

When a check decides something is anomalous, the monitor (`internal/monitor`) builds an `Alert` — a small struct carrying which check fired, a severity (`warning` or `critical`; schema type changes and missing required fields are always `critical`, since those are the ones most likely to break a downstream consumer outright), a human-readable message, and a `Details` map with the specific numbers behind the decision (the z-score, the baseline rate, the field and its old/new type, etc.).

That alert then passes through a **deduper** (`internal/alert`) before delivery: if the same check keeps breaching on the same key (say, `volume` on this topic, or `null_rate` on field `status`) within a configurable cooldown window (default 2 minutes), only the first occurrence is actually delivered — a sustained incident pages once per cooldown period, not once per window, which is what keeps a real incident from turning into alert spam. Delivery itself goes through a pluggable `Sink` interface; the CLI wires up a console sink that prints one JSON line per delivered alert to stdout, but the interface is there specifically so a real deployment could swap in a Slack webhook, PagerDuty, or a metrics backend without touching any check's logic.

## Architecture at a glance

```
Kafka topic ──▶ consumer-group reader ──▶ decode JSON envelope ──▶ Monitor
                                                                       │
                                     per event: freshness, schema drift
                                     per window (tumbling): volume, null-rate
                                                                       │
                                                      Alert ──▶ Deduper ──▶ Sink
```

- **`internal/event`** — defines the wire envelope (`event_time` + `payload`) and decodes a raw Kafka message into the generic field-name → value map every check operates on.
- **`internal/freshness`** — tracks per-window max event-time lag against a fixed threshold. Notably has *no* internal clock of its own (see bug #3) — the caller must pass in "now" explicitly on every call.
- **`internal/volume`** — the EWMA/robust-baseline event-count anomaly detector described above.
- **`internal/schema`** — learns field names and types over a warmup window, then detects new/missing/type-changed fields.
- **`internal/nullrate`** — learns a per-field baseline null rate and flags an absolute increase past it.
- **`internal/alert`** — the `Alert` type, the `Sink` interface, a `ConsoleSink` (prints JSON to a writer), a `MemorySink` (for tests), and the cooldown `Deduper`.
- **`internal/monitor`** — wires all four checks into a single per-topic pipeline: `Observe(event)` feeds the per-event checks, `Flush()` closes a window and evaluates the per-window checks, and both paths can produce alerts.
- **`internal/kafkaio`** — a thin wrapper around the `segmentio/kafka-go` client: a consumer-group reader, a writer tuned for a synthetic load generator, and explicit topic creation (done up front rather than relying on Kafka's auto-create, which can otherwise race the very first produced message).
- **`cmd/dqstream`** — the CLI binary with two subcommands: `monitor` (runs the pipeline above against a live topic) and `generate` (produces a synthetic, configurable event stream — including deliberately injected anomalies — for demos and manual testing).
- **`test/integration`** — a harness that boots a real, ephemeral, single-node Kafka broker in KRaft mode per test (on a random port, in a temp directory, torn down afterward) and proves the pipeline end to end against actual Kafka wire-protocol behavior, not a mock.
- **`docs/ARCHITECTURE.md`** — the deeper design rationale and the full bug write-ups, for anyone who wants more detail than this README's summary below.

## Four real bugs found while building this

Each of these was a genuine defect caught during development, not a hypothetical. The fourth is the most interesting, because it's the one unit tests structurally could not have caught — it only surfaced by running the actual demo against a real, live Kafka broker.

### 1. Schema drift silently stopped firing for new fields

The original `schema.Observe` had a single loop that ran on every call, warmup or not, and that loop recorded a field's type into the "known fields" map the moment it saw that field for the first time — including when that first sighting happened *after* warmup had already closed. By the time the drift-detection logic ran afterward and checked "do I already know this field?", the answer was always yes, because the very same function call had just recorded it a few lines earlier. The result: a genuinely new field appearing in live traffic after warmup never triggered a `new_field` alert — the alert was being silently swallowed by the check's own bookkeeping.

A unit test (`TestNewFieldDetectedAfterWarmup`) caught this immediately: it fed a field that never appeared during warmup and asserted exactly one `new_field` drift, and got zero. The fix was to cleanly separate "learning" from "detecting" into two non-overlapping code paths — during warmup, only ever learn; after warmup, only ever detect — so no single code path can both record a field as known and fail to check it for drift in the same breath.

### 2. A perfectly stable baseline made the volume check permanently blind

The volume check's anomaly score is a z-score: `(observed count − baseline mean) / baseline standard deviation`. If a topic's warmup window happens to see the exact same event count in every single window, the learned variance — and therefore the standard deviation — is precisely zero. Dividing by zero is undefined, so the original code simply guarded the whole z-score computation behind `if stdDev > 0`. That guard was the bug: for any topic whose baseline happened to be perfectly constant, *no deviation, however extreme, could ever be flagged* — the guard didn't just skip a division, it skipped the entire anomaly check.

A unit test (`TestZScoreSignMatchesDirection`) caught it: a baseline of constant 100s, followed by a jump to 500, should obviously be anomalous, but the z-score came back as exactly 0. The fix treats a near-zero standard deviation as its own explicit case: if the baseline had no observed variance and the new count differs from it at all, that's anomalous by definition — there's no need for a well-defined z-score to know something changed. (The code reports a signed infinity so callers can still tell direction — did it spike or drop — without pretending a z-score exists where it mathematically can't.)

### 3. The freshness check was quietly using the real clock, not the test's fake one

The outer `Monitor` has a `now` field specifically so tests can inject a fixed, fake "current time" instead of the real clock — this is what lets a test say "pretend it's exactly 10:00:00" and get deterministic results. The freshness sub-check, however, originally kept its *own*, completely separate internal clock field, defaulting to the real `time.Now`, and nothing ever wired the outer monitor's overridden clock into it.

The very first end-to-end pipeline test exposed this immediately: it set the monitor's fake clock to a fixed date in the past and expected "fresh" baseline events to produce zero freshness alerts — instead, *every single event* was flagged, with a reported lag of roughly 27 million seconds. The freshness check had been comparing the test's fixed, fake timestamp against the actual real-world wall-clock time, because its own private clock never got the memo about the override. This is the same class of bug as a cancellation signal that silently fails to propagate into a sub-component: a piece of internal state quietly diverging from what its caller believes it controls. The fix removed the private clock entirely — `Observe` now takes "now" as an explicit parameter the caller must supply on every call, so there's no internal clock left to ever forget to override; the compiler enforces it.

### 4. The one that only live Kafka could catch: a burst during warmup poisoned the baseline

This is the bug unit tests didn't find, because no unit test happened to put an anomalous sample inside the warmup window itself — it only showed up by actually running `dqstream generate` (which periodically injects a 5x volume burst once anomaly injection starts) against `dqstream monitor` on a real local Kafka broker.

By chance, the very first injected burst landed inside the monitor's warmup period — the first few windows it uses to establish what "normal" volume looks like for this topic. The original baseline estimator was a plain EWMA, seeded directly from whatever its first observed sample happened to be and updated by ordinary exponential smoothing from there. That design absorbed the burst as if it were just normal traffic: the learned mean jumped from roughly 90 to roughly 124, and the learned standard deviation went from near-zero to roughly 52. When a *second*, comparably-sized burst arrived shortly afterward, it produced a z-score of only about 2.2 — below the default anomaly threshold of 3.0 — and was never flagged at all. The monitor had, in effect, "learned" the first anomaly as the new normal, which made it blind to the second one.

This was diagnosed by rerunning the same scenario with the monitor's JSON alert log in hand and noticing that `volume` alerts simply never showed up, despite the generator's own log clearly reporting two separate bursts. Re-deriving the EWMA math by hand against the logged window counts confirmed the baseline itself had been dragged off course by the first burst, rather than the anomaly-detection formula being wrong.

The fix replaces how the *warmup-phase* baseline is computed. Instead of seeding and updating an EWMA sample-by-sample from the very first window (which gives one early outlier outsized, permanent leverage over everything that follows), the monitor now buffers every warmup-window count and, once warmup closes, computes the initial baseline from the **median** and the **median absolute deviation (MAD)** — both are "robust statistics," meaning they can tolerate a meaningful fraction of their own training data being outliers without being dragged toward those outliers the way a plain average would be. (Concretely: the median is the middle value when all warmup counts are sorted, so one huge burst among several normal windows barely moves it; MAD is the median of how far each sample is from that median, scaled by a standard constant — 1.4826 — so it approximates a normal standard deviation.) Only after warmup closes does the monitor switch over to the cheaper, adaptive EWMA update for ongoing tracking — by which point the baseline it's updating from is already trustworthy.

A new unit test, `TestBurstDuringWarmupDoesNotMaskLaterBurst`, now reproduces this exact scenario (a burst as the very last warmup sample, followed by a second comparable burst) and asserts the second burst is still flagged. Re-running the live CLI demo afterward confirmed both bursts now correctly trigger a `volume` alert. This bug is the reason the project's integration testing philosophy (below) matters: a cleanly mocked Kafka client, or a unit test that never happens to arrange an outlier during warmup, would never have surfaced this failure mode — it took watching the real system behave badly against real, somewhat-unpredictable timing to find it.

## Try it yourself

This needs a **real local Kafka broker** — specifically Homebrew's `kafka` formula running in **KRaft mode** (Kafka's newer mode that manages its own metadata internally, so there's no separate ZooKeeper process to install or run) — and does not use Docker.

```bash
brew install kafka
```

```bash
make build
make demo        # starts kafka if it isn't already running, generates a synthetic
                  # stream with injected anomalies, monitors it live, and prints
                  # the alerts dqstream produces — see scripts/run-demo.sh
```

Or run the two pieces yourself, for more control:

```bash
brew services start kafka
./bin/dqstream generate -topic orders -rate 30 -duration 60s -inject-at 15s &
./bin/dqstream monitor -topic orders -window 3s -freshness-threshold 5s
```

`generate` produces a synthetic "order" event stream and, after `-inject-at` elapses, starts mutating a fraction of events on purpose — dropping a required field, changing a field's type, adding a brand-new field, nulling out a field, and periodically firing a volume burst — specifically so `monitor`'s four checks each have something real to catch. `monitor` prints one JSON line per alert to stdout as they're produced.

### Running the tests

```bash
make test              # pure in-process unit tests for every check's logic — no Kafka needed
make test-integration  # boots a REAL ephemeral Kafka broker (KRaft, no ZooKeeper, no Docker)
                        # per test and proves the pipeline against actual produce/consume and
                        # consumer-group offset behavior — not a mock
make test-all
```

The integration suite needs Homebrew's `kafka` formula installed (`brew install kafka`) but does **not** need `brew services start kafka` running beforehand — each test formats and boots its own single-node broker on a randomized port inside a temp directory, and tears it down when the test finishes (see `test/integration/harness.go`).

## What this deliberately doesn't do (yet)

- **Monitor multiple topics or partitions in one process.** Each `Monitor` is one check pipeline for one topic; running several means running several processes (or extending `cmd/dqstream/main.go` to fan out across topics, which is straightforward but wasn't done here since it doesn't exercise any new data-quality logic — it's just more plumbing around the same checks).
- **Use a real stream-processing engine (Flink, Kafka Streams, etc.).** The tumbling-window logic here is hand-rolled in plain Go, which is enough to prove the four checks themselves are correct. Swapping in a heavier execution engine later wouldn't change any of the actual data-quality logic this project is about.
- **Persist alert history or offer a UI/dashboard.** The `alert.Sink` interface exists precisely so a real deployment can plug in whatever it already uses for that — a Slack webhook, PagerDuty, a metrics backend — without touching any of the four checks. dqstream's job ends at "produce a well-formed alert," not "manage its lifecycle."

## Project layout

```
cmd/dqstream/           CLI: `monitor` (run the pipeline) and `generate` (synthetic load + anomalies)
internal/event/         wire envelope + decoding into a generic field map
internal/freshness/     event-time lag tracking vs. a fixed, configured threshold
internal/volume/        EWMA + robust (median/MAD) baseline volume anomaly detection
internal/schema/        warmup-learned field types + drift detection (new/missing/type-changed)
internal/nullrate/      per-field null-rate baseline learning + absolute-threshold drift detection
internal/alert/         Alert type, pluggable Sink interface, cooldown-based Deduper
internal/monitor/       wires all four checks into one per-topic Observe/Flush pipeline
internal/kafkaio/       Kafka reader/writer/topic-creation wrapper (segmentio/kafka-go)
test/integration/       real-Kafka (KRaft, ephemeral, per-test) harness + end-to-end tests
scripts/run-demo.sh     one-command demo: starts Kafka if needed, generates + monitors, prints alerts
docs/ARCHITECTURE.md    deeper design rationale and the full bug write-ups
```

---

Source: [github.com/Kuwar-Vishnudatt/dqstream](https://github.com/Kuwar-Vishnudatt/dqstream) · Licensed under the [MIT License](LICENSE).
