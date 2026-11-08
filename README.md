# dqstream

A streaming data-quality monitor: it consumes a Kafka topic and continuously
watches the shape of the data going past, not just whether the pipeline is
"up." Four independent checks run per topic, each one comparing live traffic
against a baseline the monitor itself learns from the stream — no hand-tuned
per-field thresholds to maintain:

- **Freshness** — event-time lag (producer's claimed time vs. now). Catches
  a stuck, backlogged, or silently-stalled upstream producer.
- **Volume** — per-window event count vs. a learned (robust) baseline.
  Catches a producer going quiet, or an unexpected traffic spike.
- **Schema drift** — new fields, missing required fields, and type changes,
  learned from a warmup window. Catches a silent upstream schema change
  before it breaks a downstream consumer.
- **Null-rate drift** — per-field null rate vs. its learned baseline. Catches
  the data-quality regression that never violates the schema at all: a field
  that's still present and still the right type, just empty far more often.

```
Kafka topic ──▶ consumer-group reader ──▶ decode ──▶ Monitor
                                                        │
                        per-event: freshness, schema drift
                        per-window (tumbling): volume, null-rate
                                                        │
                                                 alert.Sink (deduped)
```

## Why these four checks, and why learned baselines

The research behind this portfolio's Data Engineer projects pointed at the
same thing from a different angle each time: idempotent/replayable
pipelines and schema contracts are now table-stakes, but the actual
on-call-page-worthy failures are usually *quieter* than a crash — a producer
silently stalls, a field quietly starts being null, a schema changes and
nothing downstream errors, it just gets subtly wrong data. A monitor that
only checks "is the consumer lagging" or "did deserialization throw" misses
all of these. Each check here targets one of them specifically, and each
learns its own baseline (a warmup period, or an EWMA/robust estimator) so it
doesn't need a human to pick a magic threshold per field per topic before
it's useful.

## Quickstart

Requires a local Kafka broker. On macOS:

```bash
brew install kafka
```

```bash
make build
make demo        # starts kafka if needed, generates + monitors live, prints alerts
```

Or manually:

```bash
brew services start kafka
./bin/dqstream generate -topic orders -rate 30 -duration 60s -inject-at 15s &
./bin/dqstream monitor -topic orders -window 3s -freshness-threshold 5s
```

`generate` produces a synthetic order-event stream and, after `-inject-at`,
starts mutating a fraction of events (dropping a required field, changing a
field's type, adding a new field, nulling out a field, and periodically
bursting volume) specifically so `monitor`'s checks have something real to
catch. `monitor` prints one JSON line per alert to stdout.

## Testing

```bash
make test              # all check logic, pure in-process, no Kafka needed
make test-integration  # spins up a REAL ephemeral Kafka broker (KRaft, no
                        # ZooKeeper) per test — not a mock — and proves the
                        # pipeline against actual produce/consume/consumer-
                        # group-offset behavior
make test-all
```

The integration suite requires Homebrew's `kafka` formula to be installed
(`brew install kafka`) but does **not** require `brew services start kafka`
or Docker — each test formats and boots its own single-node broker on
randomized ports in a temp directory and tears it down afterward (see
`test/integration/harness.go`), the same approach this portfolio's `pgcdc`
project took with a real local Postgres instead of a mock WAL.

## Real bugs found and fixed

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full narrative.
Briefly: a schema-drift bug where a field's type got recorded as "known"
before the drift check ever ran, silently swallowing every `new_field` alert
after the schema's warmup window closed; a volume-anomaly bug where a
perfectly-constant baseline (zero variance) made the z-score undefined and
so *no* deviation could ever be flagged; a clock-injection bug where the
freshness sub-check kept its own private clock, so overriding the outer
monitor's clock in a test silently did nothing and the check used real
wall-clock time instead; and — the one actually found by running the CLI
against a live local Kafka broker, not by a unit test — a volume burst
landing inside the warmup window poisoning a naive EWMA baseline badly
enough that an equally-sized second burst right afterward stopped being
detected.

## Project layout

```
internal/event/      wire envelope + decoding
internal/schema/      schema learning + drift detection
internal/freshness/   event-time lag tracking
internal/volume/       EWMA/robust-baseline volume anomaly detection
internal/nullrate/     per-field null-rate drift detection
internal/alert/        Alert type, Sink interface, cooldown dedup wrapper
internal/monitor/      wires all four checks into one per-topic pipeline
internal/kafkaio/       Kafka reader/writer/topic-creation wrapper
cmd/dqstream/           CLI: `monitor`, `generate`
test/integration/       real-Kafka harness + end-to-end tests
docs/ARCHITECTURE.md    design rationale and the bugs found while building it
```

## What's deliberately out of scope (v1)

- **Multiple topics/partitions in one process.** The monitor is one pipeline
  per topic; running several is a matter of running several processes
  (or extending `main.go` to fan out — straightforward, just not done here
  since it doesn't exercise any new data-quality logic).
- **A real stream-processing engine (Flink, Kafka Streams).** The windowed
  computation here is hand-rolled in Go, which is enough to prove the checks
  themselves are correct; swapping the execution engine wouldn't change any
  of the actual data-quality logic this project is about.
- **Persistent alert history / a UI.** `alert.Sink` is pluggable
  specifically so a real deployment can wire in whatever it already uses
  (Slack webhook, PagerDuty, a metrics backend) without touching the checks.
