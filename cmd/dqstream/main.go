// Command dqstream is the CLI: `monitor` runs the data-quality monitor
// against a live Kafka topic, and `generate` produces a synthetic event
// stream (with optional injected anomalies) for demos and manual testing.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Kuwar-Vishnudatt/dqstream/internal/alert"
	"github.com/Kuwar-Vishnudatt/dqstream/internal/event"
	"github.com/Kuwar-Vishnudatt/dqstream/internal/kafkaio"
	"github.com/Kuwar-Vishnudatt/dqstream/internal/monitor"
	kafka "github.com/segmentio/kafka-go"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "monitor":
		runMonitor(os.Args[2:])
	case "generate":
		runGenerate(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: dqstream <monitor|generate> [flags]")
}

func runMonitor(args []string) {
	fs := flag.NewFlagSet("monitor", flag.ExitOnError)
	brokers := fs.String("brokers", "localhost:9092", "comma-separated Kafka broker addresses")
	topic := fs.String("topic", "", "Kafka topic to monitor (required)")
	group := fs.String("group", "dqstream-monitor", "consumer group id")
	window := fs.Duration("window", 5*time.Second, "tumbling window size for windowed checks")
	freshness := fs.Duration("freshness-threshold", 30*time.Second, "max acceptable event-time lag")
	alertCooldown := fs.Duration("alert-cooldown", 2*time.Minute, "min time between repeat alerts on the same check+key")
	fs.Parse(args)

	if *topic == "" {
		log.Fatal("monitor: -topic is required")
	}

	sink := alert.NewDeduper(alert.NewConsoleSink(os.Stdout), *alertCooldown)
	cfg := monitor.Config{Topic: *topic, FreshnessThreshold: *freshness}
	m := monitor.New(cfg, sink)

	reader := kafkaio.NewReader(strings.Split(*brokers, ","), *topic, *group)
	defer reader.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(*window)
	defer ticker.Stop()

	msgCh := make(chan kafka.Message)
	errCh := make(chan error, 1)
	go func() {
		for {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				errCh <- err
				return
			}
			msgCh <- msg
		}
	}()

	log.Printf("dqstream monitor: topic=%s window=%s freshness_threshold=%s", *topic, *window, *freshness)
	for {
		select {
		case <-ctx.Done():
			m.Flush()
			log.Println("shutting down")
			return
		case err := <-errCh:
			log.Printf("reader error (exiting): %v", err)
			return
		case msg := <-msgCh:
			rec, err := event.Decode(msg.Value)
			if err != nil {
				log.Printf("skipping undecodable message at offset %d: %v", msg.Offset, err)
				continue
			}
			m.Observe(rec)
		case <-ticker.C:
			m.Flush()
		}
	}
}

func runGenerate(args []string) {
	fs := flag.NewFlagSet("generate", flag.ExitOnError)
	brokers := fs.String("brokers", "localhost:9092", "comma-separated Kafka broker addresses")
	topic := fs.String("topic", "", "Kafka topic to produce to (required)")
	rate := fs.Int("rate", 20, "events per second")
	duration := fs.Duration("duration", 60*time.Second, "how long to run")
	injectAt := fs.Duration("inject-at", 20*time.Second, "when to start injecting anomalies")
	fs.Parse(args)

	if *topic == "" {
		log.Fatal("generate: -topic is required")
	}

	brokerList := strings.Split(*brokers, ",")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := kafkaio.EnsureTopic(ctx, brokerList, *topic, 1); err != nil {
		log.Printf("ensure topic (continuing, may already exist): %v", err)
	}

	writer := kafkaio.NewWriter(brokerList, *topic)
	defer writer.Close()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	interval := time.Second / time.Duration(*rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	deadline := time.Now().Add(*duration)
	injectDeadline := time.Now().Add(*injectAt)
	n := 0
	log.Printf("dqstream generate: topic=%s rate=%d/s duration=%s inject-at=%s", *topic, *rate, *duration, *injectAt)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		injecting := time.Now().After(injectDeadline)
		payload := syntheticPayload(rng, n, injecting)
		eventTime := time.Now()
		if injecting && rng.Float64() < 0.3 {
			eventTime = eventTime.Add(-45 * time.Second) // simulate a stale/backlogged event
		}

		env := event.Envelope{EventTime: eventTime, Payload: payload}
		data, _ := marshalEnvelope(env)
		if err := writer.WriteMessages(ctx, kafka.Message{Value: data}); err != nil {
			log.Printf("write failed: %v", err)
		}
		n++
		if injecting && n%(*rate*5) == 0 {
			// Periodic burst to also exercise the volume-anomaly check.
			burst := make([]kafka.Message, 0, *rate*5)
			for i := 0; i < *rate*5; i++ {
				p := syntheticPayload(rng, n+i, injecting)
				e := event.Envelope{EventTime: time.Now(), Payload: p}
				d, _ := marshalEnvelope(e)
				burst = append(burst, kafka.Message{Value: d})
			}
			if err := writer.WriteMessages(ctx, burst...); err != nil {
				log.Printf("burst write failed: %v", err)
			}
			log.Printf("injected a volume burst of %d events", len(burst))
		}
	}
	log.Printf("generate: wrote %d events", n)
}
