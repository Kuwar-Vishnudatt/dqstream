// Package integration spins up a real, ephemeral, single-node Kafka broker
// (KRaft mode, no ZooKeeper) per test run, so tests exercise the actual
// Kafka wire protocol rather than a mock. This mirrors the same choice made
// for pgcdc's integration tests against a real local Postgres: a fake
// broker can't tell you whether your consumer-group/offset/partition
// handling actually works against the real thing.
package integration

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const kafkaBinDir = "/opt/homebrew/opt/kafka/bin"

// Broker is a running ephemeral Kafka broker for one test.
type Broker struct {
	Addr string

	cmd *exec.Cmd
	dir string
	t   *testing.T
}

// requireKafkaBinaries skips the test if Homebrew's kafka formula isn't
// installed, rather than failing — this lets the unit-test-only parts of CI
// (or a machine without Homebrew Kafka) run the rest of the suite cleanly.
func requireKafkaBinaries(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(kafkaBinDir, "kafka-storage")); err != nil {
		t.Skipf("kafka binaries not found at %s (brew install kafka): %v", kafkaBinDir, err)
	}
}

// StartBroker formats a fresh KRaft log directory and starts a single-node
// broker+controller on randomized local ports, so multiple test runs (or
// parallel tests) never collide with each other or with a developer's own
// `brew services start kafka` instance on the default 9092/9093 ports.
func StartBroker(t *testing.T) *Broker {
	t.Helper()
	requireKafkaBinaries(t)

	dir := t.TempDir()
	brokerPort := randomPort()
	controllerPort := randomPort()

	uuid := runCapture(t, filepath.Join(kafkaBinDir, "kafka-storage"), "random-uuid")
	uuid = strings.TrimSpace(lastNonEmptyLine(uuid))

	propsPath := filepath.Join(dir, "server.properties")
	props := fmt.Sprintf(`process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:%d
listeners=PLAINTEXT://localhost:%d,CONTROLLER://localhost:%d
advertised.listeners=PLAINTEXT://localhost:%d
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=%s
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
num.partitions=1
group.initial.rebalance.delay.ms=0
`, controllerPort, brokerPort, controllerPort, brokerPort, filepath.Join(dir, "logs"))
	if err := os.WriteFile(propsPath, []byte(props), 0o644); err != nil {
		t.Fatalf("write server.properties: %v", err)
	}

	formatOut, err := exec.Command(filepath.Join(kafkaBinDir, "kafka-storage"), "format",
		"-t", uuid, "-c", propsPath, "--standalone").CombinedOutput()
	if err != nil {
		t.Fatalf("kafka-storage format failed: %v\n%s", err, formatOut)
	}

	logPath := filepath.Join(dir, "kafka.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create kafka log file: %v", err)
	}

	cmd := exec.Command(filepath.Join(kafkaBinDir, "kafka-server-start"), propsPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("start kafka-server-start: %v", err)
	}

	addr := fmt.Sprintf("localhost:%d", brokerPort)
	b := &Broker{Addr: addr, cmd: cmd, dir: dir, t: t}

	if err := waitForBrokerReady(addr, 30*time.Second); err != nil {
		contents, _ := os.ReadFile(logPath)
		b.Stop()
		t.Fatalf("broker did not become ready: %v\n--- kafka.log tail ---\n%s", err, tail(string(contents), 4000))
	}

	t.Cleanup(b.Stop)
	return b
}

// Stop terminates the broker process. Safe to call multiple times.
func (b *Broker) Stop() {
	if b.cmd == nil || b.cmd.Process == nil {
		return
	}
	_ = b.cmd.Process.Kill()
	_, _ = b.cmd.Process.Wait()
	b.cmd = nil
}

func waitForBrokerReady(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := kafka.DialContext(ctx, "tcp", addr)
		cancel()
		if err == nil {
			// A successful TCP+Kafka-protocol dial plus a metadata fetch is
			// what actually proves the broker is serving requests — a bare
			// TCP connect can succeed while the broker is still finishing
			// controller election.
			_, metaErr := conn.Controller()
			conn.Close()
			if metaErr == nil {
				return nil
			}
			lastErr = metaErr
		} else {
			lastErr = err
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for broker at %s: %w", addr, lastErr)
}

func randomPort() int {
	// A random high port per test run. Collisions are possible but
	// astronomically unlikely across a reasonable test run; retry-on-bind
	// failure would be the next step up in rigor if this ever flakes.
	return 20000 + rand.Intn(20000)
}

func runCapture(t *testing.T, name string, args ...string) string {
	t.Helper()
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v failed: %v\n%s", name, args, err, out)
	}
	return string(out)
}

func lastNonEmptyLine(s string) string {
	scanner := bufio.NewScanner(strings.NewReader(s))
	last := ""
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			last = line
		}
	}
	return last
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// UniqueTopic returns a topic name unlikely to collide across test runs.
func UniqueTopic(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), rand.Intn(1_000_000))
}
