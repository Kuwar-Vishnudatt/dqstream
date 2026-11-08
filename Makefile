.PHONY: build test test-integration test-all fmt vet clean demo

build:
	go build -o bin/dqstream ./cmd/dqstream

test:
	go test ./internal/... -count=1

test-integration:
	go test ./test/integration/... -count=1 -v

test-all:
	go test ./... -count=1 -timeout 120s

fmt:
	gofmt -l .

vet:
	go vet ./...

demo: build
	./scripts/run-demo.sh

clean:
	rm -rf bin/
