VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo 0.0.0-dev)
LDFLAGS := -X main.version=$(VERSION)

.PHONY: build test vet fmt check clean

build:
	go build -ldflags "$(LDFLAGS)" -o bin/sync-mcp ./cmd/sync-mcp

test:
	go test ./... -race

vet:
	go vet ./...

fmt:
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed on:"; gofmt -l .; exit 1)

check: fmt vet test
	go build ./...

clean:
	rm -rf bin
