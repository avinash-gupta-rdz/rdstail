BINARY      := rdstail
PKG         := github.com/avinash-gupta-rdz/rdstail
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE        := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS     := -s -w \
               -X $(PKG)/internal/cli.version=$(VERSION) \
               -X $(PKG)/internal/cli.commit=$(COMMIT) \
               -X $(PKG)/internal/cli.buildDate=$(DATE)

GO          ?= go
GOFLAGS     ?=
CGO_ENABLED ?= 0

.PHONY: all build test vet lint tidy run validate clean cover e2e

all: build

build:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o bin/$(BINARY) ./cmd/$(BINARY)

test:
	$(GO) test ./... -race -count=1

cover:
	$(GO) test ./... -race -count=1 -coverprofile=coverage.out
	$(GO) tool cover -html=coverage.out -o coverage.html

vet:
	$(GO) vet ./...

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "install golangci-lint: https://golangci-lint.run"; exit 1; }
	golangci-lint run ./...

tidy:
	$(GO) mod tidy

run: build
	./bin/$(BINARY) run -c examples/config.yaml

validate: build
	./bin/$(BINARY) validate -c examples/config.yaml

e2e:
	$(GO) test -tags=e2e ./... -count=1

clean:
	rm -rf bin dist coverage.out coverage.html
