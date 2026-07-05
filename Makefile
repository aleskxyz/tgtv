.PHONY: help build install test test-race vet fmt clean docker-build \
	dist dist-checksums all \
	linux-amd64 linux-arm64 \
	cli-asset-linux-amd64 cli-asset-linux-arm64

BIN        := tgtv
CMD        := ./cmd/tgtv
PKG        := ./...
IMAGE      ?= ghcr.io/aleskxyz/tgtv:dev
LDFLAGS    := -s -w
CGO_ENABLED := 0
DIST       ?= dist
BUILD_DIR  ?= .build

CLI_ASSET_LINUX_AMD64 := $(DIST)/tgtv-linux-amd64
CLI_ASSET_LINUX_ARM64 := $(DIST)/tgtv-linux-arm64

.DEFAULT_GOAL := help

help: ## Show targets
	@grep -E '^[a-zA-Z0-9_.-]+:.*##' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*## "}; {printf "  %-20s %s\n", $$1, $$2}'

build: ## Build $(BIN) binary (CGO_ENABLED=0)
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=$(CGO_ENABLED) go build -trimpath -ldflags="$(LDFLAGS)" -o $(BUILD_DIR)/$(BIN) $(CMD)

install: ## Install $(BIN) to $$GOPATH/bin
	CGO_ENABLED=$(CGO_ENABLED) go install -trimpath -ldflags="$(LDFLAGS)" $(CMD)

test: ## Run unit tests
	go test $(PKG)

test-race: ## Run tests with -race
	go test -race $(PKG)

vet: ## Run go vet
	go vet $(PKG)

fmt: ## Run go fmt
	go fmt $(PKG)

clean: ## Remove built binary and release artifacts
	rm -rf $(BUILD_DIR) $(DIST)
	rm -f $(BIN) $(BIN).exe

docker-build: ## Build container image ($(IMAGE))
	docker build -t $(IMAGE) .

linux-amd64:
	@mkdir -p $(DIST)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=linux GOARCH=amd64 \
		go build -trimpath -ldflags="$(LDFLAGS)" -o $(CLI_ASSET_LINUX_AMD64) $(CMD)

linux-arm64:
	@mkdir -p $(DIST)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=linux GOARCH=arm64 \
		go build -trimpath -ldflags="$(LDFLAGS)" -o $(CLI_ASSET_LINUX_ARM64) $(CMD)

dist all: linux-amd64 linux-arm64
	@echo "Done. Binaries in $(DIST)/"
	@ls -lh $(DIST)/

dist-checksums:
	@cd $(DIST) && (ls -A 2>/dev/null | grep -v '^SHA256SUMS$$' | xargs -r sha256sum) > SHA256SUMS

cli-asset-linux-amd64:
	@echo $(CLI_ASSET_LINUX_AMD64)

cli-asset-linux-arm64:
	@echo $(CLI_ASSET_LINUX_ARM64)
