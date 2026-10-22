UV ?= uv
GO ?= go
OUT ?= /tmp/adaptive-routing.wasm

.PHONY: test build
test:
	$(UV) run --no-sync $(GO) test -race ./...
	$(UV) run --no-sync $(GO) vet ./...

build:
	GOOS=wasip1 GOARCH=wasm $(UV) run --no-sync $(GO) build -trimpath -buildmode=c-shared -o $(OUT) .
