# VS Code's TinyGo extension sets these for the editor's Go tools, and its
# integrated terminal inherits them. A GOROOT pointing into TinyGo's cache
# breaks tinygo ("package sync/atomic is not in std"), and the TinyGo
# GOOS/GOARCH/GOFLAGS break go test, so builds here never use them. The
# terminal's PATH also starts with that GOROOT's bin, whose go reports the
# cache as its GOROOT, so the real GOROOT is found without those entries and
# set explicitly (macOS's make looks commands up in the original PATH).
unexport GOOS GOARCH GOFLAGS
export PATH := $(shell printf '%s' "$$PATH" | tr ':' '\n' | grep -v 'tinygo/goroot-' | paste -s -d : -)
export GOROOT := $(shell PATH='$(PATH)' GOROOT= GOFLAGS= go env GOROOT)

TARGET  ?= esp32s3-generic
GATEWAY := ./gateway/cmd/t3s3
CONFIG  := gateway/cmd/t3s3/config.go
NODE    := ./examples/rak4631
NODE_CONFIG := examples/rak4631/config.go
BUILD   := build
PORT    ?=

# noslog compiles out lneto's log/slog debug logging, which also drops
# encoding/json and about 290 KB of code. That keeps the code segment well
# under the ESP32-S3 ROM bootloader's 1 MiB limit ("Invalid image block, can't
# boot"), which TinyGo 0.44 lifts (tinygo-org/tinygo#6001).
TINYGOFLAGS ?= -target $(TARGET) -tags noslog -size short

NODEFLAGS ?= -target rak4631 -size short

ifneq ($(PORT),)
TINYGOFLAGS += -port $(PORT)
NODEFLAGS += -port $(PORT)
endif

.PHONY: all gateway flash monitor config node node-flash node-config test server broker broker-down broker-logs mqtt-sub clean

all: test gateway node

gateway: $(CONFIG)
	@mkdir -p $(BUILD)
	tinygo build $(TINYGOFLAGS) -o $(BUILD)/gateway-t3s3.bin $(GATEWAY)

flash: $(CONFIG)
	tinygo flash $(TINYGOFLAGS) -monitor $(GATEWAY)

monitor:
	tinygo monitor $(if $(PORT),-port $(PORT))

# Creates config.go from the template; edit it before building.
config: $(CONFIG)

# The example node (RAK4631). Flashing uses the UF2 bootloader: double-tap
# reset if it does not enter it by itself.
node: $(NODE_CONFIG)
	@mkdir -p $(BUILD)
	tinygo build $(NODEFLAGS) -o $(BUILD)/node-rak4631.uf2 $(NODE)

node-flash: $(NODE_CONFIG)
	tinygo flash $(NODEFLAGS) -monitor $(NODE)

node-config: $(NODE_CONFIG)

$(CONFIG) $(NODE_CONFIG):
	cp $@.example $@
	@echo "created $@: fill in your settings, then run make again"
	@false

test:
	go vet ./...
	go test -race ./...

# Runs the backend against the local broker; the UI is on http://localhost:8080.
server:
	@mkdir -p $(BUILD)
	go run ./server/cmd/meshub -db $(BUILD)/meshub.db

broker:
	docker compose up -d

broker-down:
	docker compose down

broker-logs:
	docker compose logs -f mosquitto

mqtt-sub:
	docker compose exec mosquitto mosquitto_sub -v -t 'meshcore/#' -t 'meshub/#'

clean:
	rm -rf $(BUILD)
