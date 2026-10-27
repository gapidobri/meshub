TARGET  ?= esp32s3-generic
GATEWAY := ./gateway/cmd/t3s3
CONFIG  := gateway/cmd/t3s3/config.go
BUILD   := build
PORT    ?=

# noslog compiles out lneto's log/slog debug logging, which also drops
# encoding/json and about 290 KB of code. That keeps the code segment well
# under the ESP32-S3 ROM bootloader's 1 MiB limit ("Invalid image block, can't
# boot"), which TinyGo 0.44 lifts (tinygo-org/tinygo#6001).
TINYGOFLAGS ?= -target $(TARGET) -tags noslog -size short

ifneq ($(PORT),)
TINYGOFLAGS += -port $(PORT)
endif

.PHONY: all gateway flash monitor config test broker broker-down broker-logs mqtt-sub clean

all: test gateway

gateway: $(CONFIG)
	@mkdir -p $(BUILD)
	tinygo build $(TINYGOFLAGS) -o $(BUILD)/gateway-t3s3.bin $(GATEWAY)

flash: $(CONFIG)
	tinygo flash $(TINYGOFLAGS) -monitor $(GATEWAY)

monitor:
	tinygo monitor $(if $(PORT),-port $(PORT))

# Creates config.go from the template; edit it before building.
config: $(CONFIG)

$(CONFIG):
	cp $(CONFIG).example $(CONFIG)
	@echo "created $(CONFIG): fill in your settings, then run make again"
	@false

test:
	go vet ./...
	go test -race ./...

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
