# Developing meshub

This guide is for working on meshub itself. If you are building devices or
applications on it, the [README](../README.md) is what you need.

## Repository layout

| Path | What |
|------|------|
| [`node/`](../node/) | Device library (TinyGo, hardware independent): pairing, config sync, events, commands, remote logs |
| [`node/flashstore/`](../node/flashstore/) | Power-loss safe key-value store on raw flash, the device's `node.Storage` |
| [`examples/rak4631/`](../examples/rak4631/) | Example device firmware for the RAK4631, and the template for real ones |
| [`gateway/`](../gateway/) | Gateway bridge between radio and MQTT; [`gateway/cmd/t3s3`](../gateway/cmd/t3s3/) is the LilyGo T3S3 firmware |
| [`proto/gwproto/`](../proto/gwproto/) | Gateway ↔ backend MQTT topics and encodings, compatible with [meshcoretomqtt](https://github.com/Cisien/meshcoretomqtt) |
| [`proto/hubproto/`](../proto/hubproto/) | Device ↔ hub messages, and the CBOR values that carry config and events |
| [`proto/appproto/`](../proto/appproto/) | Backend ↔ application contract: the HTTP API's values and the MQTT topics |
| [`client/`](../client/) | Go client for applications |
| [`server/`](../server/) | The backend: MQTT ingest, the hub, the SQLite store, web UI, HTTP API and application topics |
| [`deploy/`](../deploy/) | Production Docker Compose setup and broker access rules |
| [`docs/`](.) | Protocol, the generated OpenAPI description, this guide |

### Modules

The repository has two Go modules:

* **`github.com/gapidobri/meshub`** (the root module): `node`, `gateway`,
  `proto` and `client`. These are what devices and applications import, so
  the module depends only on what they need.
* **`github.com/gapidobri/meshub/server`**: the backend. Its `go.mod` has a
  `replace` that points the root module at `../`, so the backend always
  builds against the `proto` packages in the same checkout. It is built
  from the repository (`make server`, the Dockerfile), not installed with
  `go install`.

A gitignored `go.work` can tie both modules to a local `../meshcore-go`
while changes there are unreleased:

```
go 1.27.3

use (
	.
	./server
	../meshcore-go
)
```

Without one, both modules build against the meshcore-go version in their
`go.mod`, as CI does.

## Building and testing

```sh
make test    # go vet and all tests of both modules, with the race detector
make all     # tests, then both firmwares
```

| Target | |
|--------|---|
| `make gateway` / `make flash` | build / flash the T3S3 gateway |
| `make node` / `make node-flash` | build / flash the RAK4631 example device |
| `make monitor` | serial console (`PORT=` picks the port) |
| `make server` | run the backend against the local broker |
| `make openapi` | regenerate `docs/openapi.yaml` from the API's code |
| `make docker` | build the backend's image |
| `make broker`, `broker-down`, `broker-logs` | the local test broker |
| `make mqtt-sub` | watch all MQTT traffic |

The tests run without hardware:

* The node library has unit tests against a fake hub, covering request
  numbering, retries, the event queue and corrupt stored state.
* The hub's end-to-end tests drive a real node against the real hub,
  through simulated gateways and repeaters.
* The flash store is tested by cutting power at every byte it writes.
* `server/internal/e2e` tests the backend as applications use it, through
  package `client`, over HTTP and an embedded MQTT broker.
* A test fails when `docs/openapi.yaml` no longer matches the API. Run
  `make openapi` to update it.

## Notes

* **VS Code.** The TinyGo extension sets `GOROOT`, `GOOS`, `GOARCH`,
  `GOFLAGS` and `PATH` in the integrated terminal, for the editor's tools.
  The Makefile undoes that for its builds, but plain `go` and `tinygo`
  commands in that terminal still see it. Set
  `"go.terminal.activateEnvironment": false` to keep it out of terminals.
* **ESP32-S3 image size.** The gateway builds with `-tags noslog`, which
  keeps its code under the ESP32-S3 ROM bootloader's 1 MiB segment limit
  (about 960 KB with the display). TinyGo 0.44 removes that limit.
* **The API.** It is built with [huma](https://huma.rocks), so the OpenAPI
  description is generated from the Go types. Its values are in
  `proto/appproto`, which the MQTT messages share. Within v1, add fields
  but do not change or remove them: clients ignore fields they do not know.
* **Event IDs** only increase, but may skip numbers. Applications resume
  from them, so they must never be reused.

## CI and releases

[CI](../.github/workflows/ci.yml) runs these on every push and pull
request:

* gofmt and `go mod tidy` checks
* `make test`
* both firmware builds
* the Docker image build

To release:

1. Move the changelog's Unreleased entries under the new version.
2. Tag it, for example `git tag v0.1.0 && git push origin v0.1.0`.

CI then pushes the image as `ghcr.io/gapidobri/meshub:0.1.0`, `:0.1` and
`:latest`. Applications import the root module at the same tag.

## Roadmap

### Device library

* **Optional repeating** for mains-powered devices.
* **Read-out protection.** Enable the nRF52's APPROTECT in production
  builds, so that the device key cannot be read over the debug port.

### Backend

* **Per-program API tokens and broker users**, so that applications
  sharing a backend cannot see each other's devices.
* **Webhooks** for applications that do not use MQTT, and **commands over
  MQTT**.
* **Metrics** for Prometheus: gateways online, check-ins, event publish
  lag, transmit failures.
* **Pairing by claim code.** Accept devices whose claim code is known,
  without manual approval.
* **Alerts** on events, and on devices or gateways that miss their
  check-ins (webhook, ntfy, Telegram).
* **TLS client certificates** and custom CAs for the broker connection.
* **Embedded broker**, for a single binary.
* **Map and topology views** from advert locations and packet paths.

### Gateway

* **Setup without reflashing:** WiFi and broker credentials over USB
  serial, and radio settings from the backend over MQTT.
* **TLS** to the broker, once TinyGo's network stack supports it.

### Not planned

* **Firmware updates over LoRa.** LoRa's data rate makes them impractical.
  Devices report their firmware version, and are updated over USB or BLE.
