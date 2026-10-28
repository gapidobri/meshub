# meshub

A hub for battery-powered LoRa devices on a [MeshCore](https://github.com/meshcore-dev/meshcore)
mesh. Devices pair with a backend, receive their configuration from it,
report events to it, and sleep in between. Everything runs in Go: the
device side on [TinyGo](https://tinygo.org), on top of
[meshcore-go](https://github.com/gapidobri/meshcore-go).

```
device ⇄ (MeshCore repeaters) ⇄ gateway ⇄ MQTT broker ⇄ backend ⇄ web UI
RAK4631        LoRa              T3S3                    SQLite
                                 WiFi
```

* **Devices** run the `node` library. It pairs with the hub, checks in on
  an interval, fetches new configuration, runs commands from the hub, and
  delivers events. Between check-ins the radio sleeps and the CPU idles.
* **Gateways** are MeshCore radios with WiFi. They publish every packet
  they hear to MQTT and transmit what the backend asks for. They hold no
  keys and decrypt nothing.
* **The backend** decodes the packets, keeps track of gateways, mesh nodes
  and paired devices, answers devices through the gateway that hears them
  best, and serves a web UI for approving devices, editing their config and
  reading their events.

Traffic between devices and the backend is end to end encrypted and
authenticated with MeshCore's own datagrams. See
[docs/protocol.md](docs/protocol.md) for the protocol.

## Repository layout

| Path | What |
|------|------|
| [`node/`](node/) | Device library (TinyGo, hardware independent): pairing, config sync, events, commands, remote logs |
| [`node/flashstore/`](node/flashstore/) | Power-loss safe key-value store on raw flash, the device's `node.Storage` |
| [`examples/rak4631/`](examples/rak4631/) | Example device firmware for the RAK4631, and the template for real ones |
| [`gateway/`](gateway/) | Gateway bridge between radio and MQTT; [`gateway/cmd/t3s3`](gateway/cmd/t3s3/) is the LilyGo T3S3 firmware |
| [`proto/gwproto/`](proto/gwproto/) | Gateway ↔ backend MQTT topics and encodings, compatible with [meshcoretomqtt](https://github.com/Cisien/meshcoretomqtt) |
| [`proto/hubproto/`](proto/hubproto/) | Device ↔ hub messages, and the CBOR values that carry config and events |
| [`proto/appproto/`](proto/appproto/) | Backend ↔ application contract: the HTTP API's values and the MQTT topics |
| [`server/`](server/) | Backend: MQTT ingest, the hub, SQLite store, web UI, HTTP API and application topics |
| [`docs/protocol.md`](docs/protocol.md) | The device ↔ hub protocol |

## Hardware

| Role | Board | Notes |
|------|-------|-------|
| Gateway | LilyGo T3S3 (ESP32-S3, SX1262) | Needs WiFi and USB power |
| Device | RAK4631 (nRF52840, SX1262) on a WisBlock base | Battery powered |

All radios must share the same LoRa settings. The defaults are MeshCore's
EU/UK narrow preset (869.618 MHz, 62.5 kHz, SF8), with a 10% transmit duty
cycle for that sub-band.

## Getting started

You need Go, [TinyGo](https://tinygo.org/getting-started/) 0.43 or later,
and Docker for the test broker.

### 1. Broker and backend

```sh
make broker   # Mosquitto in Docker, MQTT on :1883
make server   # backend on http://localhost:8080, data in build/meshub.db
```

The backend creates its MeshCore identity on first start. Its public key is
shown in the **Hub** panel of the web UI: devices are built with it.

The backend is configured with a YAML file, `MESHUB_*` environment
variables and flags, in increasing order of precedence. See
[`server/meshub.example.yaml`](server/meshub.example.yaml) for every
setting and its default. Commands (`go run ./server/cmd/meshub -h`):

| Command | |
|---------|---|
| `meshub serve` | run the backend; `--config`, `--db`, `--http`, `--mqtt`, `--log-level` and `--auto-pair` override the config |
| `meshub identity` | print the hub's public key, which devices are built with |
| `meshub openapi` | print the HTTP API's OpenAPI description (`-f json` for JSON) |
| `meshub version` | print the version |

The database (`db.path`) holds the hub's private key: back it up. Set
`api.tokens` and `ui.password` on anything but a trusted network; without
them the API and web UI are open, and the backend warns about it at start.

### Running in Docker

```sh
make docker   # builds meshub:<git describe>, a static binary on scratch
docker run -v meshub-data:/data -p 8080:8080 \
  -e MESHUB_MQTT_BROKER=tcp://broker:1883 -e MESHUB_API_TOKENS=$(openssl rand -hex 32) \
  meshub:<version>
```

The database lives in the `/data` volume. `/healthz` reports the process
alive, and the image's health check uses it; `/readyz` also needs the
database and the broker connection.

## Building applications on meshub

Applications use the hub as a service, through the HTTP API and MQTT. Their
devices run the `node` library, and are told apart by the program they
report when they join (`node.Config.Program`). Package
[`proto/appproto`](proto/appproto/) defines what applications exchange with
the backend.

* **HTTP API** under `/api/v1`, described by
  [`docs/openapi.yaml`](docs/openapi.yaml) and browsable at `/api/v1/docs`:
  devices and their pairing, config, commands and events, and the mesh.
  Requests carry `Authorization: Bearer <one of api.tokens>`.
* **MQTT** topics the backend publishes on, for reacting to devices as they
  report:

  | Topic | |
  |-------|---|
  | `meshub/app/v1/{program}/devices/{pubkey}/events` | each event, at least once: deduplicate by `id` |
  | `meshub/app/v1/{program}/devices/{pubkey}/status` | the device, retained; republished on every change, emptied when it is deleted |
  | `meshub/app/v1/{program}/devices/{pubkey}/commands` | a command when it is queued, finished or cancelled (best effort) |

  An application subscribes to `meshub/app/v1/{program}/#` with a
  persistent session, and on start fills any gap with
  `GET /api/v1/events?program={program}&after={last id it has}`.

### 2. Gateway

```sh
make config   # creates gateway/cmd/t3s3/config.go from the template
# fill in WiFi, the broker's LAN address (not localhost), region code and radio
make flash    # build, flash and open the serial monitor
```

The gateway appears as online in the web UI once it reaches the broker.
Its OLED shows the boot steps, then its status: uptime, IP and WiFi signal,
broker connection, radio settings, packet counts, the last packet's signal
and the noise floor. The screen blanks after five minutes to avoid burn-in
(it stays on while the broker is unreachable); **BOOT** turns it on or off.
Press **Send advert** there to check the downlink: the hub then shows up as
a contact in the MeshCore phone app.

### 3. Device

```sh
make node-config   # creates examples/rak4631/config.go from the template
# set HubPublicKey to the key from the web UI, and match the gateway's radio settings
make node-flash    # double-tap reset if the board does not enter its UF2 bootloader
```

The device asks to pair and appears as **pending** under Devices in the web
UI. Approve it; at its next check-in (a minute later while pending) it
pairs. Its page lets you edit its config as JSON and queue commands, both
of which it picks up at its next check-in. It lists the events the device
reports, and the results of its commands.

The example reads two config keys, `interval_s` (seconds between
check-ins, at least 10) and `pin_holdoff_s`, and rejects a config that sets
them out of range; the web UI shows why. It reports a `pin` event when its
wake pin (P0.17) goes high. Every device runs the commands `logs` (its
recent log lines) and `reset` (drop its queued events, config and route);
the example adds `status`.

## Writing a device program

Start from [`examples/rak4631`](examples/rak4631/). The library takes the
radio, clocks and storage from your program and does the rest:

```go
n, err := node.New(node.Config{
	Radio: radio, Millis: ms, RNG: rng, RTC: clock,
	Storage: store,            // flashstore.Open(machine.Flash, 2)
	Hub:     hubKey,           // the hub's public key
	Program: "hydrant", Firmware: "0.1.0",
	Validate: func(cfg hubproto.Value) error { ... },  // optional: reject a bad config
	Command: func(name string, args hubproto.Value) (hubproto.Value, error) {
		// optional: run the hub's commands, e.g. "read" to take a reading now
	},
})

for {
	n.SetWakeReason(reason)
	n.SetBattery(millivolts)
	err := n.Sync(ctx)         // pair if needed, check in, fetch new config, send events
	threshold := n.Config().Get("audio.threshold").Int(40)

	// ... do the work, then report results:
	data := hubproto.Map()
	data.Set("duration_s", hubproto.Int(93))
	n.Report("flow", data)     // stamped with the time, stored in flash until the hub acknowledges it
	n.Log("flow ended")        // kept in RAM for the "logs" command

	radio.Sleep()              // then sleep until the next check-in or a wake pin
	// ...
	radio.Begin()
}
```

Config, event data and command arguments and results are JSON-like values
(`hubproto.Value`), encoded as CBOR on the air and shown as JSON in the web
UI. An event can be up to about 2 KB; one larger than a datagram is sent
in fragments.

Requests that get no reply are retried after a random backoff, and a check-in
starts after a random delay of up to 2 s, so devices woken together do not
collide. The library learns how quickly the hub answers and waits only
that long, so a lost packet costs little radio time. `node.Config` has the
knobs.

## Development

```sh
make test    # go vet and all tests, with the race detector
make all     # tests, then both firmwares
```

The tests run without hardware. The node library has unit tests against a
fake hub (request numbering, retries, the event queue, corrupt stored
state), the end-to-end tests drive it against the real hub through
simulated gateways and repeaters, and the flash store is tested by cutting
power at every byte it writes.

| Target | |
|--------|---|
| `make gateway` / `make flash` | build / flash the T3S3 gateway |
| `make node` / `make node-flash` | build / flash the RAK4631 example device |
| `make monitor` | serial console (`PORT=` picks the port) |
| `make server` | run the backend |
| `make broker`, `broker-down`, `broker-logs` | the test broker |
| `make mqtt-sub` | watch all gateway traffic |

Notes:

* **meshcore-go checkout.** A gitignored `go.work` can point meshub at a
  local `../meshcore-go` while changes there are unreleased. Without one,
  meshub builds against the version in `go.mod`.
* **VS Code.** The TinyGo extension sets `GOROOT`, `GOOS`, `GOARCH` and
  `GOFLAGS` (and `PATH`) in the integrated terminal for the editor's
  tools. The Makefile undoes that for its builds; plain `go` and `tinygo`
  commands in that terminal still see it. Set
  `"go.terminal.activateEnvironment": false` to keep it out of terminals.
* **ESP32-S3 image size.** The gateway builds with `-tags noslog`, which
  keeps its code under the ESP32-S3 ROM bootloader's 1 MiB segment limit
  (about 960 KB with the display). TinyGo 0.44 removes that limit.

## Status

Working end to end on real hardware: a T3S3 gateway and a RAK4631 device
pair, sync config and deliver events through the backend.

Not yet verified on hardware: the RAK4631's battery reading (divider ratio
taken from other firmware), its sleep current, and the wake pin assignment
on your base board.

## Future features

### Device library

* **Optional repeating** for mains-powered devices.
* **Read-out protection.** Enable the nRF52's APPROTECT in production builds
  so the device key cannot be read over the debug port.

### Backend

* **Pairing by claim code.** Accept devices whose claim code is known
  without manual approval.
* **Alerts.** Notify on events and on devices or gateways that miss their
  check-ins (webhook, ntfy, Telegram).
* **Embedded broker.** Run the MQTT broker inside the backend, for a single
  binary.
* **Authentication** for the web UI.
* **Map and topology views** from advert locations and packet paths.
* **Exports** to Home Assistant, InfluxDB or Prometheus.

### Gateway

* **Setup without reflashing:** WiFi and broker credentials over USB serial,
  and radio settings from the backend over MQTT.
* **TLS** to the broker, once TinyGo's network stack supports it.

### Not planned

* **Firmware updates over LoRa.** LoRa's data rate makes them impractical;
  devices report their firmware version, and are updated over USB or BLE.
