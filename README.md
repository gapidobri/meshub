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
  an interval, fetches new configuration, and delivers events. Between
  check-ins the radio sleeps and the CPU idles.
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
| [`node/`](node/) | Device library (TinyGo, hardware independent): pairing, config sync, events |
| [`node/flashstore/`](node/flashstore/) | Power-loss safe key-value store on raw flash, the device's `node.Storage` |
| [`examples/rak4631/`](examples/rak4631/) | Example device firmware for the RAK4631, and the template for real ones |
| [`gateway/`](gateway/) | Gateway bridge between radio and MQTT; [`gateway/cmd/t3s3`](gateway/cmd/t3s3/) is the LilyGo T3S3 firmware |
| [`proto/gwproto/`](proto/gwproto/) | Gateway ↔ backend MQTT topics and encodings, compatible with [meshcoretomqtt](https://github.com/Cisien/meshcoretomqtt) |
| [`proto/hubproto/`](proto/hubproto/) | Device ↔ hub messages, and the CBOR values that carry config and events |
| [`server/`](server/) | Backend: MQTT ingest, the hub, SQLite store and web UI |
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

Backend flags (`go run ./server/cmd/meshub -h`):

| Flag | Default | |
|------|---------|---|
| `-mqtt` | `tcp://localhost:1883` | broker URL |
| `-mqtt-user`, `-mqtt-pass` | | broker credentials; the password can also come from `$MESHUB_MQTT_PASSWORD` |
| `-db` | `meshub.db` | SQLite database. It holds the hub's private key: back it up |
| `-http` | `:8080` | web UI address |
| `-name` | `meshub` | name the hub advertises on the mesh |
| `-advert-interval` | `12h` | how often the hub floods its advert, `0` to disable |
| `-retention` | `168h` | how long to keep raw packets |
| `-auto-pair` | `false` | accept every device without approval (development only) |

The web UI has no login. Keep it on a trusted network.

### 2. Gateway

```sh
make config   # creates gateway/cmd/t3s3/config.go from the template
# fill in WiFi, the broker's LAN address (not localhost), region code and radio
make flash    # build, flash and open the serial monitor
```

The gateway appears as online in the web UI once it reaches the broker.
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
pairs. Its page lets you edit its config as JSON, which it fetches at its
next check-in, and lists the events it reports.

The example reads two config keys, `interval_s` (seconds between
check-ins) and `pin_holdoff_s`, and reports a `pin` event when its wake pin
(P0.17) goes high.

## Writing a device program

Start from [`examples/rak4631`](examples/rak4631/). The library takes the
radio, clocks and storage from your program and does the rest:

```go
n, err := node.New(node.Config{
	Radio: radio, Millis: ms, RNG: rng, RTC: clock,
	Storage: store,            // flashstore.Open(machine.Flash, 2)
	Hub:     hubKey,           // the hub's public key
	Program: "hydrant", Firmware: "0.1.0",
})

for {
	n.SetWakeReason(reason)
	n.SetBattery(millivolts)
	err := n.Sync(ctx)         // pair if needed, check in, fetch new config, send events
	threshold := n.Config().Get("audio.threshold").Int(40)

	// ... do the work, then report results:
	data := hubproto.Map()
	data.Set("duration_s", hubproto.Int(93))
	n.Report("flow", data)     // stored in flash until the hub acknowledges it

	radio.Sleep()              // then sleep until the next check-in or a wake pin
	// ...
	radio.Begin()
}
```

Config and event data are JSON-like values (`hubproto.Value`), encoded as
CBOR on the air and shown as JSON in the web UI.

## Development

```sh
make test    # go vet and all tests, with the race detector
make all     # tests, then both firmwares
```

The tests run without hardware. The end-to-end tests drive the real node
library against the real hub through simulated gateways and repeaters, and
the flash store is tested by cutting power at every byte it writes.

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
  keeps its code under the ESP32-S3 ROM bootloader's 1 MiB segment limit.
  TinyGo 0.44 removes that limit.

## Status

Working end to end on real hardware: a T3S3 gateway and a RAK4631 device
pair, sync config and deliver events through the backend.

Not yet verified on hardware: the RAK4631's battery reading (divider ratio
taken from other firmware), its sleep current, and the wake pin assignment
on your base board.

## Future features

### Device library

* **Retries with jitter.** A lost packet currently fails the whole
  check-in. Retry requests a couple of times with random backoff, and add a
  random delay before the first transmission so devices waking together
  (after a power cut, say) do not collide.
* **Event timestamps.** Events carry no time of their own; the hub records
  when it received them, which is wrong for events queued while it was
  unreachable. Stamp them on the device with the clock the hub sets at every
  check-in.
* **Event batching.** Each event is its own request and reply. Packing
  several into one message saves airtime and battery after an outage.
* **Adaptive reply timeout.** Every request listens up to 8 s for the hub,
  which usually answers within a second or two. Learning the typical reply
  time per route would cut most of the radio-on time a lost packet costs.
* **Commands from the hub,** such as "take a reading now", "send logs" or
  "reset state", fetched at check-in the way config is.
* **Config validation.** Let the program accept or reject a new config, and
  report the outcome to the hub so a bad config shows in the web UI.
* **Remote logs.** A small log buffer the hub can fetch from a device in the
  field.
* **Larger payloads.** Events larger than one datagram, split over several.
* **Optional repeating** for mains-powered devices.
* **Read-out protection.** Enable the nRF52's APPROTECT in production builds
  so the device key cannot be read over the debug port.
* **Unit tests** for the library on its own (request numbering, the event
  queue, corrupt stored state); today it is tested through the hub's
  end-to-end tests.

The timestamp, batching, command and validation changes touch the protocol,
so they are best done together, in one protocol version, before devices are
deployed.

### Backend

* **Pairing by claim code.** Accept devices whose claim code is known
  without manual approval.
* **Sleep hints.** Let the hub suggest each device's next check-in (the
  protocol carries it; the hub always sends 0 today).
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
