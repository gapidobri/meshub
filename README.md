# meshub

A Go framework for battery-powered LoRa devices on a
[MeshCore](https://github.com/meshcore-dev/meshcore) mesh. You write the
device firmware and the application. meshub handles pairing, config,
events, commands and getting packets across the mesh.

```
device ⇄ (repeaters) ⇄ gateway ⇄ MQTT broker ⇄ meshub backend ⇄ your application
RAK4631     LoRa        T3S3                                    HTTP API, MQTT
```

* **Devices** run the `node` library ([TinyGo](https://tinygo.org)) under
  your program, and sleep between check-ins.
* **Gateways** forward packets between the mesh and MQTT. They hold no keys.
* **The backend** pairs and serves devices, keeps their state, and has a
  web UI.
* **Your application** uses the backend's HTTP API and MQTT topics.

Traffic between devices and the backend is end-to-end encrypted. Each
device reports the *program* it runs, so one backend can serve several
applications.

## Try it

You need Go, TinyGo 0.43 or later, Docker, a LilyGo T3S3 (gateway) and a
RAK4631 (device). All radios must use the same LoRa settings. The default
is MeshCore's EU/UK narrow preset.

```sh
make broker        # local Mosquitto, without authentication
make server        # backend and web UI on http://localhost:8080

make config        # gateway: fill in gateway/cmd/t3s3/config.go (WiFi, broker LAN address)
make flash

make node-config   # device: set HubPublicKey (shown in the web UI) in examples/rak4631/config.go
make node-flash
```

The device then shows as **pending** in the web UI. Approve it, and it
pairs at its next check-in. You can then edit its config and queue
commands, and its events appear as they arrive.

## Device firmware

Start from [`examples/rak4631`](examples/rak4631/), and import
`github.com/gapidobri/meshub/node`:

```go
n, err := node.New(node.Config{
	Radio: radio, Millis: ms, RNG: rng, RTC: clock,
	Storage: store,  // flashstore.Open(machine.Flash, 2)
	Hub:     hubKey, // the backend's public key
	Program: "weather", Firmware: "0.1.0",
	Validate: func(cfg hubproto.Value) error { ... },                                    // optional
	Command:  func(name string, args hubproto.Value) (hubproto.Value, error) { ... }, // optional
})

for {
	n.SetBattery(millivolts)
	n.Sync(ctx) // pair, check in, fetch config, send events, run commands
	interval := n.Config().Get("interval_s").Int(300)

	data := hubproto.Map()
	data.Set("temp_c", hubproto.Float(21.5))
	n.Report("reading", data) // kept in flash until delivered

	// sleep for interval
}
```

Config, events and commands carry JSON-like values, sent as CBOR over the
air. Every device has the built-in commands `logs` and `reset`.

## Applications

* **HTTP API** under `/api/v1` ([OpenAPI](docs/openapi.yaml), with docs at
  `/api/v1/docs`): devices, pairing, config, commands and events.
* **MQTT topics**, published by the backend:

  | Topic | |
  |-------|---|
  | `meshub/app/v1/{program}/devices/{pubkey}/events` | each event, at least once; deduplicate by `id` |
  | `meshub/app/v1/{program}/devices/{pubkey}/status` | the device's state, retained; empty once it is deleted |
  | `meshub/app/v1/{program}/devices/{pubkey}/commands` | command updates, best effort |

  After each connect, fetch what you missed with
  `GET /api/v1/events?program=…&after={last id}`.

In Go, package [`client`](client/) does both. `Listen` delivers each event
exactly once, in order:

```go
c := client.New("http://meshub:8080", client.WithToken(token))
err := c.Listen(ctx, client.Subscription{
	Broker:  "tcp://broker:1883", Username: "weather", Password: pw,
	Program: "weather",
	After:   lastEventID, // the last event you stored
	OnEvent: func(ctx context.Context, e appproto.Event) error { return save(ctx, e) },
	OnStatus: func(d appproto.Device) { /* battery, last check-in, … */ },
})
```

## Deploying

[deploy/](deploy/) has a Docker Compose setup, with a broker that limits
each client to its own topics, and a guide to gateways, TLS, backups and
upgrades. The image is `ghcr.io/gapidobri/meshub`. Every setting is in
[`server/meshub.example.yaml`](server/meshub.example.yaml), and can also be
set with `MESHUB_*` environment variables.

**The database holds the backend's private key.** If you lose it, every
device has to be reflashed. Back it up with `meshub backup`.

## Status

Early. Expect breaking changes before 1.0, listed in the
[changelog](CHANGELOG.md). Known limits:

* one backend instance per mesh identity
* API tokens are not limited to one program
* gateways connect to the broker without TLS, so keep them on a private
  network

More: [protocol](docs/protocol.md) · [development](docs/development.md) ·
[MIT license](LICENSE)
