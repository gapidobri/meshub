# Changelog

All notable changes to meshub. Versions follow [semantic
versioning](https://semver.org); until 1.0, a minor version may break
compatibility, and says how.

## Unreleased

Devices that use notices or byte strings need a backend with this
release; older backends drop them. Nothing else changes on the air.

### Devices

* `Node.Send` sends an event as a notice: once, without an
  acknowledgement and without storing it, for frequent data such as
  positions. It takes one transmission instead of a request and a reply.
* Values can hold byte strings (`hubproto.Bytes`), so compact binary data
  needs no encoding into numbers or text.
* A node that lost its stored state numbered its events from 0 again, and
  the backend dropped them as duplicates of earlier ones. Fresh nodes now
  start their event numbers at random, like request numbers.
* Event numbers are reserved in blocks, so reporting an event no longer
  writes the node's state to flash every time.

### Backend

* Stores notices as events, without replying.
* Byte strings appear in JSON (API, MQTT, web UI) as
  `{"$bytes": "<base64>"}`, and such an object in a config is stored as a
  byte string.

## 0.1.0

The first release.

### Devices

* The `node` library: pairing with approval, check-ins, config sync with
  validation, events delivered at least once and kept in flash, commands,
  remote logs, and fragmentation for large messages. `SetTime` sets the
  clock from the device's own time source, such as GPS.
* `node/flashstore`: a power-loss safe key-value store on raw flash.
* Example firmware for the RAK4631.

### Gateways

* Firmware for the LilyGo T3S3: forwards MeshCore packets to MQTT and
  transmits for the backend, and shows its status on the OLED.

### Backend

* Pairs and serves devices, answering each one through the gateway that
  hears it best, and keeps track of gateways and mesh nodes.
* Web UI for approving devices, editing config, queueing commands and
  reading events, behind an optional login.
* HTTP API under `/api/v1` with bearer tokens, described by
  `docs/openapi.yaml`.
* Device events, status and command updates on the MQTT topics
  `meshub/app/v1/{program}/devices/{pubkey}/…`. Events are delivered at
  least once, also across broker outages and restarts.
* Configuration by YAML file, `MESHUB_*` environment variables and flags.
  The commands are `serve`, `identity`, `backup`, `openapi` and `version`.
* `/healthz` and `/readyz` endpoints, JSON logs, and a graceful shutdown
  that answers the devices already heard.
* A Docker image on scratch, and a production Compose setup with broker
  access rules in `deploy/`.

### Applications

* `proto/appproto`: the values and topics applications exchange with the
  backend.
* `client`: a Go client for the HTTP API. Its `Listen` delivers events
  exactly once, in order, resuming after the last one an application
  stored.
