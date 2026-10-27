# meshub hub protocol

How nodes and the hub talk over MeshCore. Gateways are not involved: they
move raw packets (see `proto/gwproto`), and everything here is end to end
between a node and the backend.

The encoding is implemented in `proto/hubproto`.

## Transport

Every message rides in a MeshCore datagram, so encryption, authentication
and routing are MeshCore's own:

| Direction  | MeshCore payload type | Key                                   |
|------------|-----------------------|---------------------------------------|
| node → hub | `ANON_REQ` (join only) | ECDH(node, hub); the packet carries the node's public key |
| node → hub | `REQ`                 | ECDH(node, hub)                       |
| hub → node | `RESPONSE`            | ECDH(hub, node)                       |

The node is built with the hub's public key. The hub learns the node's key
from its join request.

The decrypted datagram is zero padded to the AES block size, so every
message is self-delimiting and trailing zeros are ignored. Plaintext is at
most 167 bytes in a `REQ`/`RESPONSE` and 136 in an `ANON_REQ`.

### Plaintext layout

```
[0:4]  seq, little-endian uint32
[4]    protocol version (1)
[5]    message type
[6:]   message body
```

`seq` sits where MeshCore puts its timestamp, keeping packet hashes unique.

* Node → hub: `seq` is the node's request counter. It must increase with
  every request; the hub drops a request whose `seq` is not greater than the
  last one it accepted from that node (replay protection). The node keeps
  the counter across deep sleep and reboots. A join resets the hub's record.
* Hub → node: `seq` echoes the request being answered. A node accepts only
  responses to its outstanding request.

### Routing

The node keeps a path to the hub, learnt from the hub's replies.

* Without a path, the node floods its request.
* With a path, it sends the request direct along it. If no reply arrives,
  it drops the path and floods.

Every gateway that hears the request reports it. The hub waits briefly
(about half a second) to collect them, then replies through the gateway
that heard the request over the fewest hops, best SNR first. The reply goes
direct, along the request's flood path reversed. For a direct request,
whose path is used up on arrival, the hub uses the route it stored from
that node's last flood request.

Replies to a hello tell the node its current path to the hub, so a node
learns and refreshes its route from the hub instead of from MeshCore path
packets.

## Messages

Integers are little-endian. `str` is a one-byte length and UTF-8 bytes.
`value` is a CBOR item (see below).

### Node → hub

| Type | Name       | Body |
|------|------------|------|
| 0x01 | Join       | `program str, firmware str, claim str` (sent as `ANON_REQ`) |
| 0x02 | Hello      | `config_version u32, wake_reason u8, battery_mv u16, uptime_s u32, firmware str` |
| 0x03 | ConfigGet  | `config_version u32, offset u16` |
| 0x04 | Event      | `event_seq u32, kind str, data value` |

`wake_reason`: 0 unknown, 1 power on, 2 timer, 3 pin, 4 other.

### Hub → node

| Type | Name        | Body |
|------|-------------|------|
| 0x81 | JoinReply   | `status u8` (0 accepted, 1 pending approval, 2 rejected) |
| 0x82 | HelloReply  | `time u32, path_len u8, path [n]u8, config_version u32, config_len u16, config_crc u32, sleep_s u32` |
| 0x83 | ConfigChunk | `config_version u32, offset u16, len u8, data [len]u8` |
| 0x84 | EventAck    | `event_seq u32` |
| 0xFF | Error       | `code u8` (1 not paired, 2 bad request, 3 config changed) |

`HelloReply.time` is the hub's clock in unix seconds, for the node's RTC.
`path_len` is MeshCore's encoded path length byte: hash size in the top two
bits, hop count in the rest.
`sleep_s` is a suggested sleep before the next check-in; 0 means the node
decides.

## Flows

### Pairing

```
node                                hub
 | Join (ANON_REQ, flood)  ------->  |  unknown node: recorded as pending
 | <----------  JoinReply(pending)   |
 ...                                  operator approves in the UI
 | Join  -------------------------->  |
 | <---------  JoinReply(accepted)   |
 | Hello  ------------------------->  |  normal operation
```

The node repeats the join on each wake until it is accepted. A join from a
node that is already paired is answered `accepted`, so a node that lost its
state can recover. `claim` is an optional code printed on the device, shown
to the operator to tell devices apart. (Accepting nodes with a known claim
code without manual approval is planned; for development the backend's
`-auto-pair` flag accepts every join.)

### Check-in and config sync

The node's configuration is a CBOR map that the hub versions. The version
increases with every change.

```
node                                       hub
 | Hello(config_version=3)  ------------->  |  hub has version 5, 300 bytes
 | <-  HelloReply(config_version=5, len=300, crc)
 | ConfigGet(5, offset 0)  -------------->  |
 | <----------  ConfigChunk(5, 0, 128 bytes)
 | ConfigGet(5, 128)  ------------------->  |
 | <----------  ConfigChunk(5, 128, 128 bytes)
 | ConfigGet(5, 256)  ------------------->  |
 | <----------  ConfigChunk(5, 256, 44 bytes)
 ...  node checks the CRC-32 (IEEE) and applies it
 | Hello(config_version=5)  ------------->  |  hub marks the node in sync
```

When `HelloReply.config_version` equals the node's version, the config is
current and no transfer follows. If the config changes during a transfer,
the hub answers `ConfigGet` for an old version with `Error(config changed)`
and the node starts over with a hello.

Chunks are whole messages, so every chunk is a separate request and
response; a lost one is simply asked for again.

### Events

```
node                               hub
 | Event(event_seq=17, "flow", {...})  ->  |  stored
 | <--------------------  EventAck(17)     |
```

The node keeps an event until it is acknowledged and resends it otherwise.
The hub stores events idempotently by `(node, event_seq)`, so resends are
harmless.

## Values

Config and event data are CBOR (RFC 8949), restricted to what a JSON
document can hold, so the web UI can show and edit them as JSON:

* unsigned and negative integers (major types 0 and 1), up to 64 bits
* floats: half, single and double precision are decoded; floats are
  encoded as the shortest of single or double that is exact
* text strings (major type 3)
* arrays (major type 4) and maps with text keys (major type 5), definite
  length only
* `false`, `true`, `null`

Map keys are encoded sorted, so equal configs encode to equal bytes and the
CRC identifies a config.
