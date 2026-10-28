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
[4]    protocol version (2)
[5]    message type
[6:]   message body
```

`seq` sits where MeshCore puts its timestamp, keeping packet hashes unique.

* Node → hub: `seq` is the node's request counter. It must increase with
  every request; the hub drops a request whose `seq` is not greater than the
  last one it accepted from that node (replay protection). The node keeps
  the counter across deep sleep and reboots. A join resets the hub's record.
  A node without stored state starts its counter at a random number, so
  its requests do not repeat packets from an earlier life, which the hub
  would drop as duplicates.
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

### Retries and timing

A request that gets no reply is resent (by default twice), each time with
a new `seq` and after a random backoff that doubles with every retry.
Every request is safe to repeat: the hub stores events and command results
idempotently and answers the rest from its current state. Falling back
from a stale direct route to a flood is not counted as a retry.

The node waits for a reply for as long as the hub usually takes over that
kind of route (direct or flood), learnt like TCP's retransmission timer:
the smoothed reply time plus four times its deviation, by default between
2 and 8 s, doubled on each retry. A new route starts again from the
maximum.

Before its first transmission of a check-in the node waits a random time
(by default up to 2 s), so nodes woken together, by a power cut say, do not
all transmit at once.

## Messages

Integers are little-endian. `str` is a one-byte length and UTF-8 bytes.
`value` is a CBOR item (see below).

### Node → hub

| Type | Name       | Body |
|------|------------|------|
| 0x01 | Join       | `program str, firmware str, claim str` (sent as `ANON_REQ`) |
| 0x02 | Hello      | `config_version u32, wake_reason u8, battery_mv u16, uptime_s u32, firmware str` |
| 0x03 | ConfigGet  | `config_version u32, offset u16` |
| 0x04 | Events     | `count u8`, then per event `event_seq u32, time u32, kind str, data value` |
| 0x05 | ConfigStatus | `config_version u32, status u8, reason str` |
| 0x06 | CommandGet | (empty) |
| 0x07 | CommandResult | `cmd_id u32, status u8, result value` |
| 0x08 | Fragment   | `xfer u16, total u16, offset u16, len u8, data [len]u8` |

`wake_reason`: 0 unknown, 1 power on, 2 timer, 3 pin, 4 other.
`Events.time`: when the event happened, unix seconds by the node's clock; 0
if the node did not know the time.
`ConfigStatus.status`: 0 applied, 1 rejected (`reason` says why).
`CommandResult.status`: 0 ok, 1 failed (`result` is the error message), 2
unknown command.

### Hub → node

| Type | Name        | Body |
|------|-------------|------|
| 0x81 | JoinReply   | `status u8` (0 accepted, 1 pending approval, 2 rejected) |
| 0x82 | HelloReply  | `time u32, path_len u8, path [n]u8, config_version u32, config_len u16, config_crc u32, commands u8` |
| 0x83 | ConfigChunk | `config_version u32, offset u16, len u8, data [len]u8` |
| 0x84 | Ack         | (empty): accepts Events, ConfigStatus, CommandResult or a Fragment |
| 0x85 | Command     | `cmd_id u32, name str, args value`; `cmd_id` 0 means none |
| 0xFF | Error       | `code u8` (1 not paired, 2 bad request, 3 config changed) |

`HelloReply.time` is the hub's clock in unix seconds, for the node's RTC.
`path_len` is MeshCore's encoded path length byte: hash size in the top two
bits, hop count in the rest.
`commands` is how many commands wait for the node.

When the node checks in next, and whether it sleeps in between, is up to
its program, typically from a key in its config; the protocol has no say.

Since the reply echoes the request's `seq`, an `Ack` needs no body: it
accepts exactly the request it answers.

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

### Config validation

The node's program may check a new config before it is applied. Either
way, the node then reports the outcome:

```
 ...  last ConfigChunk received, CRC checked
 | ConfigStatus(5, rejected, "interval_s must be at least 10")  ->  |  shown in the web UI
 | <----------  Ack
```

A rejected config is not applied: the node keeps its old one and keeps
reporting its version in hellos. It remembers the rejected version, so it
does not fetch it again at every check-in. The node keeps the outcome until
the hub acknowledges it, sending it at a later check-in if needed.

### Events

```
node                                                   hub
 | Events([17, t, "flow", {...}], [18, t, "pin", {}])  ->  |  stored
 | <---------------------------------------------  Ack     |
```

The node sends its queued events oldest first, as many per message as fit
a datagram, and keeps them until they are acknowledged. The hub stores
events idempotently by `(node, event_seq)`, so resends are harmless.

Events carry the time they happened by the node's clock, which the hub
sets in every `HelloReply`. An event reported before the first check-in
since boot is stamped when the clock is set, back-dated by the node's
uptime clock. One queued before a reboot and never stamped is sent with
time 0, and the hub uses the time it received it.

### Commands

The operator queues commands in the web UI. The hub counts the waiting
ones in `HelloReply.commands`, and the node fetches and runs them after its
check-in:

```
node                                      hub
 | Hello  -------------------------------->  |
 | <--------------  HelloReply(commands=1)   |
 | CommandGet  --------------------------->  |
 | <----------  Command(42, "read", {...})   |
 ...  runs it
 | CommandResult(42, ok, {...})  ---------->  |  shown in the web UI
 | <----------------------------------  Ack  |
```

The node stores the result before sending it and resends it until it is
acknowledged, so a command runs once even if replies are lost. The hub
records the first result for a command and acknowledges repeats. A
command cancelled in the meantime is answered with `cmd_id` 0.

The node library runs two commands itself:

* `logs` returns the node's log buffer as a string; `{"clear": true}`
  empties it afterwards.
* `reset` drops the node's queued events, its config (fetched again at the
  next check-in) and its route to the hub. Its identity and pairing stay.

The node's program runs the rest; a node without a handler for a command
answers `unknown`.

### Large messages

`Events` (a single event too large for a datagram) and `CommandResult`
(such as the log buffer) can be up to 2048 bytes. Such a message is
encoded as its type byte and body, and sent in `Fragment`s of up to 154
bytes, each a separate request:

```
node                                               hub
 | Fragment(xfer=7, total=400, offset=0, 154 bytes)  ->  |
 | <------------------------------------------  Ack      |
 | Fragment(7, 400, 154, 154 bytes)  ----------------->  |
 | <------------------------------------------  Ack      |
 | Fragment(7, 400, 308, 92 bytes)  ------------------>  |  reassembled, handled
 | <-------------------------  (reply to the message)    |
```

`xfer` is random per message. The hub keeps one partial message per node,
started by a fragment at offset 0, for up to 10 minutes. A fragment resent
after a lost reply replaces what it carried, and a resent last fragment
completes the message again, which is harmless because these messages are
idempotent.

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
