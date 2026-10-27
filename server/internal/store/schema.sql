CREATE TABLE IF NOT EXISTS gateways (
	pubkey         TEXT PRIMARY KEY,
	iata           TEXT NOT NULL,
	name           TEXT NOT NULL,
	status         TEXT NOT NULL,
	model          TEXT NOT NULL DEFAULT '',
	firmware       TEXT NOT NULL DEFAULT '',
	radio          TEXT NOT NULL DEFAULT '',
	stats          TEXT NOT NULL DEFAULT '{}', -- gwproto.Stats as JSON
	first_seen     INTEGER NOT NULL,           -- unix milliseconds
	last_status_at INTEGER NOT NULL
);

-- One row per distinct packet, keyed by its MeshCore hash, which is the same
-- however many repeaters it crossed and gateways heard it.
CREATE TABLE IF NOT EXISTS packets (
	hash         TEXT PRIMARY KEY,
	payload_type INTEGER NOT NULL,
	route        TEXT NOT NULL, -- "F" flood or "D" direct
	raw          BLOB NOT NULL, -- as first heard
	first_seen   INTEGER NOT NULL
);

-- Every time a gateway heard (or sent) a packet.
CREATE TABLE IF NOT EXISTS receptions (
	id          INTEGER PRIMARY KEY,
	hash        TEXT NOT NULL REFERENCES packets(hash) ON DELETE CASCADE,
	gateway     TEXT NOT NULL,
	direction   TEXT NOT NULL, -- "rx" or "tx"
	snr         INTEGER NOT NULL,
	rssi        INTEGER NOT NULL,
	hops        INTEGER NOT NULL,
	received_at INTEGER NOT NULL
	-- path_len and path are added by migration 1
);
CREATE INDEX IF NOT EXISTS receptions_hash ON receptions(hash);
CREATE INDEX IF NOT EXISTS receptions_received_at ON receptions(received_at);

-- Nodes known from their signed adverts.
CREATE TABLE IF NOT EXISTS nodes (
	pubkey        TEXT PRIMARY KEY,
	name          TEXT NOT NULL,
	type          INTEGER NOT NULL,
	lat           REAL, -- NULL without a location
	lon           REAL,
	advert_time   INTEGER NOT NULL, -- the node's own advert timestamp, unix seconds
	first_seen    INTEGER NOT NULL,
	last_heard_at INTEGER NOT NULL
);

-- How each gateway hears each node, from the node's adverts. The backend
-- picks the gateway to reach a node by from these.
CREATE TABLE IF NOT EXISTS node_links (
	node          TEXT NOT NULL REFERENCES nodes(pubkey) ON DELETE CASCADE,
	gateway       TEXT NOT NULL,
	snr           INTEGER NOT NULL, -- of the latest reception
	rssi          INTEGER NOT NULL,
	hops          INTEGER NOT NULL,
	heard_count   INTEGER NOT NULL,
	last_heard_at INTEGER NOT NULL,
	PRIMARY KEY (node, gateway)
);

-- Backend settings, such as its MeshCore identity.
CREATE TABLE IF NOT EXISTS settings (
	key   TEXT PRIMARY KEY,
	value BLOB NOT NULL
);

-- Nodes paired with the hub (see docs/protocol.md). The nodes table above is
-- everything heard on the mesh; devices are the ones the hub manages.
CREATE TABLE IF NOT EXISTS devices (
	pubkey         TEXT PRIMARY KEY,
	status         TEXT NOT NULL,              -- pending, paired or rejected
	program        TEXT NOT NULL,
	firmware       TEXT NOT NULL,
	claim          TEXT NOT NULL,
	last_seq       INTEGER NOT NULL DEFAULT -1, -- highest request number accepted
	config_version INTEGER NOT NULL DEFAULT 0,  -- as reported by the node
	battery_mv     INTEGER NOT NULL DEFAULT 0,
	wake_reason    INTEGER NOT NULL DEFAULT 0,
	uptime_s       INTEGER NOT NULL DEFAULT 0,
	last_hello_at  INTEGER NOT NULL DEFAULT 0,
	joined_at      INTEGER NOT NULL
);

-- The config the hub wants each device to have.
CREATE TABLE IF NOT EXISTS device_configs (
	pubkey     TEXT PRIMARY KEY REFERENCES devices(pubkey) ON DELETE CASCADE,
	version    INTEGER NOT NULL,
	cbor       BLOB NOT NULL,
	json       TEXT NOT NULL,
	updated_at INTEGER NOT NULL
);

-- How the hub reaches each device: the gateway and path of its best recent
-- flood request.
CREATE TABLE IF NOT EXISTS device_routes (
	pubkey     TEXT PRIMARY KEY REFERENCES devices(pubkey) ON DELETE CASCADE,
	gateway    TEXT NOT NULL,
	path_len   INTEGER NOT NULL,
	path       BLOB NOT NULL, -- device to hub
	updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
	pubkey      TEXT NOT NULL REFERENCES devices(pubkey) ON DELETE CASCADE,
	event_seq   INTEGER NOT NULL,
	kind        TEXT NOT NULL,
	data        TEXT NOT NULL, -- JSON
	received_at INTEGER NOT NULL,
	PRIMARY KEY (pubkey, event_seq)
);
CREATE INDEX IF NOT EXISTS events_received_at ON events(received_at);
