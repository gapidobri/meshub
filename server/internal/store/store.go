// Package store persists what the backend learns from the mesh in SQLite.
package store

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

// migrations change tables that schema.sql created in earlier versions; new
// tables go in schema.sql. The database's user_version counts the applied
// migrations.
var migrations = []string{
	// 1: reception paths, for routing replies to devices
	`ALTER TABLE receptions ADD COLUMN path_len INTEGER NOT NULL DEFAULT 0; -- MeshCore encoded path length
	 ALTER TABLE receptions ADD COLUMN path BLOB NOT NULL DEFAULT x'';     -- repeater hashes, in the order traversed`,
	// 2: protocol version 2: event times and config validation
	`ALTER TABLE events ADD COLUMN device_time INTEGER NOT NULL DEFAULT 0;      -- when it happened by the device's clock, 0 if unknown
	 ALTER TABLE devices ADD COLUMN rejected_version INTEGER NOT NULL DEFAULT 0; -- last config version the device rejected
	 ALTER TABLE devices ADD COLUMN config_error TEXT NOT NULL DEFAULT '';      -- and why`,
	// 3: event IDs, which applications page through and resume from
	`CREATE TABLE events_v3 (
		id          INTEGER PRIMARY KEY AUTOINCREMENT, -- never reused
		pubkey      TEXT NOT NULL REFERENCES devices(pubkey) ON DELETE CASCADE,
		event_seq   INTEGER NOT NULL,
		kind        TEXT NOT NULL,
		data        TEXT NOT NULL, -- JSON
		received_at INTEGER NOT NULL,
		device_time INTEGER NOT NULL DEFAULT 0,
		UNIQUE (pubkey, event_seq)
	 );
	 INSERT INTO events_v3 (pubkey, event_seq, kind, data, received_at, device_time)
		SELECT pubkey, event_seq, kind, data, received_at, device_time FROM events
		ORDER BY received_at, pubkey, event_seq;
	 DROP TABLE events;
	 ALTER TABLE events_v3 RENAME TO events;
	 CREATE INDEX events_received_at ON events(received_at);`,
}

func migrate(db *sql.DB) error {
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	for i := version; i < len(migrations); i++ {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(migrations[i]); err != nil {
			tx.Rollback()
			return fmt.Errorf("migration %d: %w", i+1, err)
		}
		if _, err := tx.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, i+1)); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// Store is the backend's database.
type Store struct {
	db *sql.DB
}

// Open opens (creating if needed) the database at path.
func Open(path string) (*Store, error) {
	dsn := "file:" + path + "?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // SQLite has a single writer; this avoids SQLITE_BUSY
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: schema: %w", err)
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Backup writes a consistent copy of the database to path, which must not
// exist. It is safe while the backend runs.
func (s *Store) Backup(ctx context.Context, path string) error {
	_, err := s.db.ExecContext(ctx, `VACUUM INTO ?`, path)
	return err
}

// Ping checks that the database is usable.
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func millis(t time.Time) int64 { return t.UnixMilli() }

// Gateway is a gateway's last reported status.
type Gateway struct {
	PubKey       string    `json:"pubkey"`
	IATA         string    `json:"iata"`
	Name         string    `json:"name"`
	Status       string    `json:"status"`
	Model        string    `json:"model"`
	Firmware     string    `json:"firmware"`
	Radio        string    `json:"radio"`
	Stats        string    `json:"stats"` // gwproto.Stats as JSON
	FirstSeen    time.Time `json:"first_seen"`
	LastStatusAt time.Time `json:"last_status_at"`
}

// UpsertGateway records a gateway status message.
func (s *Store) UpsertGateway(ctx context.Context, g Gateway) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO gateways (pubkey, iata, name, status, model, firmware, radio, stats, first_seen, last_status_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (pubkey) DO UPDATE SET
			iata = excluded.iata, name = excluded.name, status = excluded.status,
			-- A sparse status (such as a will message) keeps what was last reported.
			model    = CASE WHEN excluded.model = '' THEN gateways.model ELSE excluded.model END,
			firmware = CASE WHEN excluded.firmware = '' THEN gateways.firmware ELSE excluded.firmware END,
			radio    = CASE WHEN excluded.radio = '' THEN gateways.radio ELSE excluded.radio END,
			stats    = CASE WHEN excluded.stats = '{}' THEN gateways.stats ELSE excluded.stats END,
			last_status_at = excluded.last_status_at`,
		g.PubKey, g.IATA, g.Name, g.Status, g.Model, g.Firmware, g.Radio, g.Stats,
		millis(g.LastStatusAt), millis(g.LastStatusAt))
	return err
}

// Gateways returns all known gateways, online first, then by name.
func (s *Store) Gateways(ctx context.Context) ([]Gateway, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT pubkey, iata, name, status, model, firmware, radio, stats, first_seen, last_status_at
		FROM gateways ORDER BY status = 'online' DESC, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Gateway{}
	for rows.Next() {
		var g Gateway
		var first, last int64
		if err := rows.Scan(&g.PubKey, &g.IATA, &g.Name, &g.Status, &g.Model, &g.Firmware, &g.Radio, &g.Stats, &first, &last); err != nil {
			return nil, err
		}
		g.FirstSeen, g.LastStatusAt = time.UnixMilli(first), time.UnixMilli(last)
		out = append(out, g)
	}
	return out, rows.Err()
}

// GatewayStaleAfter is how long after its last status an online gateway is
// no longer trusted to be online. Gateways republish status every 5 minutes.
const GatewayStaleAfter = 15 * time.Minute

// OnlineGateways returns gateways whose latest status is online and was
// reported after since.
func (s *Store) OnlineGateways(ctx context.Context, since time.Time) ([]Gateway, error) {
	all, err := s.Gateways(ctx)
	if err != nil {
		return nil, err
	}
	out := all[:0]
	for _, g := range all {
		if g.Status == "online" && g.LastStatusAt.After(since) {
			out = append(out, g)
		}
	}
	return out, nil
}

// Reception is one gateway hearing (or sending) one packet.
type Reception struct {
	Hash        string
	PayloadType uint8
	Route       string
	Raw         []byte
	Gateway     string
	Direction   string
	SNR, RSSI   int
	Hops        int
	PathLen     uint8 // MeshCore encoded path length
	Path        []byte
	ReceivedAt  time.Time
}

// AddReception records a reception, adding the packet if it is new. It
// reports whether this was the first time the packet was seen.
func (s *Store) AddReception(ctx context.Context, r Reception) (first bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `
		INSERT INTO packets (hash, payload_type, route, raw, first_seen) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (hash) DO NOTHING`,
		r.Hash, r.PayloadType, r.Route, r.Raw, millis(r.ReceivedAt))
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO receptions (hash, gateway, direction, snr, rssi, hops, path_len, path, received_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.Hash, r.Gateway, r.Direction, r.SNR, r.RSSI, r.Hops, r.PathLen, nonNil(r.Path), millis(r.ReceivedAt))
	if err != nil {
		return false, err
	}
	return n == 1, tx.Commit()
}

// BestReception returns the reception of a packet over the fewest hops, best
// SNR first.
func (s *Store) BestReception(ctx context.Context, hash string) (Reception, bool, error) {
	r := Reception{Hash: hash}
	var at int64
	err := s.db.QueryRowContext(ctx, `
		SELECT gateway, snr, rssi, hops, path_len, path, received_at FROM receptions
		WHERE hash = ? AND direction = 'rx' ORDER BY hops, snr DESC LIMIT 1`, hash).
		Scan(&r.Gateway, &r.SNR, &r.RSSI, &r.Hops, &r.PathLen, &r.Path, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, nil
	}
	r.ReceivedAt = time.UnixMilli(at)
	return r, err == nil, err
}

func nonNil(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

// Advert is a verified node advertisement as heard by one gateway.
type Advert struct {
	PubKey     string
	Name       string
	Type       uint8
	HasLoc     bool
	Lat, Lon   float64
	AdvertTime uint32 // the node's timestamp
	Gateway    string
	SNR, RSSI  int
	Hops       int
	HeardAt    time.Time
}

// AddAdvert updates a node from its advert, and the link from the gateway
// that heard it. Name, type and location only change when the advert is at
// least as new as the last one, so replayed old adverts cannot roll them back.
func (s *Store) AddAdvert(ctx context.Context, a Advert) error {
	var lat, lon any
	if a.HasLoc {
		lat, lon = a.Lat, a.Lon
	}
	heard := millis(a.HeardAt)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO nodes (pubkey, name, type, lat, lon, advert_time, first_seen, last_heard_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (pubkey) DO UPDATE SET
			name        = CASE WHEN excluded.advert_time >= nodes.advert_time THEN excluded.name ELSE nodes.name END,
			type        = CASE WHEN excluded.advert_time >= nodes.advert_time THEN excluded.type ELSE nodes.type END,
			lat         = CASE WHEN excluded.advert_time >= nodes.advert_time THEN excluded.lat ELSE nodes.lat END,
			lon         = CASE WHEN excluded.advert_time >= nodes.advert_time THEN excluded.lon ELSE nodes.lon END,
			advert_time = max(excluded.advert_time, nodes.advert_time),
			last_heard_at = max(excluded.last_heard_at, nodes.last_heard_at)`,
		a.PubKey, a.Name, a.Type, lat, lon, a.AdvertTime, heard, heard)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO node_links (node, gateway, snr, rssi, hops, heard_count, last_heard_at)
		VALUES (?, ?, ?, ?, ?, 1, ?)
		ON CONFLICT (node, gateway) DO UPDATE SET
			snr = excluded.snr, rssi = excluded.rssi, hops = excluded.hops,
			heard_count = node_links.heard_count + 1,
			last_heard_at = max(excluded.last_heard_at, node_links.last_heard_at)`,
		a.PubKey, a.Gateway, a.SNR, a.RSSI, a.Hops, heard)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// Node is a node known from its adverts.
type Node struct {
	PubKey      string    `json:"pubkey"`
	Name        string    `json:"name"`
	Type        uint8     `json:"type"`
	HasLoc      bool      `json:"has_location"`
	Lat         float64   `json:"lat,omitempty"`
	Lon         float64   `json:"lon,omitempty"`
	AdvertTime  time.Time `json:"advert_time"`
	FirstSeen   time.Time `json:"first_seen"`
	LastHeardAt time.Time `json:"last_heard_at"`
	Links       []Link    `json:"links"` // best first
}

// Link is how one gateway hears a node.
type Link struct {
	Gateway     string    `json:"gateway"`
	SNR         int       `json:"snr"`
	RSSI        int       `json:"rssi"`
	Hops        int       `json:"hops"`
	HeardCount  int       `json:"heard_count"`
	LastHeardAt time.Time `json:"last_heard_at"`
}

// Nodes returns all known nodes, most recently heard first, each with its
// links ordered by fewest hops, then best SNR.
func (s *Store) Nodes(ctx context.Context) ([]Node, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT pubkey, name, type, lat, lon, advert_time, first_seen, last_heard_at
		FROM nodes ORDER BY last_heard_at DESC`)
	if err != nil {
		return nil, err
	}
	out := []Node{}
	index := map[string]int{}
	for rows.Next() {
		var n Node
		var lat, lon sql.NullFloat64
		var advert, first, last int64
		if err := rows.Scan(&n.PubKey, &n.Name, &n.Type, &lat, &lon, &advert, &first, &last); err != nil {
			rows.Close()
			return nil, err
		}
		n.HasLoc, n.Lat, n.Lon = lat.Valid, lat.Float64, lon.Float64
		n.AdvertTime = time.Unix(advert, 0)
		n.FirstSeen, n.LastHeardAt = time.UnixMilli(first), time.UnixMilli(last)
		index[n.PubKey] = len(out)
		out = append(out, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	links, err := s.db.QueryContext(ctx, `
		SELECT node, gateway, snr, rssi, hops, heard_count, last_heard_at
		FROM node_links ORDER BY hops, snr DESC`)
	if err != nil {
		return nil, err
	}
	defer links.Close()
	for links.Next() {
		var node string
		var l Link
		var last int64
		if err := links.Scan(&node, &l.Gateway, &l.SNR, &l.RSSI, &l.Hops, &l.HeardCount, &last); err != nil {
			return nil, err
		}
		l.LastHeardAt = time.UnixMilli(last)
		if i, ok := index[node]; ok {
			out[i].Links = append(out[i].Links, l)
		}
	}
	return out, links.Err()
}

// PruneEvents deletes events received before before.
func (s *Store) PruneEvents(ctx context.Context, before time.Time) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM events WHERE received_at < ?`, millis(before))
	return err
}

// Prune deletes packets and receptions older than before.
func (s *Store) Prune(ctx context.Context, before time.Time) error {
	ms := millis(before)
	if _, err := s.db.ExecContext(ctx, `DELETE FROM receptions WHERE received_at < ?`, ms); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM packets WHERE first_seen < ?
		AND NOT EXISTS (SELECT 1 FROM receptions WHERE receptions.hash = packets.hash)`, ms)
	return err
}

// PacketCount returns the number of distinct packets and receptions stored.
func (s *Store) PacketCount(ctx context.Context) (packets, receptions int, err error) {
	err = s.db.QueryRowContext(ctx, `
		SELECT (SELECT count(*) FROM packets), (SELECT count(*) FROM receptions)`).Scan(&packets, &receptions)
	return packets, receptions, err
}

// Setting returns the value stored under key, or nil if there is none.
func (s *Store) Setting(ctx context.Context, key string) ([]byte, error) {
	var v []byte
	err := s.db.QueryRowContext(ctx, `SELECT value FROM settings WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return v, err
}

// SetSetting stores value under key, replacing any previous value.
func (s *Store) SetSetting(ctx context.Context, key string, value []byte) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO settings (key, value) VALUES (?, ?)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}
