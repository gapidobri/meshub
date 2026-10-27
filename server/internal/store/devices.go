package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Device statuses.
const (
	DevicePending  = "pending"
	DevicePaired   = "paired"
	DeviceRejected = "rejected"
)

// Device is a node paired (or asking to pair) with the hub.
type Device struct {
	PubKey        string    `json:"pubkey"`
	Status        string    `json:"status"`
	Program       string    `json:"program"`
	Firmware      string    `json:"firmware"`
	Claim         string    `json:"claim"`
	LastSeq       int64     `json:"-"`
	ConfigVersion uint32    `json:"config_version"` // as reported by the device
	WantVersion   uint32    `json:"want_version"`   // the hub's config version for it
	BatteryMV     int       `json:"battery_mv"`
	WakeReason    int       `json:"wake_reason"`
	UptimeS       uint32    `json:"uptime_s"`
	LastHelloAt   time.Time `json:"last_hello_at"`
	JoinedAt      time.Time `json:"joined_at"`
}

const deviceColumns = `d.pubkey, d.status, d.program, d.firmware, d.claim, d.last_seq, d.config_version,
	coalesce(c.version, 0), d.battery_mv, d.wake_reason, d.uptime_s, d.last_hello_at, d.joined_at`

func scanDevice(sc interface{ Scan(...any) error }) (Device, error) {
	var d Device
	var hello, joined int64
	err := sc.Scan(&d.PubKey, &d.Status, &d.Program, &d.Firmware, &d.Claim, &d.LastSeq, &d.ConfigVersion,
		&d.WantVersion, &d.BatteryMV, &d.WakeReason, &d.UptimeS, &hello, &joined)
	if hello > 0 {
		d.LastHelloAt = time.UnixMilli(hello)
	}
	d.JoinedAt = time.UnixMilli(joined)
	return d, err
}

// Device returns the device with pubKey.
func (s *Store) Device(ctx context.Context, pubKey string) (Device, bool, error) {
	d, err := scanDevice(s.db.QueryRowContext(ctx, `SELECT `+deviceColumns+`
		FROM devices d LEFT JOIN device_configs c ON c.pubkey = d.pubkey WHERE d.pubkey = ?`, pubKey))
	if errors.Is(err, sql.ErrNoRows) {
		return d, false, nil
	}
	return d, err == nil, err
}

// Devices returns all devices, pending first, then by when they joined.
func (s *Store) Devices(ctx context.Context) ([]Device, error) {
	return s.queryDevices(ctx, `SELECT `+deviceColumns+`
		FROM devices d LEFT JOIN device_configs c ON c.pubkey = d.pubkey
		ORDER BY d.status = 'pending' DESC, d.joined_at`)
}

// DevicesByHash returns the devices whose public key starts with the given
// hex prefix, the MeshCore hash a packet names its sender by.
func (s *Store) DevicesByHash(ctx context.Context, hexPrefix string) ([]Device, error) {
	return s.queryDevices(ctx, `SELECT `+deviceColumns+`
		FROM devices d LEFT JOIN device_configs c ON c.pubkey = d.pubkey
		WHERE substr(d.pubkey, 1, ?) = ?`, len(hexPrefix), hexPrefix)
}

func (s *Store) queryDevices(ctx context.Context, query string, args ...any) ([]Device, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Device{}
	for rows.Next() {
		d, err := scanDevice(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// Join records a join request. A new device gets status; a known one keeps
// its status. The request number starts over at seq either way. It returns
// the device as stored.
func (s *Store) Join(ctx context.Context, pubKey, program, firmware, claim, status string, seq uint32, at time.Time) (Device, error) {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO devices (pubkey, status, program, firmware, claim, last_seq, joined_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (pubkey) DO UPDATE SET
			program = excluded.program, firmware = excluded.firmware, claim = excluded.claim,
			last_seq = excluded.last_seq`,
		pubKey, status, program, firmware, claim, seq, millis(at))
	if err != nil {
		return Device{}, err
	}
	d, _, err := s.Device(ctx, pubKey)
	return d, err
}

// SetDeviceStatus changes a device's status.
func (s *Store) SetDeviceStatus(ctx context.Context, pubKey, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE devices SET status = ? WHERE pubkey = ?`, status, pubKey)
	return err
}

// DeleteDevice forgets a device, its config, route and events.
func (s *Store) DeleteDevice(ctx context.Context, pubKey string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM devices WHERE pubkey = ?`, pubKey)
	return err
}

// AcceptSeq records seq as the device's latest request number if it is
// newer than the last one, reporting whether it was. This is the hub's
// replay protection.
func (s *Store) AcceptSeq(ctx context.Context, pubKey string, seq uint32) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE devices SET last_seq = ? WHERE pubkey = ? AND last_seq < ?`, seq, pubKey, seq)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// Hello is what a device reports when it checks in.
type Hello struct {
	ConfigVersion uint32
	WakeReason    uint8
	BatteryMV     uint16
	UptimeS       uint32
	Firmware      string
}

// RecordHello stores a device's check-in.
func (s *Store) RecordHello(ctx context.Context, pubKey string, h Hello, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE devices SET config_version = ?, wake_reason = ?, battery_mv = ?, uptime_s = ?,
			firmware = CASE WHEN ? = '' THEN firmware ELSE ? END, last_hello_at = ?
		WHERE pubkey = ?`,
		h.ConfigVersion, h.WakeReason, h.BatteryMV, h.UptimeS, h.Firmware, h.Firmware, millis(at), pubKey)
	return err
}

// DeviceConfig is the config the hub wants a device to have.
type DeviceConfig struct {
	Version uint32
	CBOR    []byte
	JSON    string
}

// Config returns a device's config, or a zero DeviceConfig (version 0,
// empty) if it has none.
func (s *Store) Config(ctx context.Context, pubKey string) (DeviceConfig, error) {
	var c DeviceConfig
	err := s.db.QueryRowContext(ctx, `SELECT version, cbor, json FROM device_configs WHERE pubkey = ?`, pubKey).
		Scan(&c.Version, &c.CBOR, &c.JSON)
	if errors.Is(err, sql.ErrNoRows) {
		return DeviceConfig{}, nil
	}
	return c, err
}

// SetConfig stores a new config for a device under the next version, which
// it returns.
func (s *Store) SetConfig(ctx context.Context, pubKey string, cbor []byte, json string, at time.Time) (uint32, error) {
	var version uint32
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO device_configs (pubkey, version, cbor, json, updated_at) VALUES (?, 1, ?, ?, ?)
		ON CONFLICT (pubkey) DO UPDATE SET
			version = device_configs.version + 1, cbor = excluded.cbor, json = excluded.json,
			updated_at = excluded.updated_at
		RETURNING version`, pubKey, cbor, json, millis(at)).Scan(&version)
	return version, err
}

// Route is how the hub reaches a device.
type Route struct {
	Gateway string
	PathLen uint8
	Path    []byte // device to hub
}

// SetRoute stores the route to a device.
func (s *Store) SetRoute(ctx context.Context, pubKey string, r Route, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO device_routes (pubkey, gateway, path_len, path, updated_at) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (pubkey) DO UPDATE SET
			gateway = excluded.gateway, path_len = excluded.path_len, path = excluded.path,
			updated_at = excluded.updated_at`, pubKey, r.Gateway, r.PathLen, nonNil(r.Path), millis(at))
	return err
}

// Route returns the stored route to a device.
func (s *Store) Route(ctx context.Context, pubKey string) (Route, bool, error) {
	var r Route
	err := s.db.QueryRowContext(ctx, `SELECT gateway, path_len, path FROM device_routes WHERE pubkey = ?`, pubKey).
		Scan(&r.Gateway, &r.PathLen, &r.Path)
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, nil
	}
	return r, err == nil, err
}

// Event is something a device reported.
type Event struct {
	Seq        uint32    `json:"seq"`
	Kind       string    `json:"kind"`
	Data       string    `json:"data"` // JSON
	ReceivedAt time.Time `json:"received_at"`
}

// AddEvent stores an event, ignoring a resend of one already stored.
func (s *Store) AddEvent(ctx context.Context, pubKey string, e Event) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO events (pubkey, event_seq, kind, data, received_at) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (pubkey, event_seq) DO NOTHING`, pubKey, e.Seq, e.Kind, e.Data, millis(e.ReceivedAt))
	return err
}

// Events returns a device's latest events, newest first.
func (s *Store) Events(ctx context.Context, pubKey string, limit int) ([]Event, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT event_seq, kind, data, received_at FROM events WHERE pubkey = ?
		ORDER BY received_at DESC, event_seq DESC LIMIT ?`, pubKey, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Event{}
	for rows.Next() {
		var e Event
		var at int64
		if err := rows.Scan(&e.Seq, &e.Kind, &e.Data, &at); err != nil {
			return nil, err
		}
		e.ReceivedAt = time.UnixMilli(at)
		out = append(out, e)
	}
	return out, rows.Err()
}
