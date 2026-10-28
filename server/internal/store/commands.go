package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Command statuses.
const (
	CommandPending   = "pending"
	CommandOK        = "ok"
	CommandFailed    = "failed"
	CommandUnknown   = "unknown" // the device does not know the command
	CommandCancelled = "cancelled"
)

// Command is a command queued for a device.
type Command struct {
	ID        uint32    `json:"id"`
	Name      string    `json:"name"`
	Args      string    `json:"args"` // JSON
	ArgsCBOR  []byte    `json:"-"`
	Status    string    `json:"status"`
	Result    string    `json:"result"` // JSON, once done
	CreatedAt time.Time `json:"created_at"`
	DoneAt    time.Time `json:"done_at"`
}

const commandColumns = `id, name, args, args_cbor, status, result, created_at, done_at`

func scanCommand(sc interface{ Scan(...any) error }) (Command, error) {
	var c Command
	var created, done int64
	err := sc.Scan(&c.ID, &c.Name, &c.Args, &c.ArgsCBOR, &c.Status, &c.Result, &created, &done)
	c.CreatedAt = time.UnixMilli(created)
	if done > 0 {
		c.DoneAt = time.UnixMilli(done)
	}
	return c, err
}

// AddCommand queues a command for a device and returns its ID.
func (s *Store) AddCommand(ctx context.Context, pubKey, name, args string, argsCBOR []byte, at time.Time) (uint32, error) {
	var id uint32
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO device_commands (pubkey, name, args, args_cbor, status, created_at) VALUES (?, ?, ?, ?, ?, ?)
		RETURNING id`, pubKey, name, args, nonNil(argsCBOR), CommandPending, millis(at)).Scan(&id)
	return id, err
}

// NextCommand returns a device's oldest pending command.
func (s *Store) NextCommand(ctx context.Context, pubKey string) (Command, bool, error) {
	c, err := scanCommand(s.db.QueryRowContext(ctx, `SELECT `+commandColumns+` FROM device_commands
		WHERE pubkey = ? AND status = ? ORDER BY id LIMIT 1`, pubKey, CommandPending))
	if errors.Is(err, sql.ErrNoRows) {
		return c, false, nil
	}
	return c, err == nil, err
}

// Command returns one of a device's commands.
func (s *Store) Command(ctx context.Context, pubKey string, id uint32) (Command, bool, error) {
	c, err := scanCommand(s.db.QueryRowContext(ctx, `SELECT `+commandColumns+` FROM device_commands
		WHERE pubkey = ? AND id = ?`, pubKey, id))
	if errors.Is(err, sql.ErrNoRows) {
		return c, false, nil
	}
	return c, err == nil, err
}

// PendingCommands returns how many commands wait for a device.
func (s *Store) PendingCommands(ctx context.Context, pubKey string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM device_commands WHERE pubkey = ? AND status = ?`,
		pubKey, CommandPending).Scan(&n)
	return n, err
}

// FinishCommand records a pending command's result. A resent result for a
// command already finished is ignored.
func (s *Store) FinishCommand(ctx context.Context, pubKey string, id uint32, status, result string, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE device_commands SET status = ?, result = ?, done_at = ?
		WHERE pubkey = ? AND id = ? AND status = ?`, status, result, millis(at), pubKey, id, CommandPending)
	return err
}

// CancelCommand cancels a pending command, reporting whether it was still
// pending.
func (s *Store) CancelCommand(ctx context.Context, pubKey string, id uint32, at time.Time) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE device_commands SET status = ?, done_at = ?
		WHERE pubkey = ? AND id = ? AND status = ?`, CommandCancelled, millis(at), pubKey, id, CommandPending)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// Commands returns a device's latest commands, newest first.
func (s *Store) Commands(ctx context.Context, pubKey string, limit int) ([]Command, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+commandColumns+` FROM device_commands
		WHERE pubkey = ? ORDER BY id DESC LIMIT ?`, pubKey, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Command{}
	for rows.Next() {
		c, err := scanCommand(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
