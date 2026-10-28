// Package client is a Go client for the meshub backend, for applications
// built on it: its HTTP API (/api/v1), and with Listen the device events,
// status and command updates it publishes over MQTT. The values are those
// of package appproto.
//
//	c := client.New("http://meshub:8080", client.WithToken(token))
//	devices, err := c.Devices(ctx, client.DeviceFilter{Program: "hydrant"})
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gapidobri/meshub/proto/appproto"
)

// Client calls a meshub backend. It is safe for concurrent use.
type Client struct {
	base  string
	token string
	http  *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithToken authorizes requests with one of the backend's API tokens.
func WithToken(token string) Option { return func(c *Client) { c.token = token } }

// WithHTTPClient makes requests with hc instead of a client with a 30
// second timeout.
func WithHTTPClient(hc *http.Client) Option { return func(c *Client) { c.http = hc } }

// New returns a client for the backend at baseURL, such as
// "http://meshub:8080".
func New(baseURL string, opts ...Option) *Client {
	c := &Client{base: strings.TrimRight(baseURL, "/") + "/api/v1", http: &http.Client{Timeout: 30 * time.Second}}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Errors that an *Error matches with errors.Is, by its HTTP status.
var (
	ErrUnauthorized = errors.New("meshub: unauthorized")        // 401: a missing or wrong token
	ErrNotFound     = errors.New("meshub: not found")           // 404: no such device or command
	ErrConflict     = errors.New("meshub: conflict")            // 409: e.g. cancelling a finished command
	ErrInvalid      = errors.New("meshub: invalid request")     // 400, 422: e.g. a malformed config
	ErrUnavailable  = errors.New("meshub: service unavailable") // 503: e.g. no gateway online
)

// Error is an error response from the backend (RFC 9457).
type Error struct {
	Status int    `json:"status"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
}

func (e *Error) Error() string {
	msg := e.Detail
	if msg == "" {
		msg = e.Title
	}
	return fmt.Sprintf("meshub: %d %s", e.Status, msg)
}

// Is matches the Err values by status.
func (e *Error) Is(target error) bool {
	switch target {
	case ErrUnauthorized:
		return e.Status == http.StatusUnauthorized
	case ErrNotFound:
		return e.Status == http.StatusNotFound
	case ErrConflict:
		return e.Status == http.StatusConflict
	case ErrInvalid:
		return e.Status == http.StatusBadRequest || e.Status == http.StatusUnprocessableEntity
	case ErrUnavailable:
		return e.Status == http.StatusServiceUnavailable
	}
	return false
}

// do sends a request with in as its JSON body, if not nil, and decodes the
// reply into out, if not nil.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, in, out any) error {
	u := c.base + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		e := &Error{Status: resp.StatusCode}
		json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(e)
		e.Status = resp.StatusCode
		if e.Title == "" {
			e.Title = http.StatusText(resp.StatusCode)
		}
		return e
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func devicePath(pubKey string) string { return "/devices/" + url.PathEscape(pubKey) }

// Hub returns the hub's identity and recent transmissions.
func (c *Client) Hub(ctx context.Context) (appproto.Hub, error) {
	var h appproto.Hub
	return h, c.do(ctx, http.MethodGet, "/hub", nil, nil, &h)
}

// SendAdvert floods the hub's advert, returning how many gateways it was
// queued on.
func (c *Client) SendAdvert(ctx context.Context) (int, error) {
	var out struct{ Gateways int }
	return out.Gateways, c.do(ctx, http.MethodPost, "/hub/advert", nil, nil, &out)
}

// Gateways lists the gateways.
func (c *Client) Gateways(ctx context.Context) ([]appproto.Gateway, error) {
	var out struct{ Gateways []appproto.Gateway }
	return out.Gateways, c.do(ctx, http.MethodGet, "/gateways", nil, nil, &out)
}

// Nodes lists the mesh nodes known from their adverts.
func (c *Client) Nodes(ctx context.Context) ([]appproto.Node, error) {
	var out struct{ Nodes []appproto.Node }
	return out.Nodes, c.do(ctx, http.MethodGet, "/nodes", nil, nil, &out)
}

// DeviceFilter selects devices; empty fields select all.
type DeviceFilter struct {
	Program string
	Status  string // appproto.DevicePending, DevicePaired or DeviceRejected
}

// Devices lists the devices f selects.
func (c *Client) Devices(ctx context.Context, f DeviceFilter) ([]appproto.Device, error) {
	q := url.Values{}
	if f.Program != "" {
		q.Set("program", f.Program)
	}
	if f.Status != "" {
		q.Set("status", f.Status)
	}
	var out struct{ Devices []appproto.Device }
	return out.Devices, c.do(ctx, http.MethodGet, "/devices", q, nil, &out)
}

// Device returns a device.
func (c *Client) Device(ctx context.Context, pubKey string) (appproto.Device, error) {
	var d appproto.Device
	return d, c.do(ctx, http.MethodGet, devicePath(pubKey), nil, nil, &d)
}

// ApproveDevice approves a device's pairing. It is paired the next time it
// asks, which a pending device does every minute.
func (c *Client) ApproveDevice(ctx context.Context, pubKey string) (appproto.Device, error) {
	var d appproto.Device
	return d, c.do(ctx, http.MethodPost, devicePath(pubKey)+"/approve", nil, nil, &d)
}

// RejectDevice rejects a device's pairing.
func (c *Client) RejectDevice(ctx context.Context, pubKey string) (appproto.Device, error) {
	var d appproto.Device
	return d, c.do(ctx, http.MethodPost, devicePath(pubKey)+"/reject", nil, nil, &d)
}

// DeleteDevice forgets a device with its config, events and commands.
func (c *Client) DeleteDevice(ctx context.Context, pubKey string) error {
	return c.do(ctx, http.MethodDelete, devicePath(pubKey), nil, nil, nil)
}

// Config returns the config the hub has for a device.
func (c *Client) Config(ctx context.Context, pubKey string) (appproto.Config, error) {
	var cfg appproto.Config
	return cfg, c.do(ctx, http.MethodGet, devicePath(pubKey)+"/config", nil, nil, &cfg)
}

// SetConfig replaces a device's config with config, which must encode as a
// JSON object. The device fetches it at its next check-in, and may reject
// it: see appproto.Device's RejectedVersion and ConfigError.
func (c *Client) SetConfig(ctx context.Context, pubKey string, config any) (appproto.Config, error) {
	var cfg appproto.Config
	return cfg, c.do(ctx, http.MethodPut, devicePath(pubKey)+"/config", nil, config, &cfg)
}

// EventQuery selects events by ID, the order the backend stored them in.
type EventQuery struct {
	Device  string // only this device's, if set
	Program string // only devices running this, if set and Device is not
	After   int64  // only IDs above this
	Before  int64  // only IDs below this, if set
	Desc    bool   // newest first
	Limit   int    // at most this many, 1 to 1000; 0 for the backend's default of 100
}

// Events returns the events q selects.
func (c *Client) Events(ctx context.Context, q EventQuery) ([]appproto.Event, error) {
	v := url.Values{}
	path := "/events"
	if q.Device != "" {
		path = devicePath(q.Device) + "/events"
	} else if q.Program != "" {
		v.Set("program", q.Program)
	}
	if q.After > 0 {
		v.Set("after", strconv.FormatInt(q.After, 10))
	}
	if q.Before > 0 {
		v.Set("before", strconv.FormatInt(q.Before, 10))
	}
	if q.Desc {
		v.Set("order", "desc")
	}
	if q.Limit > 0 {
		v.Set("limit", strconv.Itoa(q.Limit))
	}
	var out struct{ Events []appproto.Event }
	return out.Events, c.do(ctx, http.MethodGet, path, v, nil, &out)
}

// pageSize is how many events EventsAfter fetches at a time.
const pageSize = 1000

// EventsAfter yields every event of program (or of all programs, if empty)
// with an ID above after, oldest first, fetching them in pages. It stops at
// the first error, which it yields.
func (c *Client) EventsAfter(ctx context.Context, program string, after int64) iter.Seq2[appproto.Event, error] {
	return func(yield func(appproto.Event, error) bool) {
		for {
			events, err := c.Events(ctx, EventQuery{Program: program, After: after, Limit: pageSize})
			if err != nil {
				yield(appproto.Event{}, err)
				return
			}
			for _, e := range events {
				if !yield(e, nil) {
					return
				}
				after = e.ID
			}
			if len(events) < pageSize {
				return
			}
		}
	}
}

// Commands lists a device's latest commands, newest first; limit 0 for the
// backend's default of 100.
func (c *Client) Commands(ctx context.Context, pubKey string, limit int) ([]appproto.Command, error) {
	q := url.Values{}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var out struct{ Commands []appproto.Command }
	return out.Commands, c.do(ctx, http.MethodGet, devicePath(pubKey)+"/commands", q, nil, &out)
}

// Command returns one of a device's commands.
func (c *Client) Command(ctx context.Context, pubKey string, id uint32) (appproto.Command, error) {
	var cmd appproto.Command
	return cmd, c.do(ctx, http.MethodGet, fmt.Sprintf("%s/commands/%d", devicePath(pubKey), id), nil, nil, &cmd)
}

// QueueCommand queues a command, with args encoded as JSON (nil for none).
// The device fetches it at its next check-in; Listen's OnCommand hears how
// it went.
func (c *Client) QueueCommand(ctx context.Context, pubKey, name string, args any) (appproto.Command, error) {
	body := struct {
		Name string `json:"name"`
		Args any    `json:"args,omitempty"`
	}{name, args}
	var cmd appproto.Command
	return cmd, c.do(ctx, http.MethodPost, devicePath(pubKey)+"/commands", nil, body, &cmd)
}

// CancelCommand cancels a command the device has not fetched yet. It fails
// with ErrConflict once the command is no longer pending.
func (c *Client) CancelCommand(ctx context.Context, pubKey string, id uint32) (appproto.Command, error) {
	var cmd appproto.Command
	return cmd, c.do(ctx, http.MethodPost, fmt.Sprintf("%s/commands/%d/cancel", devicePath(pubKey), id), nil, nil, &cmd)
}
