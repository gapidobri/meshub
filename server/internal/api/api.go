// Package api serves the backend's HTTP API for applications under /api/v1,
// with its OpenAPI description at /api/v1/openapi.json (and .yaml) and
// browsable docs at /api/v1/docs. The values it exchanges are those of
// package appproto.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"

	"github.com/gapidobri/meshub/proto/appproto"
	"github.com/gapidobri/meshub/server/internal/appconv"
	"github.com/gapidobri/meshub/server/internal/hub"
	"github.com/gapidobri/meshub/server/internal/store"
)

// Prefix is where the API is served.
const Prefix = "/api/v1"

// Hub is the backend's mesh presence, which applies changes to devices.
type Hub interface {
	Status() hub.Status
	SendAdvert(ctx context.Context) (int, error)
	SetConfig(ctx context.Context, device string, jsonConfig []byte) (uint32, error)
	QueueCommand(ctx context.Context, device, name string, jsonArgs []byte) (uint32, error)
	CancelCommand(ctx context.Context, device string, id uint32) error
	SetDeviceStatus(ctx context.Context, device, status string) error
	DeleteDevice(ctx context.Context, device string) error
}

// Options configures the API.
type Options struct {
	Version string // of the backend, shown in the OpenAPI description
	// Tokens are the bearer tokens that authorize requests. With none, the
	// API is open to anyone who can reach it.
	Tokens []string
}

// Register adds the API to mux and returns its description.
func Register(mux *http.ServeMux, st *store.Store, h Hub, log *slog.Logger, opt Options) huma.API {
	cfg := huma.DefaultConfig("meshub API", opt.Version)
	cfg.Info.Description = "Devices, their events, config and commands, and the mesh they are reached through. " +
		"Device events and status are also published over MQTT; see package appproto."
	cfg.OpenAPIPath = Prefix + "/openapi"
	cfg.DocsPath = Prefix + "/docs"
	cfg.SchemasPath = Prefix + "/schemas"
	// Clients must ignore fields they do not know, so fields can be added
	// within v1; and bodies carry no $schema links, so they are the same
	// values as the MQTT messages.
	cfg.AllowAdditionalPropertiesByDefault = true
	cfg.CreateHooks = nil
	cfg.Components.SecuritySchemes = map[string]*huma.SecurityScheme{
		"bearer": {Type: "http", Scheme: "bearer", Description: "One of the backend's API tokens"},
	}
	cfg.Security = []map[string][]string{{"bearer": {}}}
	api := humago.New(mux, cfg)
	api.UseMiddleware(authorize(api, opt.Tokens))

	s := &server{st: st, hub: h, log: log, now: time.Now}
	s.register(api)
	return api
}

// authorize requires one of tokens as a bearer token, if there are any.
func authorize(api huma.API, tokens []string) func(huma.Context, func(huma.Context)) {
	return func(ctx huma.Context, next func(huma.Context)) {
		if len(tokens) == 0 {
			next(ctx)
			return
		}
		got, ok := strings.CutPrefix(ctx.Header("Authorization"), "Bearer ")
		if ok && slices.ContainsFunc(tokens, func(t string) bool {
			return subtle.ConstantTimeCompare([]byte(got), []byte(t)) == 1
		}) {
			next(ctx)
			return
		}
		ctx.SetHeader("WWW-Authenticate", `Bearer realm="meshub"`)
		huma.WriteErr(api, ctx, http.StatusUnauthorized, "a valid bearer token is required")
	}
}

type server struct {
	st  *store.Store
	hub Hub
	log *slog.Logger
	now func() time.Time
}

// internal logs an unexpected error and hides it from the client.
func (s *server) internal(err error) error {
	s.log.Error("api", "err", err)
	return huma.Error500InternalServerError("internal error")
}

// hubError maps an error from the hub to a response.
func (s *server) hubError(err error) error {
	switch {
	case errors.Is(err, hub.ErrUnknownDevice):
		return huma.Error404NotFound(err.Error())
	case errors.Is(err, hub.ErrNotPending):
		return huma.Error409Conflict(err.Error())
	case errors.Is(err, hub.ErrNoGateway):
		return huma.Error503ServiceUnavailable(err.Error())
	case errors.Is(err, hub.ErrInvalid):
		return huma.Error422UnprocessableEntity(err.Error())
	}
	return s.internal(err)
}

// device returns the device a request names.
func (s *server) device(ctx context.Context, pubKey string) (store.Device, error) {
	d, ok, err := s.st.Device(ctx, strings.ToUpper(pubKey))
	if err != nil {
		return d, s.internal(err)
	}
	if !ok {
		return d, huma.Error404NotFound(hub.ErrUnknownDevice.Error())
	}
	return d, nil
}

// Inputs shared by several operations. They are exported so that huma can
// fill them in when they are embedded.
type (
	DevicePath struct {
		PubKey string `path:"pubkey" pattern:"^[0-9A-Fa-f]{64}$" doc:"The device's public key, hex"`
	}
	CommandPath struct {
		DevicePath
		ID uint32 `path:"id"`
	}
	EventPage struct {
		After  int64  `query:"after" minimum:"0" doc:"Only events with a higher ID: the last ID seen, to resume from it"`
		Before int64  `query:"before" minimum:"0" doc:"Only events with a lower ID, to page backwards"`
		Order  string `query:"order" enum:"asc,desc" default:"asc" doc:"By ID, oldest or newest first"`
		Limit  int    `query:"limit" minimum:"1" maximum:"1000" default:"100"`
	}
)

func (p EventPage) query() store.EventQuery {
	return store.EventQuery{After: p.After, Before: p.Before, Desc: p.Order == "desc", Limit: p.Limit}
}

type body[T any] struct{ Body T }

func reply[T any](v T) *body[T] { return &body[T]{Body: v} }

type (
	devicesBody struct {
		Devices []appproto.Device `json:"devices"`
	}
	eventsBody struct {
		Events []appproto.Event `json:"events"`
	}
	commandsBody struct {
		Commands []appproto.Command `json:"commands"`
	}
	gatewaysBody struct {
		Gateways []appproto.Gateway `json:"gateways"`
	}
	nodesBody struct {
		Nodes []appproto.Node `json:"nodes"`
	}
	advertBody struct {
		Gateways int `json:"gateways" doc:"How many gateways the advert was queued on"`
	}
)

func (s *server) register(api huma.API) {
	op := func(id, method, path, tag, summary string) huma.Operation {
		return huma.Operation{OperationID: id, Method: method, Path: Prefix + path, Tags: []string{tag}, Summary: summary}
	}

	// Hub and mesh.
	huma.Register(api, op("get-hub", http.MethodGet, "/hub", "Hub", "Get the hub's identity and recent transmissions"),
		func(ctx context.Context, _ *struct{}) (*body[appproto.Hub], error) {
			return reply(appconv.Hub(s.hub.Status())), nil
		})
	huma.Register(api, op("send-advert", http.MethodPost, "/hub/advert", "Hub", "Flood the hub's advert from every online gateway"),
		func(ctx context.Context, _ *struct{}) (*body[advertBody], error) {
			n, err := s.hub.SendAdvert(ctx)
			if err != nil {
				return nil, s.hubError(err)
			}
			return reply(advertBody{Gateways: n}), nil
		})
	huma.Register(api, op("list-gateways", http.MethodGet, "/gateways", "Mesh", "List gateways"),
		func(ctx context.Context, _ *struct{}) (*body[gatewaysBody], error) {
			gws, err := s.st.Gateways(ctx)
			if err != nil {
				return nil, s.internal(err)
			}
			out := gatewaysBody{Gateways: make([]appproto.Gateway, len(gws))}
			for i, g := range gws {
				out.Gateways[i] = appconv.Gateway(g, s.now())
			}
			return reply(out), nil
		})
	huma.Register(api, op("list-nodes", http.MethodGet, "/nodes", "Mesh", "List mesh nodes known from their adverts"),
		func(ctx context.Context, _ *struct{}) (*body[nodesBody], error) {
			nodes, err := s.st.Nodes(ctx)
			if err != nil {
				return nil, s.internal(err)
			}
			out := nodesBody{Nodes: make([]appproto.Node, len(nodes))}
			for i, n := range nodes {
				out.Nodes[i] = appconv.Node(n)
			}
			return reply(out), nil
		})

	// Devices.
	huma.Register(api, op("list-devices", http.MethodGet, "/devices", "Devices", "List devices"),
		func(ctx context.Context, in *struct {
			Program string `query:"program" doc:"Only devices running this program"`
			Status  string `query:"status" enum:"pending,paired,rejected" doc:"Only devices with this status"`
		}) (*body[devicesBody], error) {
			devices, err := s.st.Devices(ctx)
			if err != nil {
				return nil, s.internal(err)
			}
			out := devicesBody{Devices: []appproto.Device{}}
			for _, d := range devices {
				if (in.Program == "" || d.Program == in.Program) && (in.Status == "" || d.Status == in.Status) {
					out.Devices = append(out.Devices, appconv.Device(d))
				}
			}
			return reply(out), nil
		})
	huma.Register(api, op("get-device", http.MethodGet, "/devices/{pubkey}", "Devices", "Get a device"),
		func(ctx context.Context, in *DevicePath) (*body[appproto.Device], error) {
			d, err := s.device(ctx, in.PubKey)
			if err != nil {
				return nil, err
			}
			return reply(appconv.Device(d)), nil
		})
	setStatus := func(status string) func(context.Context, *DevicePath) (*body[appproto.Device], error) {
		return func(ctx context.Context, in *DevicePath) (*body[appproto.Device], error) {
			pk := strings.ToUpper(in.PubKey)
			if err := s.hub.SetDeviceStatus(ctx, pk, status); err != nil {
				return nil, s.hubError(err)
			}
			d, err := s.device(ctx, pk)
			if err != nil {
				return nil, err
			}
			return reply(appconv.Device(d)), nil
		}
	}
	approve := op("approve-device", http.MethodPost, "/devices/{pubkey}/approve", "Devices", "Approve a device's pairing")
	approve.Description = "The device is paired the next time it asks, which a pending device does every minute."
	huma.Register(api, approve, setStatus(store.DevicePaired))
	huma.Register(api, op("reject-device", http.MethodPost, "/devices/{pubkey}/reject", "Devices", "Reject a device's pairing"), setStatus(store.DeviceRejected))
	del := op("delete-device", http.MethodDelete, "/devices/{pubkey}", "Devices", "Forget a device")
	del.Description = "Its config, events and commands are deleted. If it asks to pair again, it is a new, pending device."
	del.DefaultStatus = http.StatusNoContent
	huma.Register(api, del, func(ctx context.Context, in *DevicePath) (*struct{}, error) {
		if err := s.hub.DeleteDevice(ctx, strings.ToUpper(in.PubKey)); err != nil {
			return nil, s.hubError(err)
		}
		return nil, nil
	})

	// Config.
	huma.Register(api, op("get-device-config", http.MethodGet, "/devices/{pubkey}/config", "Config", "Get the config the hub has for a device"),
		func(ctx context.Context, in *DevicePath) (*body[appproto.Config], error) {
			d, err := s.device(ctx, in.PubKey)
			if err != nil {
				return nil, err
			}
			cfg, err := s.st.Config(ctx, d.PubKey)
			if err != nil {
				return nil, s.internal(err)
			}
			return reply(appconv.Config(cfg)), nil
		})
	setCfg := op("set-device-config", http.MethodPut, "/devices/{pubkey}/config", "Config", "Replace a device's config")
	setCfg.Description = "Stored as the next config version, which the device fetches at its next check-in. " +
		"The device may reject it; its status then shows rejected_version and config_error."
	huma.Register(api, setCfg, func(ctx context.Context, in *struct {
		DevicePath
		Body map[string]any
	}) (*body[appproto.Config], error) {
		raw, _ := json.Marshal(in.Body)
		pk := strings.ToUpper(in.PubKey)
		if _, err := s.hub.SetConfig(ctx, pk, raw); err != nil {
			return nil, s.hubError(err)
		}
		cfg, err := s.st.Config(ctx, pk)
		if err != nil {
			return nil, s.internal(err)
		}
		return reply(appconv.Config(cfg)), nil
	})

	// Events.
	huma.Register(api, op("list-events", http.MethodGet, "/events", "Events", "List events from all devices"),
		func(ctx context.Context, in *struct {
			EventPage
			Program string `query:"program" doc:"Only events from devices running this program"`
		}) (*body[eventsBody], error) {
			q := in.query()
			q.Program = in.Program
			return s.events(ctx, q)
		})
	huma.Register(api, op("list-device-events", http.MethodGet, "/devices/{pubkey}/events", "Events", "List a device's events"),
		func(ctx context.Context, in *struct {
			DevicePath
			EventPage
		}) (*body[eventsBody], error) {
			d, err := s.device(ctx, in.PubKey)
			if err != nil {
				return nil, err
			}
			q := in.query()
			q.PubKey = d.PubKey
			return s.events(ctx, q)
		})

	// Commands.
	huma.Register(api, op("list-device-commands", http.MethodGet, "/devices/{pubkey}/commands", "Commands", "List a device's commands, newest first"),
		func(ctx context.Context, in *struct {
			DevicePath
			Limit int `query:"limit" minimum:"1" maximum:"1000" default:"100"`
		}) (*body[commandsBody], error) {
			d, err := s.device(ctx, in.PubKey)
			if err != nil {
				return nil, err
			}
			cmds, err := s.st.Commands(ctx, d.PubKey, in.Limit)
			if err != nil {
				return nil, s.internal(err)
			}
			out := commandsBody{Commands: make([]appproto.Command, len(cmds))}
			for i, c := range cmds {
				out.Commands[i] = appconv.Command(d.PubKey, c)
			}
			return reply(out), nil
		})
	queue := op("queue-device-command", http.MethodPost, "/devices/{pubkey}/commands", "Commands", "Queue a command for a device")
	queue.Description = "The device fetches it at its next check-in. Its result is published on the device's commands topic."
	queue.DefaultStatus = http.StatusCreated
	huma.Register(api, queue, func(ctx context.Context, in *struct {
		DevicePath
		Body struct {
			Name string          `json:"name" minLength:"1" maxLength:"32"`
			Args json.RawMessage `json:"args,omitempty" doc:"Any JSON value; null if absent"`
		}
	}) (*body[appproto.Command], error) {
		pk := strings.ToUpper(in.PubKey)
		id, err := s.hub.QueueCommand(ctx, pk, in.Body.Name, in.Body.Args)
		if err != nil {
			return nil, s.hubError(err)
		}
		return s.command(ctx, pk, id)
	})
	huma.Register(api, op("get-device-command", http.MethodGet, "/devices/{pubkey}/commands/{id}", "Commands", "Get one of a device's commands"),
		func(ctx context.Context, in *CommandPath) (*body[appproto.Command], error) {
			return s.command(ctx, strings.ToUpper(in.PubKey), in.ID)
		})
	huma.Register(api, op("cancel-device-command", http.MethodPost, "/devices/{pubkey}/commands/{id}/cancel", "Commands", "Cancel a command the device has not fetched"),
		func(ctx context.Context, in *CommandPath) (*body[appproto.Command], error) {
			pk := strings.ToUpper(in.PubKey)
			if _, err := s.command(ctx, pk, in.ID); err != nil {
				return nil, err
			}
			if err := s.hub.CancelCommand(ctx, pk, in.ID); err != nil {
				return nil, s.hubError(err)
			}
			return s.command(ctx, pk, in.ID)
		})
}

func (s *server) events(ctx context.Context, q store.EventQuery) (*body[eventsBody], error) {
	events, err := s.st.EventsByID(ctx, q)
	if err != nil {
		return nil, s.internal(err)
	}
	out := eventsBody{Events: make([]appproto.Event, len(events))}
	for i, e := range events {
		out.Events[i] = appconv.Event(e)
	}
	return reply(out), nil
}

func (s *server) command(ctx context.Context, pubKey string, id uint32) (*body[appproto.Command], error) {
	c, ok, err := s.st.Command(ctx, pubKey, id)
	if err != nil {
		return nil, s.internal(err)
	}
	if !ok {
		return nil, huma.Error404NotFound("unknown command")
	}
	return reply(appconv.Command(pubKey, c)), nil
}
