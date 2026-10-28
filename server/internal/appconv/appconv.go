// Package appconv converts the backend's records to the values
// applications see (package appproto).
package appconv

import (
	"encoding/json"
	"time"

	"github.com/gapidobri/meshub/proto/appproto"
	"github.com/gapidobri/meshub/proto/gwproto"
	"github.com/gapidobri/meshub/proto/hubproto"
	"github.com/gapidobri/meshub/server/internal/hub"
	"github.com/gapidobri/meshub/server/internal/store"
)

// Device converts a device.
func Device(d store.Device) appproto.Device {
	return appproto.Device{
		PubKey:          d.PubKey,
		Status:          d.Status,
		Program:         d.Program,
		Firmware:        d.Firmware,
		Claim:           d.Claim,
		ConfigVersion:   d.ConfigVersion,
		WantVersion:     d.WantVersion,
		RejectedVersion: d.RejectedVersion,
		ConfigError:     d.ConfigError,
		BatteryMV:       d.BatteryMV,
		WakeReason:      WakeReason(d.WakeReason),
		UptimeS:         d.UptimeS,
		LastHelloAt:     timePtr(d.LastHelloAt),
		JoinedAt:        d.JoinedAt.UTC(),
	}
}

// WakeReason names a hubproto wake reason.
func WakeReason(r int) string {
	switch r {
	case hubproto.WakePowerOn:
		return "power_on"
	case hubproto.WakeTimer:
		return "timer"
	case hubproto.WakePin:
		return "pin"
	case hubproto.WakeOther:
		return "other"
	}
	return "unknown"
}

// Event converts an event read with its device and program.
func Event(e store.Event) appproto.Event {
	return appproto.Event{
		ID:         e.ID,
		Device:     e.PubKey,
		Program:    e.Program,
		Seq:        e.Seq,
		Kind:       e.Kind,
		Data:       rawJSON(e.Data),
		DeviceTime: timePtr(e.DeviceTime),
		ReceivedAt: e.ReceivedAt.UTC(),
	}
}

// Command converts a device's command.
func Command(device string, c store.Command) appproto.Command {
	out := appproto.Command{
		ID:        c.ID,
		Device:    device,
		Name:      c.Name,
		Args:      rawJSON(c.Args),
		Status:    c.Status,
		CreatedAt: c.CreatedAt.UTC(),
		DoneAt:    timePtr(c.DoneAt),
	}
	if c.Result != "" {
		out.Result = rawJSON(c.Result)
	}
	return out
}

// Config converts a device's config.
func Config(c store.DeviceConfig) appproto.Config {
	out := appproto.Config{Version: c.Version, Config: json.RawMessage("{}")}
	if c.Version > 0 {
		out.Config = rawJSON(c.JSON)
		out.UpdatedAt = timePtr(c.UpdatedAt)
	}
	return out
}

// Gateway converts a gateway, judging whether it is online at now.
func Gateway(g store.Gateway, now time.Time) appproto.Gateway {
	return appproto.Gateway{
		PubKey:       g.PubKey,
		IATA:         g.IATA,
		Name:         g.Name,
		Status:       g.Status,
		Online:       g.Status == gwproto.StatusOnline && now.Sub(g.LastStatusAt) <= store.GatewayStaleAfter,
		Model:        g.Model,
		Firmware:     g.Firmware,
		Radio:        g.Radio,
		Stats:        rawJSON(g.Stats),
		FirstSeen:    g.FirstSeen.UTC(),
		LastStatusAt: g.LastStatusAt.UTC(),
	}
}

// Node converts a mesh node.
func Node(n store.Node) appproto.Node {
	out := appproto.Node{
		PubKey:      n.PubKey,
		Name:        n.Name,
		Type:        NodeType(n.Type),
		AdvertTime:  n.AdvertTime.UTC(),
		FirstSeen:   n.FirstSeen.UTC(),
		LastHeardAt: n.LastHeardAt.UTC(),
		Links:       []appproto.Link{},
	}
	if n.HasLoc {
		lat, lon := n.Lat, n.Lon
		out.Lat, out.Lon = &lat, &lon
	}
	for _, l := range n.Links {
		out.Links = append(out.Links, appproto.Link{
			Gateway: l.Gateway, SNR: l.SNR, RSSI: l.RSSI, Hops: l.Hops,
			HeardCount: l.HeardCount, LastHeardAt: l.LastHeardAt.UTC(),
		})
	}
	return out
}

// NodeType names a MeshCore advert type.
func NodeType(t uint8) string {
	switch t {
	case 1:
		return "chat"
	case 2:
		return "repeater"
	case 3:
		return "room"
	case 4:
		return "sensor"
	}
	return "unknown"
}

// Hub converts the hub's status.
func Hub(s hub.Status) appproto.Hub {
	out := appproto.Hub{PubKey: s.PublicKey, Name: s.Name, LastAdvert: timePtr(s.LastAdvert), Results: []appproto.TxResult{}}
	if e := s.LastEcho; e != nil {
		out.LastEcho = &appproto.Echo{Gateway: e.Gateway, Hops: e.Hops, SNR: e.SNR, At: e.At.UTC()}
	}
	for _, r := range s.Results {
		out.Results = append(out.Results, appproto.TxResult{Gateway: r.Gateway, What: r.What, Status: r.Status, At: r.At.UTC()})
	}
	return out
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	t = t.UTC()
	return &t
}

// rawJSON passes on stored JSON, as null if it is not valid.
func rawJSON(s string) json.RawMessage {
	if !json.Valid([]byte(s)) {
		return json.RawMessage("null")
	}
	return json.RawMessage(s)
}
