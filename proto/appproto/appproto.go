// Package appproto is the contract between the meshub backend and the
// applications built on it: the JSON values of its HTTP API (/api/v1) and
// the MQTT topics it publishes device data on.
//
//	meshub/app/v1/{program}/devices/{PUBLIC_KEY}/events    Event JSON, QoS 1
//	meshub/app/v1/{program}/devices/{PUBLIC_KEY}/status    Device JSON, QoS 1, retained
//	meshub/app/v1/{program}/devices/{PUBLIC_KEY}/commands  Command JSON, QoS 1
//
// {program} is what the device runs, as it reported when it joined (see
// TopicLevel), so an application subscribes to its own devices with
// "meshub/app/v1/{program}/#".
//
// Events are delivered at least once, also across broker outages and
// backend restarts: deduplicate them by ID, and backfill a gap with
// GET /api/v1/events?after={ID}. A device's status is republished whenever
// it changes and on every backend connect; an empty retained message clears
// it when the device is deleted. Command updates are best effort; GET
// /api/v1/devices/{PUBLIC_KEY}/commands has the full history.
//
// Package github.com/gapidobri/meshub/client implements both for Go.
package appproto

import (
	"encoding/json"
	"strings"
	"time"
)

// TopicPrefix starts every application topic. Its version changes only
// with incompatible changes to the topics or their messages.
const TopicPrefix = "meshub/app/v1"

// TopicLevel makes s usable as one MQTT topic level: the wildcards '+' and
// '#' and the separator '/' become '_', and an empty s becomes "_".
func TopicLevel(s string) string {
	if s == "" {
		return "_"
	}
	return strings.Map(func(r rune) rune {
		switch r {
		case '/', '+', '#', 0:
			return '_'
		}
		return r
	}, s)
}

func deviceTopic(program, pubKey, kind string) string {
	return TopicPrefix + "/" + TopicLevel(program) + "/devices/" + pubKey + "/" + kind
}

// EventsTopic is where a device's events are published.
func EventsTopic(program, pubKey string) string { return deviceTopic(program, pubKey, KindEvents) }

// StatusTopic is where a device's status is retained.
func StatusTopic(program, pubKey string) string { return deviceTopic(program, pubKey, KindStatus) }

// CommandsTopic is where updates to a device's commands are published.
func CommandsTopic(program, pubKey string) string { return deviceTopic(program, pubKey, KindCommands) }

// ProgramFilter matches all of a program's topics.
func ProgramFilter(program string) string { return TopicPrefix + "/" + TopicLevel(program) + "/#" }

// DevicesFilter matches the device topics of a program, or of all programs
// if program is empty.
func DevicesFilter(program string) string {
	level := "+"
	if program != "" {
		level = TopicLevel(program)
	}
	return TopicPrefix + "/" + level + "/devices/+/+"
}

// Topic kinds, the last level of a device topic.
const (
	KindEvents   = "events"
	KindStatus   = "status"
	KindCommands = "commands"
)

// ParseTopic splits a device topic. program is as it appears in the topic,
// after TopicLevel.
func ParseTopic(topic string) (program, pubKey, kind string, ok bool) {
	rest, ok := strings.CutPrefix(topic, TopicPrefix+"/")
	if !ok {
		return "", "", "", false
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 4 || parts[1] != "devices" {
		return "", "", "", false
	}
	return parts[0], parts[2], parts[3], true
}

// Device statuses.
const (
	DevicePending  = "pending"  // asked to pair, waiting for approval
	DevicePaired   = "paired"   // approved
	DeviceRejected = "rejected" // refused; it is told so when it asks again
)

// Device is a device paired, or asking to pair, with the hub.
type Device struct {
	PubKey   string `json:"pubkey" doc:"MeshCore public key, upper-case hex"`
	Status   string `json:"status" enum:"pending,paired,rejected"`
	Program  string `json:"program" doc:"What the device runs, as reported when it joined"`
	Firmware string `json:"firmware"`
	Claim    string `json:"claim" doc:"Claim code the device joined with, for matching it to its owner"`

	ConfigVersion   uint32 `json:"config_version" doc:"Config version the device has"`
	WantVersion     uint32 `json:"want_version" doc:"Config version the hub has for it; 0 if none"`
	RejectedVersion uint32 `json:"rejected_version" doc:"Last config version the device rejected; 0 if none"`
	ConfigError     string `json:"config_error" doc:"Why the device rejected RejectedVersion"`

	BatteryMV   int        `json:"battery_mv" doc:"Battery voltage at the last check-in, in millivolts"`
	WakeReason  string     `json:"wake_reason" enum:"unknown,power_on,timer,pin,other"`
	UptimeS     uint32     `json:"uptime_s" doc:"Uptime at the last check-in, in seconds"`
	LastHelloAt *time.Time `json:"last_hello_at,omitempty" doc:"Last check-in; absent before the first"`
	JoinedAt    time.Time  `json:"joined_at"`
}

// Event is something a device reported.
type Event struct {
	ID      int64  `json:"id" doc:"Increases with every event the backend stores, across devices"`
	Device  string `json:"device" doc:"The device's public key"`
	Program string `json:"program"`
	Seq     uint32 `json:"seq" doc:"The device's own event number"`
	Kind    string `json:"kind"`
	// Data is the event's value as the device encoded it.
	Data       json.RawMessage `json:"data"`
	DeviceTime *time.Time      `json:"device_time,omitempty" doc:"When it happened by the device's clock; absent if the device did not know"`
	ReceivedAt time.Time       `json:"received_at"`
}

// Command statuses.
const (
	CommandPending   = "pending"   // waiting for the device to fetch it
	CommandOK        = "ok"        // the device ran it
	CommandFailed    = "failed"    // the device ran it and it failed
	CommandUnknown   = "unknown"   // the device does not know the command
	CommandCancelled = "cancelled" // cancelled before the device fetched it
)

// Command is a command queued for a device.
type Command struct {
	ID        uint32          `json:"id"`
	Device    string          `json:"device"`
	Name      string          `json:"name"`
	Args      json.RawMessage `json:"args"`
	Status    string          `json:"status" enum:"pending,ok,failed,unknown,cancelled"`
	Result    json.RawMessage `json:"result,omitempty" doc:"What the device returned, once done"`
	CreatedAt time.Time       `json:"created_at"`
	DoneAt    *time.Time      `json:"done_at,omitempty"`
}

// Config is the config the hub has for a device.
type Config struct {
	Version   uint32          `json:"version" doc:"0 if none was ever set"`
	Config    json.RawMessage `json:"config" doc:"A JSON object"`
	UpdatedAt *time.Time      `json:"updated_at,omitempty"`
}

// Gateway is a gateway's last reported status.
type Gateway struct {
	PubKey       string          `json:"pubkey"`
	IATA         string          `json:"iata"`
	Name         string          `json:"name"`
	Status       string          `json:"status" doc:"As last reported: online or offline"`
	Online       bool            `json:"online" doc:"Reported online recently enough to be trusted"`
	Model        string          `json:"model"`
	Firmware     string          `json:"firmware"`
	Radio        string          `json:"radio"`
	Stats        json.RawMessage `json:"stats"`
	FirstSeen    time.Time       `json:"first_seen"`
	LastStatusAt time.Time       `json:"last_status_at"`
}

// Node is a mesh node known from its adverts.
type Node struct {
	PubKey      string    `json:"pubkey"`
	Name        string    `json:"name"`
	Type        string    `json:"type" enum:"unknown,chat,repeater,room,sensor"`
	Lat         *float64  `json:"lat,omitempty"`
	Lon         *float64  `json:"lon,omitempty"`
	AdvertTime  time.Time `json:"advert_time"`
	FirstSeen   time.Time `json:"first_seen"`
	LastHeardAt time.Time `json:"last_heard_at"`
	Links       []Link    `json:"links" doc:"Gateways that hear it, fewest hops first"`
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

// Hub is the backend's own presence on the mesh.
type Hub struct {
	PubKey     string     `json:"pubkey" doc:"Devices are built with this key"`
	Name       string     `json:"name"`
	LastAdvert *time.Time `json:"last_advert,omitempty"`
	LastEcho   *Echo      `json:"last_echo,omitempty" doc:"The hub's own advert last heard back from the mesh"`
	Results    []TxResult `json:"results" doc:"Recent transmit results, newest first"`
}

// Echo is the hub's own advert heard back through a gateway.
type Echo struct {
	Gateway string    `json:"gateway"`
	Hops    int       `json:"hops"`
	SNR     int       `json:"snr"`
	At      time.Time `json:"at"`
}

// TxResult is a gateway's report on something the hub asked it to send.
type TxResult struct {
	Gateway string    `json:"gateway"`
	What    string    `json:"what"`
	Status  string    `json:"status"`
	At      time.Time `json:"at"`
}
