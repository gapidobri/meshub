package gwproto

import "time"

// Gateway status values.
const (
	StatusOnline  = "online"
	StatusOffline = "offline"
)

// Status is a gateway's status message. The online status is published
// retained on connect and periodically; the offline status is the MQTT will.
type Status struct {
	Status          string `json:"status"`
	Timestamp       string `json:"timestamp"`
	Origin          string `json:"origin"`
	OriginID        string `json:"origin_id"`
	Radio           string `json:"radio"` // "freq_mhz,bw_khz,sf,cr", like the MeshCore CLI's "get radio"
	Model           string `json:"model"`
	FirmwareVersion string `json:"firmware_version"`
	ClientVersion   string `json:"client_version"`
	Stats           *Stats `json:"stats,omitempty"`
}

// Stats are gateway counters. The first block mirrors the device stats
// meshcoretomqtt reports; the rest are meshub-specific.
type Stats struct {
	UptimeSecs uint32 `json:"uptime_secs"`
	DebugFlags uint16 `json:"debug_flags"`
	QueueLen   int    `json:"queue_len"`
	NoiseFloor int    `json:"noise_floor"`
	TxAirSecs  uint32 `json:"tx_air_secs"`
	RxAirSecs  uint32 `json:"rx_air_secs"`

	FreePackets    int    `json:"free_packets"`
	UplinkDropped  uint32 `json:"uplink_dropped"`
	TxBudgetMs     uint32 `json:"tx_budget_ms"`
	MQTTReconnects uint32 `json:"mqtt_reconnects"`
}

// AppendJSON appends the JSON encoding of s, with ts as its timestamp.
// s.Timestamp is ignored.
func (s *Status) AppendJSON(dst []byte, ts time.Time) []byte {
	dst = append(dst, '{')
	dst = appendStrField(dst, "status", s.Status)
	dst = appendTimestamp(dst, "timestamp", ts)
	dst = appendStrField(dst, "origin", s.Origin)
	dst = appendStrField(dst, "origin_id", s.OriginID)
	dst = appendStrField(dst, "radio", s.Radio)
	dst = appendStrField(dst, "model", s.Model)
	dst = appendStrField(dst, "firmware_version", s.FirmwareVersion)
	dst = appendStrField(dst, "client_version", s.ClientVersion)
	if st := s.Stats; st != nil {
		dst = append(appendKey(dst, "stats"), '{')
		dst = appendUintField(dst, "uptime_secs", uint64(st.UptimeSecs))
		dst = appendUintField(dst, "debug_flags", uint64(st.DebugFlags))
		dst = appendIntField(dst, "queue_len", int64(st.QueueLen))
		dst = appendIntField(dst, "noise_floor", int64(st.NoiseFloor))
		dst = appendUintField(dst, "tx_air_secs", uint64(st.TxAirSecs))
		dst = appendUintField(dst, "rx_air_secs", uint64(st.RxAirSecs))
		dst = appendIntField(dst, "free_packets", int64(st.FreePackets))
		dst = appendUintField(dst, "uplink_dropped", uint64(st.UplinkDropped))
		dst = appendUintField(dst, "tx_budget_ms", uint64(st.TxBudgetMs))
		dst = appendUintField(dst, "mqtt_reconnects", uint64(st.MQTTReconnects))
		dst = append(dst, '}')
	}
	return append(dst, '}')
}
