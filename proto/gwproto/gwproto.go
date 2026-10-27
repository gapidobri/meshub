// Package gwproto is the MQTT contract between meshub gateways and the
// backend.
//
// Uplink (packets and status) uses the topics and JSON shape of
// github.com/Cisien/meshcoretomqtt, so existing MeshCore observers can feed
// the backend and meshub gateways can feed existing analyzers. Downlink is
// meshub-specific: a small binary transmit request, answered with a JSON
// result.
//
//	meshcore/{IATA}/{PUBLIC_KEY}/packets  gateway → backend  Packet JSON
//	meshcore/{IATA}/{PUBLIC_KEY}/status   gateway → backend  Status JSON (retained, also the will)
//	meshub/gw/{PUBLIC_KEY}/tx             backend → gateway  TxRequest binary
//	meshub/gw/{PUBLIC_KEY}/tx/result      gateway → backend  TxResult JSON
//
// The encoders append to a caller-supplied buffer and do not allocate, so
// gateways can use them under TinyGo. The backend decodes with encoding/json.
package gwproto

// PacketsTopic returns the uplink topic for packets heard or sent by a
// gateway.
func PacketsTopic(iata, pubKey string) string {
	return "meshcore/" + iata + "/" + pubKey + "/packets"
}

// StatusTopic returns the gateway's status topic.
func StatusTopic(iata, pubKey string) string {
	return "meshcore/" + iata + "/" + pubKey + "/status"
}

// TxTopic returns the topic a gateway subscribes to for transmit requests.
func TxTopic(pubKey string) string { return "meshub/gw/" + pubKey + "/tx" }

// TxResultTopic returns the topic a gateway reports transmit outcomes on.
func TxResultTopic(pubKey string) string { return "meshub/gw/" + pubKey + "/tx/result" }
