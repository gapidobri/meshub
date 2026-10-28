package appproto

import "testing"

func TestTopics(t *testing.T) {
	pk := "ABCD"
	topic := EventsTopic("weather/v2+#", pk)
	if topic != "meshub/app/v1/weather_v2__/devices/ABCD/events" {
		t.Errorf("EventsTopic = %q", topic)
	}
	program, gotPK, kind, ok := ParseTopic(topic)
	if !ok || program != "weather_v2__" || gotPK != pk || kind != KindEvents {
		t.Errorf("ParseTopic = %q %q %q %v", program, gotPK, kind, ok)
	}
	for _, bad := range []string{"meshub/app/v1/x/devices/ABCD", "meshub/app/v2/x/devices/A/status", "meshub/app/v1/x/nodes/A/status"} {
		if _, _, _, ok := ParseTopic(bad); ok {
			t.Errorf("ParseTopic(%q) ok", bad)
		}
	}
	if f := DevicesFilter(""); f != "meshub/app/v1/+/devices/+/+" {
		t.Errorf("DevicesFilter = %q", f)
	}
	if TopicLevel("") != "_" {
		t.Error("empty program not replaced")
	}
}
