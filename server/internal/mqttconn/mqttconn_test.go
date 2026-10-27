package mqttconn

import "testing"

func TestTopicMatches(t *testing.T) {
	for _, tc := range []struct {
		filter, topic string
		want          bool
	}{
		{"meshcore/+/+/status", "meshcore/LJU/AB/status", true},
		{"meshcore/+/+/status", "meshcore/LJU/AB/packets", false},
		{"meshcore/+/+/status", "meshcore/LJU/status", false},
		{"meshcore/+/+/status", "meshcore/LJU/AB/status/x", false},
		{"meshub/gw/+/tx/result", "meshub/gw/AB/tx/result", true},
		{"meshub/gw/+/tx/result", "meshub/gw/AB/tx", false},
		{"meshub/#", "meshub/gw/AB/tx", true},
		{"a/b", "a/b", true},
		{"a/b", "a/c", false},
	} {
		if got := topicMatches(tc.filter, tc.topic); got != tc.want {
			t.Errorf("topicMatches(%q, %q) = %v, want %v", tc.filter, tc.topic, got, tc.want)
		}
	}
}
