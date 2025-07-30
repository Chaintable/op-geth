package util

import "testing"

func TestToHash(t *testing.T) {
	event_id := ToHash([]string{"abcd", "2"})
	if event_id != "6e24a85785fd5e2688f1a23aee9d88f3" {
		t.Errorf("Expected 6e24a85785fd5e2688f1a23aee9d88f3, got %s", event_id)
	}
}
