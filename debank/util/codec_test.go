package util

import "testing"

func TestCodec(t *testing.T) {
	buf, err := EncodeToJsonGzip(1)
	if err != nil {
		t.Fatal(err)
	}
	var v int
	err = DecodeFromGzipJson(buf, &v)
	if err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Fatalf("expect 1, got %d", v)
	}
}
