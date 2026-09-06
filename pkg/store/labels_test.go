package store

import "testing"

func TestLabelsJSONV2RoundTrip(t *testing.T) {
	want := map[string]string{
		"environment": "production",
		"region":      "ap-south-1",
		"component":   "ingester",
	}

	encoded := encodeLabels(want)
	got := decodeLabels(encoded)
	if len(got) != len(want) {
		t.Fatalf("got %d labels, want %d", len(got), len(want))
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("label %q = %q, want %q", key, got[key], value)
		}
	}
}

func BenchmarkFingerprintDelimiterCount(b *testing.B) {
	fingerprint := []byte("component\xffingester\xffenvironment\xffproduction\xffregion\xffap-south-1\xff")
	b.ReportMetric(float64(len(fingerprint)), "bytes/op")
	for b.Loop() {
		if got := countByte(fingerprint, customLabelSeparator); got != 6 {
			b.Fatalf("got %d separators, want 6", got)
		}
	}
}
