//go:build goexperiment.simd

package store

import "simd"

// countByte is the Go 1.27 SIMD experiment used by fingerprint decoding.
// Keep the scalar implementation in simd_scalar.go as the default and
// compare both versions with `GOEXPERIMENT=simd go test ./pkg/store`.
func countByte(input []byte, want byte) int {
	needle := simd.BroadcastInt8s(int8(want))
	count := 0
	for len(input) > 0 {
		values := make([]int8, len(input))
		for i, value := range input {
			values[i] = int8(value)
		}

		vector, loaded := simd.LoadInt8sPart(values)
		matched := make([]int8, vector.Len())
		vector.Equal(needle).ToInt8s().Store(matched)
		for i := 0; i < loaded; i++ {
			if matched[i] != 0 {
				count++
			}
		}
		input = input[loaded:]
	}
	return count
}
