//go:build !goexperiment.simd

package store

func countByte(input []byte, want byte) int {
	count := 0
	for _, value := range input {
		if value == want {
			count++
		}
	}
	return count
}
