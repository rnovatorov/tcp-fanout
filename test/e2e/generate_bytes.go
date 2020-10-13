package e2e

import (
	"crypto/sha256"
	"strconv"
)

func generateBytes(size int) (string, string) {
	var data []byte
	for i := 0; len(data) < size; i++ {
		chunk := []byte(strconv.Itoa(i))
		data = append(data, chunk...)
	}
	data = data[:size]
	checksum := sha256.Sum256(data)
	return string(data), string(checksum[:])
}
