package builder

import (
	"hash/crc64"
	"testing"
)

func TestCRC64NVMECheckVector(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantHex uint64
	}{
		// Standard check value from the CRC RevEng catalog for CRC-64/NVME.
		{"check vector", []byte("123456789"), 0xAE8B14860A799888},
		// Empty input: init XOR xorout = 0xFFFF... XOR 0xFFFF... = 0.
		{"empty", []byte{}, 0x0000000000000000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := crc64.Checksum(tt.input, crc64NVMETable)
			if got != tt.wantHex {
				t.Errorf("CRC-64/NVME(%q) = 0x%016X, want 0x%016X", tt.input, got, tt.wantHex)
			}
		})
	}
}
