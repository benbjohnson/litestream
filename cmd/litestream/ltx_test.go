package main

import (
	"testing"

	"github.com/superfly/ltx"
)

// TestTXIDString verifies the ltx.TXID.String() method formats correctly
func TestTXIDString(t *testing.T) {
	tests := []struct {
		name     string
		txid     ltx.TXID
		expected string
	}{
		{
			name:     "zero",
			txid:     ltx.TXID(0),
			expected: "0000000000000000",
		},
		{
			name:     "small value",
			txid:     ltx.TXID(2),
			expected: "0000000000000002",
		},
		{
			name:     "large value",
			txid:     ltx.TXID(0x30303030303032),
			expected: "0030303030303032",
		},
		{
			name:     "max value",
			txid:     ltx.TXID(0xffffffffffffffff),
			expected: "ffffffffffffffff",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.txid.String()
			if result != tt.expected {
				t.Errorf("TXID(%x).String() = %s, want %s", uint64(tt.txid), result, tt.expected)
			}
			// Verify it's always 16 characters
			if len(result) != 16 {
				t.Errorf("TXID string should always be 16 characters, got %d", len(result))
			}
		})
	}
}
