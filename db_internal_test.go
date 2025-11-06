package litestream

import "testing"

// TestCalcWALSize ensures calcWALSize doesn't overflow with large page sizes.
// Regression test for uint32 overflow bug where large page sizes (>=16KB)
// caused incorrect WAL size calculations, triggering checkpoints too early.
func TestCalcWALSize(t *testing.T) {
	tests := []struct {
		name     string
		pageSize uint32
		pageN    uint32
		expected int64
	}{
		{
			name:     "4KB pages, 500k pages (default TruncatePageN)",
			pageSize: 4096,
			pageN:    500000,
			expected: int64(WALHeaderSize) + (int64(WALFrameHeaderSize+4096) * 500000),
		},
		{
			name:     "16KB pages, 500k pages",
			pageSize: 16384,
			pageN:    500000,
			expected: int64(WALHeaderSize) + (int64(WALFrameHeaderSize+16384) * 500000),
		},
		{
			name:     "32KB pages, 500k pages",
			pageSize: 32768,
			pageN:    500000,
			// Expected: 16,396,000,032 bytes (~16.4 GB). Bug previously overflowed.
			expected: int64(WALHeaderSize) + (int64(WALFrameHeaderSize+32768) * 500000),
		},
		{
			name:     "64KB pages, 500k pages",
			pageSize: 65536,
			pageN:    500000,
			expected: int64(WALHeaderSize) + (int64(WALFrameHeaderSize+65536) * 500000),
		},
		{
			name:     "1KB pages, 1k pages (min checkpoint)",
			pageSize: 1024,
			pageN:    1000,
			expected: int64(WALHeaderSize) + (int64(WALFrameHeaderSize+1024) * 1000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calcWALSize(tt.pageSize, tt.pageN)
			if got != tt.expected {
				t.Errorf("calcWALSize(%d, %d) = %d, want %d (%.2f GB vs %.2f GB)",
					tt.pageSize, tt.pageN, got, tt.expected,
					float64(got)/(1024*1024*1024), float64(tt.expected)/(1024*1024*1024))
			}

			if got <= 0 {
				t.Errorf("calcWALSize(%d, %d) = %d, should be positive", tt.pageSize, tt.pageN, got)
			}

			if tt.pageSize >= 32768 && tt.pageN >= 100000 {
				minExpected := int64(10 * 1024 * 1024 * 1024)
				if got < minExpected {
					t.Errorf("calcWALSize(%d, %d) = %d (%.2f GB), suspiciously small, possible overflow",
						tt.pageSize, tt.pageN, got, float64(got)/(1024*1024*1024))
				}
			}
		})
	}
}
