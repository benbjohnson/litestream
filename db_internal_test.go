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
			name:     "4KB pages, 121359 pages (default TruncatePageN)",
			pageSize: 4096,
			pageN:    121359,
			expected: int64(WALHeaderSize) + (int64(WALFrameHeaderSize+4096) * 121359),
		},
		{
			name:     "16KB pages, 121359 pages",
			pageSize: 16384,
			pageN:    121359,
			expected: int64(WALHeaderSize) + (int64(WALFrameHeaderSize+16384) * 121359),
		},
		{
			name:     "32KB pages, 121359 pages",
			pageSize: 32768,
			pageN:    121359,
			// Expected: ~4.0 GB with 32KB pages. Bug previously overflowed.
			expected: int64(WALHeaderSize) + (int64(WALFrameHeaderSize+32768) * 121359),
		},
		{
			name:     "64KB pages, 121359 pages",
			pageSize: 65536,
			pageN:    121359,
			expected: int64(WALHeaderSize) + (int64(WALFrameHeaderSize+65536) * 121359),
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
				// Sanity check: ensure result is at least (page_size * page_count)
				minExpected := int64(tt.pageSize) * int64(tt.pageN)
				if got < minExpected {
					t.Errorf("calcWALSize(%d, %d) = %d (%.2f GB), suspiciously small, possible overflow",
						tt.pageSize, tt.pageN, got, float64(got)/(1024*1024*1024))
				}
			}
		})
	}
}
