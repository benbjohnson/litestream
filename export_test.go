package litestream

// This file exports internal functions for testing purposes.

// CalcWALSizeForTest exposes calcWALSize for testing.
func CalcWALSizeForTest(pageSize, pageN uint32) int64 {
	return calcWALSize(pageSize, pageN)
}
