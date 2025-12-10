package internal

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestReplicaOperationMetrics verifies that replica operation metrics are properly
// incremented when operations are performed.
func TestReplicaOperationMetrics(t *testing.T) {
	// Get baseline counts for a unique test label to avoid interference
	testType := "test-metrics"
	initialTotal := testutil.ToFloat64(
		OperationTotalCounterVec.WithLabelValues(testType, "PUT"))
	initialBytes := testutil.ToFloat64(
		OperationBytesCounterVec.WithLabelValues(testType, "PUT"))

	// Simulate PUT operation
	OperationTotalCounterVec.WithLabelValues(testType, "PUT").Inc()
	OperationBytesCounterVec.WithLabelValues(testType, "PUT").Add(1024)

	// Verify total counter was incremented
	newTotal := testutil.ToFloat64(
		OperationTotalCounterVec.WithLabelValues(testType, "PUT"))
	if newTotal != initialTotal+1 {
		t.Fatalf("litestream_replica_operation_total=%v, want %v", newTotal, initialTotal+1)
	}

	// Verify bytes counter was incremented
	newBytes := testutil.ToFloat64(
		OperationBytesCounterVec.WithLabelValues(testType, "PUT"))
	if newBytes != initialBytes+1024 {
		t.Fatalf("litestream_replica_operation_bytes=%v, want %v", newBytes, initialBytes+1024)
	}

	// Test other operations
	operations := []string{"GET", "DELETE", "LIST"}
	for _, op := range operations {
		baseTotal := testutil.ToFloat64(
			OperationTotalCounterVec.WithLabelValues(testType, op))

		OperationTotalCounterVec.WithLabelValues(testType, op).Inc()

		afterTotal := testutil.ToFloat64(
			OperationTotalCounterVec.WithLabelValues(testType, op))
		if afterTotal != baseTotal+1 {
			t.Fatalf("litestream_replica_operation_total[%s]=%v, want %v", op, afterTotal, baseTotal+1)
		}
	}
}
