package internal

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Shared replica metrics.
var (
	ReplicaSnapshotTotalGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "litestream",
		Subsystem: "replica",
		Name:      "snapshot_total",
		Help:      "The current number of snapshots",
	}, []string{"db", "name"})

	ReplicaWALBytesCounterVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "litestream",
		Subsystem: "replica",
		Name:      "wal_bytes",
		Help:      "The number wal bytes written",
	}, []string{"db", "name"})

	ReplicaWALIndexGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "litestream",
		Subsystem: "replica",
		Name:      "wal_index",
		Help:      "The current WAL index",
	}, []string{"db", "name"})

	ReplicaWALOffsetGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "litestream",
		Subsystem: "replica",
		Name:      "wal_offset",
		Help:      "The current WAL offset",
	}, []string{"db", "name"})

	ReplicaValidationTotalCounterVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "litestream",
		Subsystem: "replica",
		Name:      "validation_total",
		Help:      "The number of validations performed",
	}, []string{"db", "name", "status"})
)
