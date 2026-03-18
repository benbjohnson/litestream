//go:build integration && soak

package integration

import (
	"os"
	"time"
)

// LoadProfile defines a data shape for behavioral testing.
// Each profile represents a different production workload pattern.
type LoadProfile struct {
	Name        string
	Description string

	// Load generation parameters
	WriteRate   int    // writes/sec
	Pattern     string // "constant", "burst", "wave", "random"
	PayloadSize int    // bytes per write (0 = default 4KB)
	Workers     int    // concurrent writers

	// Expected behavioral bounds for LTX assertions
	MaxL0Pages      int     // max pages per L0 file under normal sync
	MaxWALSizeMB    float64 // max allowed WAL size
	SnapshotWindow  float64 // multiplier on configured snapshot interval (e.g., 2.0 = allow 2x)
	CompactionSlack float64 // tolerance for compaction timing (e.g., 0.5 = +/- 50%)

	// Populate settings
	InitialSize string // initial database size (e.g., "5MB", "50MB")
}

// DefaultLoadProfiles returns the three canonical load profiles discussed by Ben and Cory:
// low volume, high volume, and burst volume.
func DefaultLoadProfiles(shortMode bool) []LoadProfile {
	if shortMode {
		return []LoadProfile{
			ShortLowVolumeProfile(),
			ShortHighVolumeProfile(),
			ShortBurstVolumeProfile(),
		}
	}
	return []LoadProfile{
		LowVolumeProfile(),
		HighVolumeProfile(),
		BurstVolumeProfile(),
	}
}

// LowVolumeProfile simulates a low-traffic database with infrequent writes.
// This catches issues where "things don't get cleaned up right" — compaction
// and retention at low write rates, WAL growth when writes are sparse.
func LowVolumeProfile() LoadProfile {
	return LoadProfile{
		Name:            "low-volume",
		Description:     "Low traffic: 10 writes/sec, constant. Catches retention/compaction issues at low write rates.",
		WriteRate:       10,
		Pattern:         "constant",
		PayloadSize:     1024,
		Workers:         1,
		MaxL0Pages:      50,
		MaxWALSizeMB:    100,
		SnapshotWindow:  2.0,
		CompactionSlack: 0.5,
		InitialSize:     "5MB",
	}
}

// HighVolumeProfile simulates a busy production database with sustained writes.
// This catches performance regressions, excessive snapshotting under load,
// and WAL growth issues.
func HighVolumeProfile() LoadProfile {
	return LoadProfile{
		Name:            "high-volume",
		Description:     "High traffic: 500 writes/sec, wave pattern. Catches performance regressions under sustained load.",
		WriteRate:       500,
		Pattern:         "wave",
		PayloadSize:     4096,
		Workers:         8,
		MaxL0Pages:      500,
		MaxWALSizeMB:    500,
		SnapshotWindow:  2.0,
		CompactionSlack: 0.5,
		InitialSize:     "50MB",
	}
}

// BurstVolumeProfile simulates a database with intermittent high-traffic bursts.
// This catches sync/snapshot bugs triggered by load transitions, where going
// from idle to heavy writes (or vice versa) can confuse WAL tracking.
func BurstVolumeProfile() LoadProfile {
	return LoadProfile{
		Name:            "burst-volume",
		Description:     "Burst traffic: alternating idle/busy periods. Catches bugs from load transitions.",
		WriteRate:       1000,
		Pattern:         "burst",
		PayloadSize:     2048,
		Workers:         4,
		MaxL0Pages:      500,
		MaxWALSizeMB:    300,
		SnapshotWindow:  2.5,
		CompactionSlack: 0.6,
		InitialSize:     "20MB",
	}
}

// Short-mode profiles reduce duration/sizes for CI gate tests (~2 min each).

func ShortLowVolumeProfile() LoadProfile {
	p := LowVolumeProfile()
	p.InitialSize = "1MB"
	return p
}

func ShortHighVolumeProfile() LoadProfile {
	p := HighVolumeProfile()
	p.WriteRate = 100
	p.Workers = 4
	p.InitialSize = "5MB"
	return p
}

func ShortBurstVolumeProfile() LoadProfile {
	p := BurstVolumeProfile()
	p.WriteRate = 500
	p.Workers = 2
	p.MaxL0Pages = 300
	p.InitialSize = "5MB"
	return p
}

// ProfileDuration returns the test duration for a profile.
// Honors the SOAK_DURATION env var for nightly workflow overrides (ignored in short mode).
func ProfileDuration(shortMode bool) time.Duration {
	if shortMode {
		return 2 * time.Minute
	}
	if v := os.Getenv("SOAK_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return 30 * time.Minute
}

// ProfileSnapshotInterval returns the snapshot interval for test configs.
// Must match the value written by CreateSoakConfig.
func ProfileSnapshotInterval(shortMode bool) time.Duration {
	if shortMode {
		return 30 * time.Second
	}
	return 10 * time.Minute
}

// ProfileCompactionIntervals returns compaction level intervals for test configs.
func ProfileCompactionIntervals(shortMode bool) map[int]time.Duration {
	if shortMode {
		return map[int]time.Duration{
			1: 15 * time.Second,
			2: 30 * time.Second,
			3: 1 * time.Minute,
		}
	}
	return map[int]time.Duration{
		1: 30 * time.Second,
		2: 1 * time.Minute,
		3: 5 * time.Minute,
		4: 15 * time.Minute,
		5: 30 * time.Minute,
	}
}
