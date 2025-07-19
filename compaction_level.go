package litestream

import (
	"fmt"
	"time"
)

// SnapshotLevel represents the level which full snapshots are held.
const SnapshotLevel = 9

// CompactionLevel represents a single part of a multi-level compaction.
// Each level merges LTX files from the previous level into larger time granularities.
type CompactionLevel struct {
	// The numeric level. Must match the index in the list of levels.
	Level int

	// The frequency that the level is compacted from the previous level.
	Interval time.Duration
}

// PrevCompactionAt returns the time when the last compaction occurred.
// Returns the current time if it is exactly a multiple of the level interval.
func (lvl *CompactionLevel) PrevCompactionAt(now time.Time) time.Time {
	return now.Truncate(lvl.Interval).UTC()
}

// NextCompactionAt returns the time until the next compaction occurs.
// Returns the current time if it is exactly a multiple of the level interval.
func (lvl *CompactionLevel) NextCompactionAt(now time.Time) time.Time {
	return lvl.PrevCompactionAt(now).Add(lvl.Interval)
}

// CompactionLevels represents a sorted slice of non-snapshot compaction levels.
type CompactionLevels []*CompactionLevel

// Level returns the compaction level at the given index.
// Returns an error if the index is a snapshot level or is out of bounds.
func (a CompactionLevels) Level(level int) (*CompactionLevel, error) {
	if level == SnapshotLevel {
		return nil, fmt.Errorf("invalid argument, snapshot level")
	}
	if level < 0 || level > a.MaxLevel() {
		return nil, fmt.Errorf("level out of bounds: %d", level)
	}
	return a[level], nil
}

// MaxLevel return the highest non-snapshot compaction level.
func (a CompactionLevels) MaxLevel() int {
	return len(a) - 1
}

// Validate returns an error if the levels are invalid.
func (a CompactionLevels) Validate() error {
	if len(a) == 0 {
		return fmt.Errorf("at least one compaction level is required")
	}

	for i, lvl := range a {
		if i != lvl.Level {
			return fmt.Errorf("compaction level number out of order: %d, expected %d", lvl.Level, i)
		} else if lvl.Level > SnapshotLevel-1 {
			return fmt.Errorf("compaction level cannot exceed %d", SnapshotLevel-1)
		}

		if lvl.Level == 0 && lvl.Interval != 0 {
			return fmt.Errorf("cannot set interval on compaction level zero")
		}

		if lvl.Level != 0 && lvl.Interval <= 0 {
			return fmt.Errorf("interval required for level %d", lvl.Level)
		}
	}
	return nil
}

// IsValidLevel returns true if level is a valid compaction level number.
func (a CompactionLevels) IsValidLevel(level int) bool {
	if level == SnapshotLevel {
		return true
	}
	return level >= 0 && level < len(a)
}

// PrevLevel returns the previous compaction level.
// Returns -1 if there is no previous level.
func (a CompactionLevels) PrevLevel(level int) int {
	if level == SnapshotLevel {
		return a.MaxLevel()
	}
	return level - 1
}

// NextLevel returns the next compaction level.
// Returns -1 if there is no next level.
func (a CompactionLevels) NextLevel(level int) int {
	if level == SnapshotLevel {
		return -1
	} else if level == a.MaxLevel() {
		return SnapshotLevel
	}
	return level + 1
}
