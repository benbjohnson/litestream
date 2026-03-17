//go:build integration && soak

package integration

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// LTXEvent represents a parsed log event related to LTX operations.
type LTXEvent struct {
	Time      time.Time
	Type      string // "sync", "snapshot", "compaction", "checkpoint"
	Level     int    // compaction level (-1 if N/A)
	MinTXID   string
	MaxTXID   string
	Size      int64
	IsSnap    bool   // true if sync created a snapshot-sized LTX
	Reason    string // snapshot reason from log
	PageCount int    // number of pages (estimated from size)
}

// LTXBehaviorReport holds all behavioral metrics from a test run.
type LTXBehaviorReport struct {
	Duration time.Duration
	Events   []LTXEvent

	// Snapshot metrics
	SnapshotCount     int
	SnapshotTimes     []time.Time
	SnapshotIntervals []time.Duration

	// Compaction metrics by level
	CompactionCounts    map[int]int
	CompactionTimes     map[int][]time.Time
	CompactionIntervals map[int][]time.Duration

	// Checkpoint metrics
	CheckpointCount int
	CheckpointTimes []time.Time

	// Sync metrics
	SyncCount       int
	SnapSyncCount   int // syncs that triggered snapshot-sized LTX
	SnapSyncReasons []string

	// L0 file metrics
	L0Sizes []int64
}

// ParseLTXEvents parses a Litestream log file and extracts LTX-related events.
func ParseLTXEvents(logPath string) ([]LTXEvent, error) {
	file, err := os.Open(logPath)
	if err != nil {
		return nil, fmt.Errorf("open log: %w", err)
	}
	defer file.Close()

	var events []LTXEvent
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()

		if ev, ok := parseSnapshotComplete(line); ok {
			events = append(events, ev)
		} else if ev, ok := parseCompactionComplete(line); ok {
			events = append(events, ev)
		} else if ev, ok := parseCheckpoint(line); ok {
			events = append(events, ev)
		} else if ev, ok := parseSyncWithSnap(line); ok {
			events = append(events, ev)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan log: %w", err)
	}

	return events, nil
}

// BuildBehaviorReport builds a full behavioral report from parsed events.
func BuildBehaviorReport(events []LTXEvent, duration time.Duration) *LTXBehaviorReport {
	report := &LTXBehaviorReport{
		Duration:            duration,
		Events:              events,
		CompactionCounts:    make(map[int]int),
		CompactionTimes:     make(map[int][]time.Time),
		CompactionIntervals: make(map[int][]time.Duration),
	}

	for _, ev := range events {
		switch ev.Type {
		case "snapshot":
			report.SnapshotCount++
			report.SnapshotTimes = append(report.SnapshotTimes, ev.Time)
		case "compaction":
			report.CompactionCounts[ev.Level]++
			report.CompactionTimes[ev.Level] = append(report.CompactionTimes[ev.Level], ev.Time)
		case "checkpoint":
			report.CheckpointCount++
			report.CheckpointTimes = append(report.CheckpointTimes, ev.Time)
		case "sync":
			report.SyncCount++
			if ev.IsSnap {
				report.SnapSyncCount++
				report.SnapSyncReasons = append(report.SnapSyncReasons, ev.Reason)
			}
			if ev.Size > 0 {
				report.L0Sizes = append(report.L0Sizes, ev.Size)
			}
		}
	}

	// Calculate snapshot intervals
	sort.Slice(report.SnapshotTimes, func(i, j int) bool {
		return report.SnapshotTimes[i].Before(report.SnapshotTimes[j])
	})
	for i := 1; i < len(report.SnapshotTimes); i++ {
		report.SnapshotIntervals = append(report.SnapshotIntervals,
			report.SnapshotTimes[i].Sub(report.SnapshotTimes[i-1]))
	}

	// Calculate compaction intervals per level
	for level, times := range report.CompactionTimes {
		sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
		for i := 1; i < len(times); i++ {
			report.CompactionIntervals[level] = append(report.CompactionIntervals[level],
				times[i].Sub(times[i-1]))
		}
	}

	return report
}

// AssertSnapshotCadence checks that snapshots occur approximately at the expected interval.
// It fails if snapshots are too frequent (less than minInterval) which indicates the
// "snapshot on checkpoint" bug or similar regression.
//
// The first snapshot interval is skipped because the initial snapshot always occurs
// quickly when Litestream starts (it captures the initial database state).
func AssertSnapshotCadence(t *testing.T, report *LTXBehaviorReport, expectedInterval, tolerance time.Duration) {
	t.Helper()

	if report.SnapshotCount <= 1 {
		t.Logf("  [snapshot-cadence] Only %d snapshot(s) recorded, skipping cadence check", report.SnapshotCount)
		return
	}

	minAllowed := expectedInterval - tolerance
	if minAllowed < 0 {
		minAllowed = 0
	}

	// Skip the first interval — the initial snapshot pair (startup + first scheduled)
	// often has a short interval that doesn't represent steady-state behavior.
	intervals := report.SnapshotIntervals
	if len(intervals) > 1 {
		intervals = intervals[1:]
	}

	violations := 0
	for i, interval := range intervals {
		if interval < minAllowed {
			t.Errorf("  [snapshot-cadence] Snapshot interval #%d: %v (min allowed: %v)",
				i+2, interval.Round(time.Second), minAllowed.Round(time.Second))
			violations++
		}
	}

	if violations == 0 {
		t.Logf("  [snapshot-cadence] PASS: %d snapshots, steady-state intervals >= %v (skipped first interval)",
			report.SnapshotCount, minAllowed.Round(time.Second))
	} else {
		t.Errorf("  [snapshot-cadence] FAIL: %d/%d steady-state snapshot intervals violated minimum cadence",
			violations, len(intervals))
	}
}

// AssertNoExcessiveSnapshots checks that the total number of snapshots doesn't exceed
// what's expected for the test duration and configured interval.
func AssertNoExcessiveSnapshots(t *testing.T, report *LTXBehaviorReport, expectedInterval time.Duration) {
	t.Helper()

	// Allow 2x the expected count as tolerance (timing jitter, startup snapshot, etc.)
	expectedCount := int(report.Duration/expectedInterval) + 1
	maxAllowed := expectedCount * 2

	if report.SnapshotCount > maxAllowed {
		t.Errorf("  [excessive-snapshots] FAIL: %d snapshots in %v (expected ~%d, max allowed %d)",
			report.SnapshotCount, report.Duration.Round(time.Second), expectedCount, maxAllowed)
	} else {
		t.Logf("  [excessive-snapshots] PASS: %d snapshots in %v (expected ~%d)",
			report.SnapshotCount, report.Duration.Round(time.Second), expectedCount)
	}
}

// AssertL0PageCount checks that L0 LTX files are approximately the right size.
// Under normal incremental syncs with moderate writes, each L0 should contain
// only a handful of pages, not a full database snapshot worth.
//
// It reads file sizes directly from the replica L0 directory since sync events
// are logged at DEBUG level and may not be present in the log.
//
// NOTE: This only inspects files surviving at test end. Oversized L0 files
// created earlier may have been compacted away. A future improvement could
// sample L0 sizes periodically during the run.
func AssertL0PageCount(t *testing.T, report *LTXBehaviorReport, pageSize int, maxPagesPerL0 int, replicaPath string) {
	t.Helper()

	// Read L0 file sizes directly from replica directory
	l0Dir := filepath.Join(replicaPath, "ltx", "0")
	l0Files, err := filepath.Glob(filepath.Join(l0Dir, "*.ltx"))
	if err != nil || len(l0Files) == 0 {
		t.Logf("  [l0-page-count] No L0 files found in %s, skipping check", l0Dir)
		return
	}

	var sizes []int64
	for _, f := range l0Files {
		info, err := os.Stat(f)
		if err != nil {
			continue
		}
		sizes = append(sizes, info.Size())
	}

	if len(sizes) == 0 {
		t.Log("  [l0-page-count] No L0 file sizes available, skipping check")
		return
	}

	maxSizeBytes := int64(maxPagesPerL0 * pageSize)
	oversized := 0
	var maxSeen int64

	for _, size := range sizes {
		if size > maxSeen {
			maxSeen = size
		}
		if size > maxSizeBytes {
			oversized++
		}
	}

	// Allow a percentage of oversized files. Occasional full-DB snapshots are
	// expected from gap recovery (checkpointGapSnapshot) and scheduled L9
	// snapshots that land in L0. The original bug showed >50% oversized, so
	// 15% catches systematic issues while allowing legitimate recovery snapshots.
	maxOversizedPct := 0.15 // 15%
	oversizedPct := float64(oversized) / float64(len(sizes))

	if oversizedPct > maxOversizedPct {
		t.Errorf("  [l0-page-count] FAIL: %d/%d (%.1f%%) L0 files exceed %d pages (%s). Max seen: %s",
			oversized, len(sizes), oversizedPct*100,
			maxPagesPerL0, formatBytes(maxSizeBytes), formatBytes(maxSeen))
	} else {
		avgSize := avgInt64(sizes)
		t.Logf("  [l0-page-count] PASS: %d L0 files, avg size %s, max %s (%d/%d oversized, threshold %d pages)",
			len(sizes), formatBytes(avgSize), formatBytes(maxSeen), oversized, len(sizes), maxPagesPerL0)
	}
}

// AssertCompactionTiming checks that compactions at each level occur approximately
// within their configured intervals.
func AssertCompactionTiming(t *testing.T, report *LTXBehaviorReport, levelIntervals map[int]time.Duration, tolerance float64) {
	t.Helper()

	for level, expectedInterval := range levelIntervals {
		intervals, ok := report.CompactionIntervals[level]
		if !ok || len(intervals) < 3 {
			count := report.CompactionCounts[level]
			t.Logf("  [compaction-timing-L%d] Skipped: only %d compactions (need >=4 for reliable interval check)", level, count)
			continue
		}

		minAllowed := time.Duration(float64(expectedInterval) * (1 - tolerance))
		maxAllowed := time.Duration(float64(expectedInterval) * (1 + tolerance))

		violations := 0
		for _, interval := range intervals {
			if interval < minAllowed || interval > maxAllowed {
				violations++
			}
		}

		violationPct := float64(violations) / float64(len(intervals))
		if violationPct > 0.25 { // allow 25% outliers due to timing jitter
			t.Errorf("  [compaction-timing-L%d] FAIL: %d/%d intervals outside [%v, %v] (expected ~%v)",
				level, violations, len(intervals),
				minAllowed.Round(time.Second), maxAllowed.Round(time.Second),
				expectedInterval.Round(time.Second))
		} else {
			avg := avgDuration(intervals)
			t.Logf("  [compaction-timing-L%d] PASS: %d compactions, avg interval %v (expected ~%v)",
				level, report.CompactionCounts[level], avg.Round(time.Second), expectedInterval.Round(time.Second))
		}
	}
}

// AssertWALBounded checks that the WAL file stayed bounded during the test.
// It checks both the current WAL size and the peak WAL size observed during monitoring.
// peakWALSize is the maximum WAL size in bytes sampled during the test run.
func AssertWALBounded(t *testing.T, walPath string, maxWALSizeMB float64, peakWALSize int64) {
	t.Helper()

	var currentSizeMB float64
	info, err := os.Stat(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			currentSizeMB = 0
		} else {
			t.Logf("  [wal-bounded] Skipped: cannot stat WAL: %v", err)
			return
		}
	} else {
		currentSizeMB = float64(info.Size()) / (1024 * 1024)
	}

	peakSizeMB := float64(peakWALSize) / (1024 * 1024)

	// Use whichever is larger: current or peak
	checkSizeMB := currentSizeMB
	label := "current"
	if peakSizeMB > currentSizeMB {
		checkSizeMB = peakSizeMB
		label = "peak"
	}

	if checkSizeMB > maxWALSizeMB {
		t.Errorf("  [wal-bounded] FAIL: WAL %s is %.2f MB (max allowed: %.2f MB, current: %.2f MB, peak: %.2f MB)",
			label, checkSizeMB, maxWALSizeMB, currentSizeMB, peakSizeMB)
	} else {
		t.Logf("  [wal-bounded] PASS: WAL current %.2f MB, peak %.2f MB (max allowed: %.2f MB)",
			currentSizeMB, peakSizeMB, maxWALSizeMB)
	}
}

// AssertNoSnapshotOnCheckpoint detects the specific bug where a checkpoint triggers
// an unwanted full snapshot. It looks for snapshot syncs that occur within a short
// window after a checkpoint.
func AssertNoSnapshotOnCheckpoint(t *testing.T, report *LTXBehaviorReport) {
	t.Helper()

	if len(report.CheckpointTimes) == 0 || report.SnapSyncCount == 0 {
		t.Logf("  [no-snap-on-checkpoint] PASS: no checkpoint+snapshot overlap to check (checkpoints=%d, snap-syncs=%d)",
			len(report.CheckpointTimes), report.SnapSyncCount)
		return
	}

	// Build timeline: check if any snap-sync events coincide with checkpoint events
	snapSyncEvents := make([]LTXEvent, 0)
	for _, ev := range report.Events {
		if ev.Type == "sync" && ev.IsSnap {
			snapSyncEvents = append(snapSyncEvents, ev)
		}
	}

	violations := 0
	expectedRecoveries := 0
	checkpointWindow := 5 * time.Second

	for _, chkTime := range report.CheckpointTimes {
		for _, snapEv := range snapSyncEvents {
			diff := snapEv.Time.Sub(chkTime)
			if diff >= 0 && diff <= checkpointWindow {
				if isExpectedRecoverySnapshot(snapEv.Reason) {
					expectedRecoveries++
					continue
				}
				t.Errorf("  [no-snap-on-checkpoint] Snapshot sync at %v occurred %v after checkpoint at %v (reason: %s)",
					snapEv.Time.Format("15:04:05"), diff.Round(time.Millisecond),
					chkTime.Format("15:04:05"), snapEv.Reason)
				violations++
			}
		}
	}

	if violations == 0 {
		msg := fmt.Sprintf("no snapshot-on-checkpoint detected (%d checkpoints, %d snap-syncs checked",
			len(report.CheckpointTimes), len(snapSyncEvents))
		if expectedRecoveries > 0 {
			msg += fmt.Sprintf(", %d expected gap recoveries", expectedRecoveries)
		}
		t.Logf("  [no-snap-on-checkpoint] PASS: %s)", msg)
	} else {
		t.Errorf("  [no-snap-on-checkpoint] FAIL: %d snapshot-on-checkpoint violations detected", violations)
	}
}

// isExpectedRecoverySnapshot returns true if the snapshot reason indicates an
// intentional recovery mechanism (gap healing, compaction repair) rather than
// the checkpoint-triggers-unwanted-snapshot bug.
func isExpectedRecoverySnapshot(reason string) bool {
	return strings.Contains(reason, "checkpoint gap recovery") ||
		strings.Contains(reason, "repair snapshot") ||
		strings.Contains(reason, "compaction detected missing") ||
		strings.Contains(reason, "L0 file corrupted")
}

// PrintBehaviorReport prints a human-readable summary of the behavioral report.
func PrintBehaviorReport(t *testing.T, report *LTXBehaviorReport) {
	t.Helper()

	t.Log("")
	t.Log("================================================")
	t.Log("LTX Behavioral Report")
	t.Log("================================================")
	t.Log("")
	t.Logf("  Duration: %v", report.Duration.Round(time.Second))
	t.Logf("  Total syncs: %d", report.SyncCount)
	t.Logf("  Snapshot syncs: %d (%.1f%%)", report.SnapSyncCount, safePct(report.SnapSyncCount, report.SyncCount))
	t.Logf("  Scheduled snapshots (L9): %d", report.SnapshotCount)
	t.Logf("  Checkpoints: %d", report.CheckpointCount)
	t.Log("")

	// Compaction breakdown
	t.Log("  Compactions by level:")
	levels := sortedKeys(report.CompactionCounts)
	for _, level := range levels {
		count := report.CompactionCounts[level]
		t.Logf("    L%d: %d compactions", level, count)
		if intervals, ok := report.CompactionIntervals[level]; ok && len(intervals) > 0 {
			avg := avgDuration(intervals)
			t.Logf("         avg interval: %v", avg.Round(time.Second))
		}
	}
	t.Log("")

	// L0 file sizes
	if len(report.L0Sizes) > 0 {
		t.Log("  L0 file sizes:")
		t.Logf("    Count: %d", len(report.L0Sizes))
		t.Logf("    Avg: %s", formatBytes(avgInt64(report.L0Sizes)))
		t.Logf("    Min: %s", formatBytes(minInt64(report.L0Sizes)))
		t.Logf("    Max: %s", formatBytes(maxInt64(report.L0Sizes)))
		t.Logf("    Median: %s", formatBytes(medianInt64(report.L0Sizes)))
		t.Log("")
	}

	// Snapshot intervals
	if len(report.SnapshotIntervals) > 0 {
		t.Log("  Snapshot intervals:")
		for i, interval := range report.SnapshotIntervals {
			t.Logf("    #%d: %v", i+1, interval.Round(time.Second))
		}
		t.Log("")
	}

	// Snapshot sync reasons
	if len(report.SnapSyncReasons) > 0 {
		reasonCounts := make(map[string]int)
		for _, reason := range report.SnapSyncReasons {
			reasonCounts[reason]++
		}
		t.Log("  Snapshot sync reasons:")
		for reason, count := range reasonCounts {
			if reason == "" {
				reason = "(no reason logged)"
			}
			t.Logf("    %s: %d", reason, count)
		}
		t.Log("")
	}
}

// --- Log parsing helpers ---

// timeRegexp matches slog text format: time=2026-03-13T00:01:22.611Z
// Also matches standalone ISO timestamps and older log formats.
var timeRegexp = regexp.MustCompile(`(?:time=)?(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2}))|\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}`)

func parseLogTime(line string) (time.Time, bool) {
	// Try slog text format first: time=2026-03-13T00:01:22.611Z
	if idx := strings.Index(line, "time="); idx != -1 {
		rest := line[idx+5:]
		end := strings.IndexAny(rest, " \t\n")
		if end == -1 {
			end = len(rest)
		}
		ts := rest[:end]
		for _, layout := range []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02T15:04:05.000Z",
			"2006-01-02T15:04:05.000000Z",
		} {
			if t, err := time.Parse(layout, ts); err == nil {
				return t, true
			}
		}
	}

	// Fallback: timestamp at start of line
	m := timeRegexp.FindStringSubmatch(line)
	if len(m) > 1 && m[1] != "" {
		for _, layout := range []string{
			time.RFC3339Nano,
			time.RFC3339,
		} {
			if t, err := time.Parse(layout, m[1]); err == nil {
				return t, true
			}
		}
	}

	return time.Time{}, false
}

func parseSnapshotComplete(line string) (LTXEvent, bool) {
	if !strings.Contains(line, "snapshot complete") {
		return LTXEvent{}, false
	}

	t, _ := parseLogTime(line)
	ev := LTXEvent{
		Time:  t,
		Type:  "snapshot",
		Level: 9,
	}

	if v := extractField(line, "txid="); v != "" {
		ev.MaxTXID = v
	}
	if v := extractField(line, "size="); v != "" {
		ev.Size, _ = strconv.ParseInt(v, 10, 64)
	}

	return ev, true
}

func parseCompactionComplete(line string) (LTXEvent, bool) {
	if !strings.Contains(line, "compaction complete") {
		return LTXEvent{}, false
	}

	t, _ := parseLogTime(line)
	ev := LTXEvent{
		Time: t,
		Type: "compaction",
	}

	// Extract compaction level — skip past msg= to avoid matching log level=INFO
	msgIdx := strings.Index(line, "compaction complete")
	if msgIdx != -1 {
		rest := line[msgIdx:]
		if v := extractField(rest, "level="); v != "" {
			ev.Level, _ = strconv.Atoi(v)
		}
	}
	if v := extractField(line, "txid.min="); v != "" {
		ev.MinTXID = v
	}
	if v := extractField(line, "txid.max="); v != "" {
		ev.MaxTXID = v
	}
	if v := extractField(line, "size="); v != "" {
		ev.Size, _ = strconv.ParseInt(v, 10, 64)
	}

	return ev, true
}

func parseCheckpoint(line string) (LTXEvent, bool) {
	// Match checkpoint log lines: msg=checkpoint mode=PASSIVE/TRUNCATE
	// Also match: msg="checkpoint" mode=...
	// Exclude compaction/snapshot lines that happen to contain "checkpoint"
	if strings.Contains(line, "compaction") || strings.Contains(line, "snapshot") {
		return LTXEvent{}, false
	}

	isCheckpoint := (strings.Contains(line, "msg=checkpoint") || strings.Contains(line, `msg="checkpoint"`)) &&
		strings.Contains(line, "mode=")
	if !isCheckpoint {
		return LTXEvent{}, false
	}

	t, _ := parseLogTime(line)
	return LTXEvent{
		Time:  t,
		Type:  "checkpoint",
		Level: -1,
	}, true
}

func parseSyncWithSnap(line string) (LTXEvent, bool) {
	// Match slog text format: msg=sync or msg="sync", or JSON: "msg":"sync"
	isSyncMsg := strings.Contains(line, "msg=sync") ||
		strings.Contains(line, `msg="sync"`) ||
		strings.Contains(line, `"msg":"sync"`)
	if !isSyncMsg {
		return LTXEvent{}, false
	}

	t, _ := parseLogTime(line)
	ev := LTXEvent{
		Time:  t,
		Type:  "sync",
		Level: 0,
	}

	ev.IsSnap = strings.Contains(line, "snap=true")
	if ev.IsSnap {
		ev.Reason = extractField(line, "reason=")
	}

	return ev, true
}

func extractField(line, prefix string) string {
	idx := strings.Index(line, prefix)
	if idx == -1 {
		return ""
	}
	rest := line[idx+len(prefix):]

	// Handle quoted values
	if len(rest) > 0 && rest[0] == '"' {
		end := strings.Index(rest[1:], "\"")
		if end != -1 {
			return rest[1 : end+1]
		}
	}

	// Handle unquoted values (space-delimited)
	end := strings.IndexAny(rest, " \t\n}")
	if end == -1 {
		return strings.TrimSpace(rest)
	}
	return rest[:end]
}

// --- Utility helpers ---

func formatBytes(b int64) string {
	switch {
	case b >= 1024*1024:
		return fmt.Sprintf("%.1f MB", float64(b)/(1024*1024))
	case b >= 1024:
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func avgInt64(vals []int64) int64 {
	if len(vals) == 0 {
		return 0
	}
	var sum int64
	for _, v := range vals {
		sum += v
	}
	return sum / int64(len(vals))
}

func minInt64(vals []int64) int64 {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

func maxInt64(vals []int64) int64 {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

func medianInt64(vals []int64) int64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := make([]int64, len(vals))
	copy(sorted, vals)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[len(sorted)/2]
}

func avgDuration(vals []time.Duration) time.Duration {
	if len(vals) == 0 {
		return 0
	}
	var sum time.Duration
	for _, v := range vals {
		sum += v
	}
	return sum / time.Duration(len(vals))
}

func safePct(num, denom int) float64 {
	if denom == 0 {
		return 0
	}
	return float64(num) / float64(denom) * 100
}

func sortedKeys(m map[int]int) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}

// criticalErrors is a package-level variable used by soak tests to track error count.
// It's referenced from comprehensive_soak_test.go and minio_soak_test.go.
var criticalErrors int
