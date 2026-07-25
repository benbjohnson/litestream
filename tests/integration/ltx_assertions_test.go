//go:build integration && soak

package integration

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestAssertNoSnapshotOnCheckpointFailsClosed(t *testing.T) {
	if logText := os.Getenv("LITESTREAM_ASSERTION_LOG"); logText != "" {
		logPath := filepath.Join(t.TempDir(), "litestream.log")
		if err := os.WriteFile(logPath, []byte(logText), 0o600); err != nil {
			t.Fatal(err)
		}

		events, err := ParseLTXEvents(logPath)
		if err != nil {
			t.Fatal(err)
		}
		AssertNoSnapshotOnCheckpoint(t, BuildBehaviorReport(events, 0))
		return
	}

	tests := map[string]string{
		"missing checkpoint mode": `
time=2026-07-25T01:00:00Z level=DEBUG msg=checkpoint mode=TRUNCATE
time=2026-07-25T01:00:02Z level=DEBUG msg=checkpoint result=0,10,10
time=2026-07-25T01:00:03Z level=DEBUG msg=sync snap=true reason="checkpoint boundary snapshot"
`,
		"malformed checkpoint timestamp": `
time=2026-07-25T01:00:00Z level=DEBUG msg=checkpoint mode=TRUNCATE
time=invalid level=DEBUG msg=checkpoint mode=PASSIVE
time=2026-07-25T01:00:03Z level=DEBUG msg=sync snap=true reason="checkpoint boundary snapshot"
`,
		"unsupported checkpoint mode": `
time=2026-07-25T01:00:02Z level=DEBUG msg=checkpoint mode=UNKNOWN
time=2026-07-25T01:00:03Z level=DEBUG msg=sync snap=true reason="checkpoint boundary snapshot"
`,
		"no preceding checkpoint": `
time=2026-07-25T01:00:03Z level=DEBUG msg=sync snap=true reason="checkpoint boundary snapshot"
`,
		"checkpoint later in log": `
time=2026-07-25T01:00:03Z level=DEBUG msg=sync snap=true reason="checkpoint boundary snapshot"
time=2026-07-25T01:00:02Z level=DEBUG msg=checkpoint mode=TRUNCATE
`,
		"checkpoint later in log for same database": `
time=2026-07-25T01:00:03Z level=DEBUG msg=sync db=test.db snap=true reason="checkpoint boundary snapshot"
time=2026-07-25T01:00:02Z level=DEBUG msg=checkpoint db=test.db mode=TRUNCATE
`,
		"checkpoint from another database": `
time=2026-07-25T01:00:02Z level=DEBUG msg=checkpoint db=alpha.db mode=TRUNCATE
time=2026-07-25T01:00:03Z level=DEBUG msg=sync db=beta.db snap=true reason="checkpoint boundary snapshot"
`,
	}

	for name, logText := range tests {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestAssertNoSnapshotOnCheckpointFailsClosed$")
			cmd.Env = append(os.Environ(), "LITESTREAM_ASSERTION_LOG="+logText)
			if output, err := cmd.CombinedOutput(); err == nil {
				t.Fatalf("assertion passed unclassifiable input:\n%s", output)
			}
		})
	}
}

func TestAssertNoSnapshotOnCheckpointAllowsStartupAndTruncateSnapshots(t *testing.T) {
	events := []LTXEvent{
		{
			Time:   mustParseLogTime(t, "time=2026-07-25T01:00:00Z"),
			Type:   "sync",
			IsSnap: true,
		},
		{
			Time:           mustParseLogTime(t, "time=2026-07-25T01:00:02Z"),
			Database:       "test.db",
			Type:           "checkpoint",
			CheckpointMode: "TRUNCATE",
		},
		{
			Time:     mustParseLogTime(t, "time=2026-07-25T01:00:03Z"),
			Database: "test.db",
			Type:     "sync",
			IsSnap:   true,
			Reason:   "checkpoint boundary snapshot",
		},
	}

	AssertNoSnapshotOnCheckpoint(t, BuildBehaviorReport(events, 0))
}

func mustParseLogTime(t *testing.T, line string) time.Time {
	t.Helper()

	value, ok := parseLogTime(line)
	if !ok {
		t.Fatalf("parse log time: %q", line)
	}
	return value
}
