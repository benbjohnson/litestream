//go:build integration && soak

package integration

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
		"missing checkpoint database launders older truncate": `
time=2026-07-25T01:00:00Z level=DEBUG msg=checkpoint db=beta.db mode=TRUNCATE
time=2026-07-25T01:00:02Z level=DEBUG msg=checkpoint mode=PASSIVE
time=2026-07-25T01:00:03Z level=DEBUG msg=sync db=beta.db snap=true reason="checkpoint boundary snapshot"
`,
		"empty checkpoint database launders older truncate": `
time=2026-07-25T01:00:00Z level=DEBUG msg=checkpoint db=beta.db mode=TRUNCATE
time=2026-07-25T01:00:02Z level=DEBUG msg=checkpoint db="" mode=PASSIVE
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

func TestParseCompactionCompleteAndUploadLTXLevel(t *testing.T) {
	t.Run("compaction complete text with ltx_level", func(t *testing.T) {
		line := `time=2026-11-27T01:00:00Z level=INFO msg="compaction complete" system=store db=test.db ltx_level=2 txid.min=0000000000000001 txid.max=0000000000000005 size=4096`
		ev, ok := parseCompactionComplete(line)
		if !ok {
			t.Fatal("expected parseCompactionComplete to match")
		}
		if ev.Level != 2 {
			t.Fatalf("got level %d, want 2", ev.Level)
		}
	})

	t.Run("compaction complete json with ltx_level", func(t *testing.T) {
		line := `{"time":"2026-11-27T01:00:00Z","level":"INFO","msg":"compaction complete","system":"store","db":"test.db","ltx_level":3,"size":4096}`
		ev, ok := parseCompactionComplete(line)
		if !ok {
			t.Fatal("expected parseCompactionComplete to match")
		}
		if ev.Level != 3 {
			t.Fatalf("got level %d, want 3", ev.Level)
		}
	})

	t.Run("compaction complete json with db name containing ltx_level=", func(t *testing.T) {
		line := `{"time":"2026-11-27T01:00:00Z","level":"INFO","msg":"compaction complete","system":"store","db":"copy-ltx_level=8.db","ltx_level":2,"size":4096}`
		ev, ok := parseCompactionComplete(line)
		if !ok {
			t.Fatal("expected parseCompactionComplete to match")
		}
		if ev.Level != 2 {
			t.Fatalf("got level %d, want 2", ev.Level)
		}
	})

	t.Run("compaction complete text with db name containing ltx_level=", func(t *testing.T) {
		line := `time=2026-11-27T01:00:00Z level=INFO msg="compaction complete" system=store db="copy-ltx_level=8.db" ltx_level=2 size=4096`
		ev, ok := parseCompactionComplete(line)
		if !ok {
			t.Fatal("expected parseCompactionComplete to match")
		}
		if ev.Level != 2 {
			t.Fatalf("got level %d, want 2", ev.Level)
		}
	})

	t.Run("compaction complete legacy duplicate level text and json", func(t *testing.T) {
		textLine := `time=2026-11-27T01:00:00Z level=INFO msg="compaction complete" system=store db=test.db level=4 size=4096`
		ev, ok := parseCompactionComplete(textLine)
		if !ok || ev.Level != 4 {
			t.Fatalf("text legacy parse got (%v, %d), want (true, 4)", ok, ev.Level)
		}

		jsonLine := `{"time":"2026-11-27T01:00:00Z","level":"INFO","msg":"compaction complete","system":"store","db":"test.db","level":5,"size":4096}`
		ev, ok = parseCompactionComplete(jsonLine)
		if !ok || ev.Level != 5 {
			t.Fatalf("json legacy parse got (%v, %d), want (true, 5)", ok, ev.Level)
		}
	})

	t.Run("ltx file uploaded text with ltx_level", func(t *testing.T) {
		line := `time=2026-11-27T01:00:00Z level=DEBUG msg="ltx file uploaded" db=test.db ltx_level=9 minTXID=0000000000000001 maxTXID=0000000000000005 size=8192`
		ev, ok := parseLTXFileUploaded(line)
		if !ok {
			t.Fatal("expected parseLTXFileUploaded to match")
		}
		if ev.Level != 9 {
			t.Fatalf("got level %d, want 9", ev.Level)
		}
	})

	t.Run("ltx file uploaded json with ltx_level 0 and db containing ltx_level=", func(t *testing.T) {
		line := `{"time":"2026-11-27T01:00:00Z","level":"DEBUG","msg":"ltx file uploaded","db":"copy-ltx_level=8.db","ltx_level":0,"size":8192}`
		ev, ok := parseLTXFileUploaded(line)
		if !ok {
			t.Fatal("expected parseLTXFileUploaded to match")
		}
		if ev.Level != 0 {
			t.Fatalf("got level %d, want 0", ev.Level)
		}
	})

	t.Run("ltx file uploaded json with ltx_level 1", func(t *testing.T) {
		line := `{"time":"2026-11-27T01:00:00Z","level":"DEBUG","msg":"ltx file uploaded","db":"test.db","ltx_level":1,"size":8192}`
		ev, ok := parseLTXFileUploaded(line)
		if !ok {
			t.Fatal("expected parseLTXFileUploaded to match")
		}
		if ev.Level != 1 {
			t.Fatalf("got level %d, want 1", ev.Level)
		}
	})

	t.Run("soak parseLog counts compactions for json text and legacy logs", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "soak.log")
		content := strings.Join([]string{
			`time=2026-11-27T01:00:00Z level=INFO msg="compaction complete" system=store db="copy-ltx_level=8.db" ltx_level=1 txid.min=0000000000000001 txid.max=0000000000000005 size=4096`,
			`{"time":"2026-11-27T01:00:05Z","level":"INFO","msg":"compaction complete","system":"store","db":"copy-ltx_level=8.db","ltx_level":2,"size":4096}`,
			`{"time":"2026-11-27T01:00:10Z","level":"INFO","msg":"compaction complete","system":"store","db":"test.db","level":3,"size":4096}`,
		}, "\n") + "\n"
		if err := os.WriteFile(logPath, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		analysis := &SoakTestAnalysis{
			CompactionsByLevel: make(map[int]int),
		}
		parseLog(logPath, analysis)
		if analysis.TotalCompactions != 3 {
			t.Fatalf("TotalCompactions = %d, want 3", analysis.TotalCompactions)
		}
		for _, lvl := range []int{1, 2, 3} {
			if analysis.CompactionsByLevel[lvl] != 1 {
				t.Errorf("CompactionsByLevel[%d] = %d, want 1 (all: %v)", lvl, analysis.CompactionsByLevel[lvl], analysis.CompactionsByLevel)
			}
		}
		if analysis.CompactionsByLevel[8] != 0 {
			t.Errorf("CompactionsByLevel[8] = %d, want 0", analysis.CompactionsByLevel[8])
		}
	})
}
