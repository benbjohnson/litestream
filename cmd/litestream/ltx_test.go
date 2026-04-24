package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
)

func TestLTXCommand_Run_JSONOutput(t *testing.T) {
	replicaDir := filepath.Join(t.TempDir(), "replica")
	replicaURL := "file://" + replicaDir
	r, err := NewReplicaFromConfig(&ReplicaConfig{URL: replicaURL}, nil)
	if err != nil {
		t.Fatal(err)
	}

	timestamp := time.Date(2026, 4, 24, 12, 30, 0, 0, time.UTC)
	data := createLTXCommandTestData(ltx.TXID(2), ltx.TXID(3), timestamp, []byte("payload"))
	info, err := r.Client.WriteLTXFile(context.Background(), 0, ltx.TXID(2), ltx.TXID(3), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	output := captureLTXCommandStdout(t, func() {
		cmd := &LTXCommand{}
		if err := cmd.Run(context.Background(), []string{"-json", replicaURL}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var got []LTXFileInfo
	if err := json.Unmarshal([]byte(output), &got); err != nil {
		t.Fatalf("failed to parse output: %v\n%s", err, output)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 LTX file, got %d", len(got))
	}
	if got[0].Level != 0 {
		t.Fatalf("unexpected level: %d", got[0].Level)
	}
	if got[0].MinTXID != "0000000000000002" {
		t.Fatalf("unexpected min txid: %s", got[0].MinTXID)
	}
	if got[0].MaxTXID != "0000000000000003" {
		t.Fatalf("unexpected max txid: %s", got[0].MaxTXID)
	}
	if got[0].Size != info.Size {
		t.Fatalf("unexpected size: %d", got[0].Size)
	}
	if got[0].Timestamp != timestamp.Format(time.RFC3339) {
		t.Fatalf("unexpected timestamp: %s", got[0].Timestamp)
	}
}

func TestTXIDVarParsing(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    ltx.TXID
		wantErr bool
	}{
		{
			name:  "valid hex string",
			input: "0000000000000002",
			want:  ltx.TXID(2),
		},
		{
			name:  "valid hex string with letters",
			input: "00000000000000ff",
			want:  ltx.TXID(255),
		},
		{
			name:  "uppercase hex",
			input: "00000000000000FF",
			want:  ltx.TXID(255),
		},
		{
			name:    "invalid - too short",
			input:   "ff",
			wantErr: true,
		},
		{
			name:    "invalid - too long",
			input:   "00000000000000001",
			wantErr: true,
		},
		{
			name:    "invalid - non-hex characters",
			input:   "000000000000000g",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var v txidVar
			err := v.Set(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Set(%q) = nil, want error", tt.input)
				}
				return
			}

			if err != nil {
				t.Errorf("Set(%q) error = %v, want nil", tt.input, err)
				return
			}

			if ltx.TXID(v) != tt.want {
				t.Errorf("Set(%q) = %v, want %v", tt.input, ltx.TXID(v), tt.want)
			}
		})
	}
}

func createLTXCommandTestData(minTXID, maxTXID ltx.TXID, timestamp time.Time, data []byte) []byte {
	hdr := ltx.Header{
		Version:   ltx.Version,
		PageSize:  4096,
		Commit:    1,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Timestamp: timestamp.UnixMilli(),
	}
	if minTXID == 1 {
		hdr.PreApplyChecksum = 0
	} else {
		hdr.PreApplyChecksum = ltx.ChecksumFlag
	}

	header, _ := hdr.MarshalBinary()
	return append(header, data...)
}

func captureLTXCommandStdout(t *testing.T, fn func()) string {
	t.Helper()

	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	t.Cleanup(func() {
		os.Stdout = orig
	})

	fn()

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	return string(output)
}

func TestTXIDVarString(t *testing.T) {
	tests := []struct {
		name  string
		value ltx.TXID
		want  string
	}{
		{
			name:  "zero",
			value: ltx.TXID(0),
			want:  "0000000000000000",
		},
		{
			name:  "small number",
			value: ltx.TXID(2),
			want:  "0000000000000002",
		},
		{
			name:  "larger number",
			value: ltx.TXID(255),
			want:  "00000000000000ff",
		},
		{
			name:  "max value",
			value: ltx.TXID(^uint64(0)),
			want:  "ffffffffffffffff",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := txidVar(tt.value)
			if got := v.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLevelVarParsing(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int
		wantErr bool
	}{
		{
			name:  "level 0",
			input: "0",
			want:  0,
		},
		{
			name:  "level 5",
			input: "5",
			want:  5,
		},
		{
			name:  "level 9 (snapshot level)",
			input: "9",
			want:  litestream.SnapshotLevel,
		},
		{
			name:  "all levels",
			input: "all",
			want:  levelAll,
		},
		{
			name:    "invalid - negative",
			input:   "-1",
			wantErr: true,
		},
		{
			name:    "invalid - too high",
			input:   "10",
			wantErr: true,
		},
		{
			name:    "invalid - non-numeric",
			input:   "abc",
			wantErr: true,
		},
		{
			name:    "invalid - empty",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var v levelVar
			err := v.Set(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Set(%q) = nil, want error", tt.input)
				}
				return
			}

			if err != nil {
				t.Errorf("Set(%q) error = %v, want nil", tt.input, err)
				return
			}

			if int(v) != tt.want {
				t.Errorf("Set(%q) = %v, want %v", tt.input, int(v), tt.want)
			}
		})
	}
}

func TestLevelVarString(t *testing.T) {
	tests := []struct {
		name  string
		value int
		want  string
	}{
		{
			name:  "level 0",
			value: 0,
			want:  "0",
		},
		{
			name:  "level 9",
			value: 9,
			want:  "9",
		},
		{
			name:  "all levels",
			value: levelAll,
			want:  "all",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := levelVar(tt.value)
			if got := v.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}
