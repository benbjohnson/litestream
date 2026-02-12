package litestream_test

import (
	"testing"

	"github.com/benbjohnson/litestream"
)

func TestPosV3_IsZero(t *testing.T) {
	if !(litestream.PosV3{}).IsZero() {
		t.Error("zero value should return true")
	}
	if (litestream.PosV3{Generation: "abc"}).IsZero() {
		t.Error("non-zero value should return false")
	}
}

func TestPosV3_String(t *testing.T) {
	tests := []struct {
		pos  litestream.PosV3
		want string
	}{
		{litestream.PosV3{}, ""},
		{litestream.PosV3{Generation: "0123456789abcdef", Index: 1, Offset: 4096}, "0123456789abcdef/00000001:0000000000001000"},
	}
	for _, tt := range tests {
		if got := tt.pos.String(); got != tt.want {
			t.Errorf("PosV3%+v.String() = %q, want %q", tt.pos, got, tt.want)
		}
	}
}

func TestSnapshotInfoV3_Pos(t *testing.T) {
	info := litestream.SnapshotInfoV3{Generation: "abc", Index: 5}
	pos := info.Pos()
	if pos.Generation != "abc" || pos.Index != 5 || pos.Offset != 0 {
		t.Errorf("unexpected pos: %+v", pos)
	}
}

func TestWALSegmentInfoV3_Pos(t *testing.T) {
	info := litestream.WALSegmentInfoV3{Generation: "abc", Index: 5, Offset: 100}
	pos := info.Pos()
	if pos.Generation != "abc" || pos.Index != 5 || pos.Offset != 100 {
		t.Errorf("unexpected pos: %+v", pos)
	}
}

func TestFormatSnapshotFilenameV3(t *testing.T) {
	tests := []struct {
		index int
		want  string
	}{
		{0, "00000000.snapshot.lz4"},
		{1, "00000001.snapshot.lz4"},
		{255, "000000ff.snapshot.lz4"},
		{0x12345678, "12345678.snapshot.lz4"},
	}
	for _, tt := range tests {
		if got := litestream.FormatSnapshotFilenameV3(tt.index); got != tt.want {
			t.Errorf("FormatSnapshotFilenameV3(%d) = %q, want %q", tt.index, got, tt.want)
		}
	}
}

func TestParseSnapshotFilenameV3(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		tests := []struct {
			filename string
			want     int
		}{
			{"00000000.snapshot.lz4", 0},
			{"00000001.snapshot.lz4", 1},
			{"000000ff.snapshot.lz4", 255},
			{"12345678.snapshot.lz4", 0x12345678},
		}
		for _, tt := range tests {
			index, err := litestream.ParseSnapshotFilenameV3(tt.filename)
			if err != nil {
				t.Errorf("ParseSnapshotFilenameV3(%q) error: %v", tt.filename, err)
				continue
			}
			if index != tt.want {
				t.Errorf("ParseSnapshotFilenameV3(%q) = %d, want %d", tt.filename, index, tt.want)
			}
		}
	})

	t.Run("Invalid", func(t *testing.T) {
		invalids := []string{
			"",
			"invalid.txt",
			"00000001.snapshot",      // missing .lz4
			"0000001.snapshot.lz4",   // 7 chars, not 8
			"000000001.snapshot.lz4", // 9 chars, not 8
			"0000000g.snapshot.lz4",  // invalid hex
			"00000001.SNAPSHOT.lz4",  // uppercase
			"00000001.snapshot.lz4.bak",
		}
		for _, filename := range invalids {
			_, err := litestream.ParseSnapshotFilenameV3(filename)
			if err == nil {
				t.Errorf("ParseSnapshotFilenameV3(%q) expected error", filename)
			}
		}
	})
}

func TestFormatWALSegmentFilenameV3(t *testing.T) {
	tests := []struct {
		index  int
		offset int64
		want   string
	}{
		{0, 0, "00000000_00000000.wal.lz4"},
		{1, 4096, "00000001_00001000.wal.lz4"},
		{255, 0x12345678, "000000ff_12345678.wal.lz4"},
	}
	for _, tt := range tests {
		if got := litestream.FormatWALSegmentFilenameV3(tt.index, tt.offset); got != tt.want {
			t.Errorf("FormatWALSegmentFilenameV3(%d, %d) = %q, want %q", tt.index, tt.offset, got, tt.want)
		}
	}
}

func TestParseWALSegmentFilenameV3(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		tests := []struct {
			filename   string
			wantIndex  int
			wantOffset int64
		}{
			{"00000000_00000000.wal.lz4", 0, 0},
			{"00000001_00001000.wal.lz4", 1, 4096},
			{"000000ff_12345678.wal.lz4", 255, 0x12345678},
		}
		for _, tt := range tests {
			index, offset, err := litestream.ParseWALSegmentFilenameV3(tt.filename)
			if err != nil {
				t.Errorf("ParseWALSegmentFilenameV3(%q) error: %v", tt.filename, err)
				continue
			}
			if index != tt.wantIndex || offset != tt.wantOffset {
				t.Errorf("ParseWALSegmentFilenameV3(%q) = (%d, %d), want (%d, %d)",
					tt.filename, index, offset, tt.wantIndex, tt.wantOffset)
			}
		}
	})

	t.Run("Invalid", func(t *testing.T) {
		invalids := []string{
			"",
			"invalid.wal",
			"00000001.wal.lz4",         // missing offset
			"00000001_00001000.wal",    // missing .lz4
			"0000001_00001000.wal.lz4", // 7 chars index
			"00000001_0001000.wal.lz4", // 7 chars offset
		}
		for _, filename := range invalids {
			_, _, err := litestream.ParseWALSegmentFilenameV3(filename)
			if err == nil {
				t.Errorf("ParseWALSegmentFilenameV3(%q) expected error", filename)
			}
		}
	})
}

func TestIsGenerationIDV3(t *testing.T) {
	tests := []struct {
		s    string
		want bool
	}{
		{"0123456789abcdef", true},
		{"abcdef0123456789", true},
		{"aaaaaaaaaaaaaaaa", true},
		{"0000000000000000", true},
		{"ffffffffffffffff", true},
		{"0123456789ABCDEF", false},  // uppercase not valid
		{"0123456789abcde", false},   // 15 chars, too short
		{"0123456789abcdeff", false}, // 17 chars, too long
		{"0123456789abcdeg", false},  // invalid hex char
		{"", false},
		{"generations", false},
	}
	for _, tt := range tests {
		if got := litestream.IsGenerationIDV3(tt.s); got != tt.want {
			t.Errorf("IsGenerationIDV3(%q) = %v, want %v", tt.s, got, tt.want)
		}
	}
}

func TestPathsV3(t *testing.T) {
	root := "/data/replica"
	gen := "0123456789abcdef"

	t.Run("GenerationsPath", func(t *testing.T) {
		got := litestream.GenerationsPathV3(root)
		want := "/data/replica/generations"
		if got != want {
			t.Errorf("GenerationsPathV3(%q) = %q, want %q", root, got, want)
		}
	})

	t.Run("GenerationPath", func(t *testing.T) {
		got := litestream.GenerationPathV3(root, gen)
		want := "/data/replica/generations/0123456789abcdef"
		if got != want {
			t.Errorf("GenerationPathV3(%q, %q) = %q, want %q", root, gen, got, want)
		}
	})

	t.Run("SnapshotsPath", func(t *testing.T) {
		got := litestream.SnapshotsPathV3(root, gen)
		want := "/data/replica/generations/0123456789abcdef/snapshots"
		if got != want {
			t.Errorf("SnapshotsPathV3(%q, %q) = %q, want %q", root, gen, got, want)
		}
	})

	t.Run("WALPath", func(t *testing.T) {
		got := litestream.WALPathV3(root, gen)
		want := "/data/replica/generations/0123456789abcdef/wal"
		if got != want {
			t.Errorf("WALPathV3(%q, %q) = %q, want %q", root, gen, got, want)
		}
	})

	t.Run("SnapshotPath", func(t *testing.T) {
		got := litestream.SnapshotPathV3(root, gen, 1)
		want := "/data/replica/generations/0123456789abcdef/snapshots/00000001.snapshot.lz4"
		if got != want {
			t.Errorf("SnapshotPathV3(%q, %q, 1) = %q, want %q", root, gen, got, want)
		}
	})

	t.Run("WALSegmentPath", func(t *testing.T) {
		got := litestream.WALSegmentPathV3(root, gen, 1, 4096)
		want := "/data/replica/generations/0123456789abcdef/wal/00000001_00001000.wal.lz4"
		if got != want {
			t.Errorf("WALSegmentPathV3(%q, %q, 1, 4096) = %q, want %q", root, gen, got, want)
		}
	})
}

// TestFormatParseRoundtrip verifies that Format and Parse are inverses.
func TestFormatParseRoundtrip(t *testing.T) {
	t.Run("Snapshot", func(t *testing.T) {
		for _, index := range []int{0, 1, 255, 0x12345678} {
			filename := litestream.FormatSnapshotFilenameV3(index)
			got, err := litestream.ParseSnapshotFilenameV3(filename)
			if err != nil {
				t.Errorf("roundtrip failed for index %d: %v", index, err)
				continue
			}
			if got != index {
				t.Errorf("roundtrip: FormatSnapshotFilenameV3(%d) -> %q -> ParseSnapshotFilenameV3 -> %d", index, filename, got)
			}
		}
	})

	t.Run("WALSegment", func(t *testing.T) {
		cases := []struct {
			index  int
			offset int64
		}{
			{0, 0},
			{1, 4096},
			{255, 0x12345678},
		}
		for _, tc := range cases {
			filename := litestream.FormatWALSegmentFilenameV3(tc.index, tc.offset)
			gotIndex, gotOffset, err := litestream.ParseWALSegmentFilenameV3(filename)
			if err != nil {
				t.Errorf("roundtrip failed for (%d, %d): %v", tc.index, tc.offset, err)
				continue
			}
			if gotIndex != tc.index || gotOffset != tc.offset {
				t.Errorf("roundtrip: FormatWALSegmentFilenameV3(%d, %d) -> %q -> (%d, %d)",
					tc.index, tc.offset, filename, gotIndex, gotOffset)
			}
		}
	})
}
