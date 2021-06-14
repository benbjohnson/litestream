package internal_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/benbjohnson/litestream/internal"
)

func TestParseSnapshotPath(t *testing.T) {
	for _, tt := range []struct {
		s     string
		index int
		err   error
	}{
		{"00bc614e.snapshot.lz4", 12345678, nil},
		{"xxxxxxxx.snapshot.lz4", 0, fmt.Errorf("invalid snapshot path")},
		{"00bc614.snapshot.lz4", 0, fmt.Errorf("invalid snapshot path")},
		{"00bc614e.snapshot.lz", 0, fmt.Errorf("invalid snapshot path")},
		{"00bc614e.snapshot", 0, fmt.Errorf("invalid snapshot path")},
		{"00bc614e", 0, fmt.Errorf("invalid snapshot path")},
		{"", 0, fmt.Errorf("invalid snapshot path")},
	} {
		t.Run("", func(t *testing.T) {
			index, err := internal.ParseSnapshotPath(tt.s)
			if got, want := index, tt.index; got != want {
				t.Errorf("index=%#v, want %#v", got, want)
			} else if got, want := err, tt.err; !reflect.DeepEqual(got, want) {
				t.Errorf("err=%#v, want %#v", got, want)
			}
		})
	}
}

func TestParseWALSegmentPath(t *testing.T) {
	for _, tt := range []struct {
		s      string
		index  int
		offset int64
		err    error
	}{
		{"00bc614e/000003e8.wal.lz4", 12345678, 1000, nil},
		{"00000000/00000000.wal", 0, 0, fmt.Errorf("invalid wal segment path")},
		{"00000000/00000000", 0, 0, fmt.Errorf("invalid wal segment path")},
		{"00000000/", 0, 0, fmt.Errorf("invalid wal segment path")},
		{"00000000", 0, 0, fmt.Errorf("invalid wal segment path")},
		{"", 0, 0, fmt.Errorf("invalid wal segment path")},
	} {
		t.Run("", func(t *testing.T) {
			index, offset, err := internal.ParseWALSegmentPath(tt.s)
			if got, want := index, tt.index; got != want {
				t.Errorf("index=%#v, want %#v", got, want)
			} else if got, want := offset, tt.offset; got != want {
				t.Errorf("offset=%#v, want %#v", got, want)
			} else if got, want := err, tt.err; !reflect.DeepEqual(got, want) {
				t.Errorf("err=%#v, want %#v", got, want)
			}
		})
	}
}
