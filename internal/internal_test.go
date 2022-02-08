package internal_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/benbjohnson/litestream/internal"
)

func TestParseSnapshotPath(t *testing.T) {
	for _, tt := range []struct {
		s     string
		index int
		err   error
	}{
		{"0000000000bc614e.snapshot.lz4", 12345678, nil},
		{"xxxxxxxxxxxxxxxx.snapshot.lz4", 0, fmt.Errorf("invalid snapshot path")},
		{"0000000000bc614.snapshot.lz4", 0, fmt.Errorf("invalid snapshot path")},
		{"0000000000bc614e.snapshot.lz", 0, fmt.Errorf("invalid snapshot path")},
		{"0000000000bc614e.snapshot", 0, fmt.Errorf("invalid snapshot path")},
		{"0000000000bc614e", 0, fmt.Errorf("invalid snapshot path")},
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
		{"0000000000bc614e/00000000000003e8.wal.lz4", 12345678, 1000, nil},
		{"0000000000000000/0000000000000000.wal", 0, 0, fmt.Errorf("invalid wal segment path")},
		{"0000000000000000/0000000000000000", 0, 0, fmt.Errorf("invalid wal segment path")},
		{"0000000000000000/", 0, 0, fmt.Errorf("invalid wal segment path")},
		{"0000000000000000", 0, 0, fmt.Errorf("invalid wal segment path")},
		{"", 0, 0, fmt.Errorf("invalid wal segment path")},
	} {
		t.Run("", func(t *testing.T) {
			index, offset, err := internal.ParseWALSegmentPath(tt.s)
			if got, want := index, tt.index; got != want {
				t.Errorf("index=%#v, want %#v", got, want)
			}
			if got, want := offset, tt.offset; got != want {
				t.Errorf("offset=%#v, want %#v", got, want)
			}
			if got, want := err, tt.err; !reflect.DeepEqual(got, want) {
				t.Errorf("err=%#v, want %#v", got, want)
			}
		})
	}
}

func TestTruncateDuration(t *testing.T) {
	for _, tt := range []struct {
		input, output time.Duration
	}{
		{0, 0 * time.Nanosecond},

		{1, 1 * time.Nanosecond},
		{12, 12 * time.Nanosecond},
		{123, 123 * time.Nanosecond},
		{1234, 1 * time.Microsecond},
		{12345, 12 * time.Microsecond},
		{123456, 123 * time.Microsecond},
		{1234567, 1 * time.Millisecond},
		{12345678, 12 * time.Millisecond},
		{123456789, 123 * time.Millisecond},
		{1234567890, 1200 * time.Millisecond},
		{12345678900, 12 * time.Second},

		{-1, -1 * time.Nanosecond},
		{-12, -12 * time.Nanosecond},
		{-123, -123 * time.Nanosecond},
		{-1234, -1 * time.Microsecond},
		{-12345, -12 * time.Microsecond},
		{-123456, -123 * time.Microsecond},
		{-1234567, -1 * time.Millisecond},
		{-12345678, -12 * time.Millisecond},
		{-123456789, -123 * time.Millisecond},
		{-1234567890, -1200 * time.Millisecond},
		{-12345678900, -12 * time.Second},
	} {
		t.Run(fmt.Sprint(int(tt.input)), func(t *testing.T) {
			if got, want := internal.TruncateDuration(tt.input), tt.output; got != want {
				t.Fatalf("duration=%s, want %s", got, want)
			}
		})
	}
}
