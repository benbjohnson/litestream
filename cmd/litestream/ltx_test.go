package main

import (
	"testing"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
)

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
