package main

import (
	"testing"

	"github.com/superfly/ltx"
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
