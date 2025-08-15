package nats

import (
	"testing"
	"time"

	"github.com/superfly/ltx"
)

func TestReplicaClient_Type(t *testing.T) {
	client := NewReplicaClient()
	if got, want := client.Type(), "nats"; got != want {
		t.Fatalf("Type()=%s, want %s", got, want)
	}
}

func TestReplicaClient_ltxPath(t *testing.T) {
	client := NewReplicaClient()

	tests := []struct {
		level   int
		minTXID ltx.TXID
		maxTXID ltx.TXID
		want    string
	}{
		{0, ltx.TXID(0x1000), ltx.TXID(0x2000), "ltx/0/0000000000001000-0000000000002000.ltx"},
		{1, ltx.TXID(0xabcd), ltx.TXID(0xef01), "ltx/1/000000000000abcd-000000000000ef01.ltx"},
		{255, ltx.TXID(0xffffffffffffffff), ltx.TXID(0xffffffffffffffff), "ltx/255/ffffffffffffffff-ffffffffffffffff.ltx"},
	}

	for _, test := range tests {
		if got := client.ltxPath(test.level, test.minTXID, test.maxTXID); got != test.want {
			t.Errorf("ltxPath(%d, %x, %x)=%s, want %s", test.level, test.minTXID, test.maxTXID, got, test.want)
		}
	}
}

func TestReplicaClient_parseLTXPath(t *testing.T) {
	client := NewReplicaClient()

	tests := []struct {
		path    string
		level   int
		minTXID ltx.TXID
		maxTXID ltx.TXID
		wantErr bool
	}{
		{
			path:    "ltx/0/0000000000001000-0000000000002000.ltx",
			level:   0,
			minTXID: ltx.TXID(0x1000),
			maxTXID: ltx.TXID(0x2000),
			wantErr: false,
		},
		{
			path:    "ltx/1/000000000000abcd-000000000000ef01.ltx",
			level:   1,
			minTXID: ltx.TXID(0xabcd),
			maxTXID: ltx.TXID(0xef01),
			wantErr: false,
		},
		{
			path:    "invalid/path",
			wantErr: true,
		},
		{
			path:    "ltx/x/invalid-invalid.ltx",
			wantErr: true,
		},
		{
			path:    "ltx/0/invalid.ltx",
			wantErr: true,
		},
	}

	for _, test := range tests {
		level, minTXID, maxTXID, err := client.parseLTXPath(test.path)
		if test.wantErr {
			if err == nil {
				t.Errorf("parseLTXPath(%s) expected error, got nil", test.path)
			}
			continue
		}

		if err != nil {
			t.Errorf("parseLTXPath(%s) unexpected error: %v", test.path, err)
			continue
		}

		if level != test.level {
			t.Errorf("parseLTXPath(%s) level=%d, want %d", test.path, level, test.level)
		}
		if minTXID != test.minTXID {
			t.Errorf("parseLTXPath(%s) minTXID=%x, want %x", test.path, minTXID, test.minTXID)
		}
		if maxTXID != test.maxTXID {
			t.Errorf("parseLTXPath(%s) maxTXID=%x, want %x", test.path, maxTXID, test.maxTXID)
		}
	}
}

func TestReplicaClient_isNotFoundError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "not found error",
			err:  &mockNotFoundError{},
			want: true,
		},
		{
			name: "other error",
			err:  &mockOtherError{},
			want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isNotFoundError(test.err); got != test.want {
				t.Errorf("isNotFoundError() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestLtxFileIterator(t *testing.T) {
	files := []*ltx.FileInfo{
		{Level: 0, MinTXID: 1, MaxTXID: 10, Size: 100},
		{Level: 0, MinTXID: 11, MaxTXID: 20, Size: 200},
		{Level: 0, MinTXID: 21, MaxTXID: 30, Size: 300},
	}

	itr := &ltxFileIterator{files: files, index: -1}

	// Test initial state
	if item := itr.Item(); item != nil {
		t.Errorf("Item() before Next() should return nil, got %v", item)
	}

	// Test iteration
	var items []*ltx.FileInfo
	for itr.Next() {
		item := itr.Item()
		if item == nil {
			t.Fatal("Item() returned nil during valid iteration")
		}
		items = append(items, item)
	}

	if len(items) != len(files) {
		t.Errorf("Expected %d items, got %d", len(files), len(items))
	}

	for i, item := range items {
		if item != files[i] {
			t.Errorf("Item %d: expected %v, got %v", i, files[i], item)
		}
	}

	// Test after iteration ends
	if itr.Next() {
		t.Error("Next() should return false after iteration ends")
	}

	// Test Close
	if err := itr.Close(); err != nil {
		t.Errorf("Close() returned error: %v", err)
	}

	// Test Err
	if err := itr.Err(); err != nil {
		t.Errorf("Err() returned error: %v", err)
	}
}

// Mock error types for testing

type mockNotFoundError struct{}

func (e *mockNotFoundError) Error() string {
	return "not found"
}

type mockOtherError struct{}

func (e *mockOtherError) Error() string {
	return "some other error"
}

func TestReplicaClientDefaults(t *testing.T) {
	client := NewReplicaClient()

	// Test default values
	if client.MaxReconnects != -1 {
		t.Errorf("Expected MaxReconnects=-1, got %d", client.MaxReconnects)
	}

	if client.ReconnectWait != 2*time.Second {
		t.Errorf("Expected ReconnectWait=2s, got %v", client.ReconnectWait)
	}

	if client.Timeout != 10*time.Second {
		t.Errorf("Expected Timeout=10s, got %v", client.Timeout)
	}
}
