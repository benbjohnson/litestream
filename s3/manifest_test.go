package s3_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream/s3"
)

func TestManifest_AddFile(t *testing.T) {
	m := s3.NewManifest()

	m.AddFile(&ltx.FileInfo{Level: 0, MinTXID: 3, MaxTXID: 3, Size: 100})
	m.AddFile(&ltx.FileInfo{Level: 0, MinTXID: 1, MaxTXID: 1, Size: 200})
	m.AddFile(&ltx.FileInfo{Level: 0, MinTXID: 5, MaxTXID: 5, Size: 300})
	m.AddFile(&ltx.FileInfo{Level: 1, MinTXID: 1, MaxTXID: 5, Size: 500})

	entries := m.EntriesForLevel(0, 0)
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	if entries[0].MinTXID != 1 {
		t.Fatalf("expected first entry MinTXID=1, got %d", entries[0].MinTXID)
	}
	if entries[1].MinTXID != 3 {
		t.Fatalf("expected second entry MinTXID=3, got %d", entries[1].MinTXID)
	}
	if entries[2].MinTXID != 5 {
		t.Fatalf("expected third entry MinTXID=5, got %d", entries[2].MinTXID)
	}

	l1Entries := m.EntriesForLevel(1, 0)
	if len(l1Entries) != 1 {
		t.Fatalf("expected 1 level-1 entry, got %d", len(l1Entries))
	}
	if l1Entries[0].Size != 500 {
		t.Fatalf("expected level-1 entry size=500, got %d", l1Entries[0].Size)
	}
}

func TestManifest_RemoveFiles(t *testing.T) {
	m := s3.NewManifest()

	m.AddFile(&ltx.FileInfo{Level: 0, MinTXID: 1, MaxTXID: 1, Size: 100})
	m.AddFile(&ltx.FileInfo{Level: 0, MinTXID: 2, MaxTXID: 2, Size: 200})
	m.AddFile(&ltx.FileInfo{Level: 0, MinTXID: 3, MaxTXID: 3, Size: 300})

	m.RemoveFiles([]*ltx.FileInfo{
		{Level: 0, MinTXID: 1, MaxTXID: 1},
		{Level: 0, MinTXID: 3, MaxTXID: 3},
	})

	entries := m.EntriesForLevel(0, 0)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after removal, got %d", len(entries))
	}
	if entries[0].MinTXID != 2 {
		t.Fatalf("expected remaining entry MinTXID=2, got %d", entries[0].MinTXID)
	}
}

func TestManifest_RemoveFiles_CleansEmptyLevel(t *testing.T) {
	m := s3.NewManifest()

	m.AddFile(&ltx.FileInfo{Level: 0, MinTXID: 1, MaxTXID: 1, Size: 100})
	m.RemoveFiles([]*ltx.FileInfo{
		{Level: 0, MinTXID: 1, MaxTXID: 1},
	})

	entries := m.EntriesForLevel(0, 0)
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries after removing all, got %d", len(entries))
	}
}

func TestManifest_EntriesForLevel(t *testing.T) {
	m := s3.NewManifest()

	m.AddFile(&ltx.FileInfo{Level: 0, MinTXID: 1, MaxTXID: 1, Size: 100})
	m.AddFile(&ltx.FileInfo{Level: 0, MinTXID: 2, MaxTXID: 2, Size: 200})
	m.AddFile(&ltx.FileInfo{Level: 0, MinTXID: 3, MaxTXID: 5, Size: 300})
	m.AddFile(&ltx.FileInfo{Level: 0, MinTXID: 6, MaxTXID: 10, Size: 400})

	entries := m.EntriesForLevel(0, 3)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries with seek=3, got %d", len(entries))
	}
	if entries[0].MinTXID != 3 {
		t.Fatalf("expected first entry MinTXID=3, got %d", entries[0].MinTXID)
	}
	if entries[1].MinTXID != 6 {
		t.Fatalf("expected second entry MinTXID=6, got %d", entries[1].MinTXID)
	}
}

func TestManifest_EntriesForLevel_SeekWithinRange(t *testing.T) {
	m := s3.NewManifest()

	m.AddFile(&ltx.FileInfo{Level: 0, MinTXID: 1, MaxTXID: 5, Size: 100})
	m.AddFile(&ltx.FileInfo{Level: 0, MinTXID: 6, MaxTXID: 10, Size: 200})

	entries := m.EntriesForLevel(0, 3)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries when seek is within range, got %d", len(entries))
	}
	if entries[0].MinTXID != 1 {
		t.Fatalf("expected entry with MinTXID=1 (MaxTXID=5 >= seek=3), got %d", entries[0].MinTXID)
	}
}

func TestManifest_JSON(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	m := s3.NewManifest()
	m.AddFile(&ltx.FileInfo{Level: 0, MinTXID: 1, MaxTXID: 1, Size: 100, CreatedAt: ts})
	m.AddFile(&ltx.FileInfo{Level: 0, MinTXID: 2, MaxTXID: 5, Size: 500, CreatedAt: ts})
	m.AddFile(&ltx.FileInfo{Level: 1, MinTXID: 1, MaxTXID: 5, Size: 1000, CreatedAt: ts})

	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded s3.Manifest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Version != s3.ManifestVersion {
		t.Fatalf("expected version %d, got %d", s3.ManifestVersion, decoded.Version)
	}

	l0 := decoded.Levels[0]
	if len(l0) != 2 {
		t.Fatalf("expected 2 level-0 entries, got %d", len(l0))
	}
	if l0[0].MinTXID != 1 || l0[0].MaxTXID != 1 || l0[0].Size != 100 {
		t.Fatalf("unexpected level-0 entry 0: %+v", l0[0])
	}
	if l0[1].MinTXID != 2 || l0[1].MaxTXID != 5 || l0[1].Size != 500 {
		t.Fatalf("unexpected level-0 entry 1: %+v", l0[1])
	}

	l1 := decoded.Levels[1]
	if len(l1) != 1 {
		t.Fatalf("expected 1 level-1 entry, got %d", len(l1))
	}
}

func TestManifest_EmptyLevels(t *testing.T) {
	m := s3.NewManifest()

	entries := m.EntriesForLevel(0, 0)
	if entries != nil {
		t.Fatalf("expected nil for empty level, got %v", entries)
	}

	entries = m.EntriesForLevel(99, 0)
	if entries != nil {
		t.Fatalf("expected nil for nonexistent level, got %v", entries)
	}
}

func TestManifestIterator(t *testing.T) {
	m := s3.NewManifest()
	m.AddFile(&ltx.FileInfo{Level: 0, MinTXID: 1, MaxTXID: 1, Size: 100})
	m.AddFile(&ltx.FileInfo{Level: 0, MinTXID: 2, MaxTXID: 2, Size: 200})
	m.AddFile(&ltx.FileInfo{Level: 0, MinTXID: 3, MaxTXID: 3, Size: 300})

	itr := s3.NewManifestIteratorForTest(m.EntriesForLevel(0, 0))
	defer itr.Close()

	var items []*ltx.FileInfo
	for itr.Next() {
		items = append(items, itr.Item())
	}
	if err := itr.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}

	if len(items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(items))
	}
	if items[0].MinTXID != 1 || items[1].MinTXID != 2 || items[2].MinTXID != 3 {
		t.Fatalf("unexpected order: %v, %v, %v", items[0].MinTXID, items[1].MinTXID, items[2].MinTXID)
	}
}

func TestManifestIterator_Empty(t *testing.T) {
	itr := s3.NewManifestIteratorForTest(nil)
	defer itr.Close()

	if itr.Next() {
		t.Fatal("expected no items from empty iterator")
	}
	if itr.Item() != nil {
		t.Fatal("expected nil item from empty iterator")
	}
	if itr.Err() != nil {
		t.Fatal("expected no error from empty iterator")
	}
}
