package s3

import (
	"sort"
	"time"

	"github.com/superfly/ltx"
)

const ManifestVersion = 1

type Manifest struct {
	Version int                     `json:"version"`
	Levels  map[int][]ManifestEntry `json:"levels"`
}

type ManifestEntry struct {
	MinTXID   ltx.TXID  `json:"min_txid"`
	MaxTXID   ltx.TXID  `json:"max_txid"`
	Size      int64     `json:"size"`
	CreatedAt time.Time `json:"created_at"`
}

func NewManifest() *Manifest {
	return &Manifest{
		Version: ManifestVersion,
		Levels:  make(map[int][]ManifestEntry),
	}
}

func (m *Manifest) AddFile(info *ltx.FileInfo) {
	entry := ManifestEntry{
		MinTXID:   info.MinTXID,
		MaxTXID:   info.MaxTXID,
		Size:      info.Size,
		CreatedAt: info.CreatedAt,
	}

	entries := m.Levels[info.Level]
	i := sort.Search(len(entries), func(i int) bool {
		return entries[i].MinTXID >= entry.MinTXID
	})
	entries = append(entries, ManifestEntry{})
	copy(entries[i+1:], entries[i:])
	entries[i] = entry
	m.Levels[info.Level] = entries
}

func (m *Manifest) RemoveFiles(infos []*ltx.FileInfo) {
	for _, info := range infos {
		entries := m.Levels[info.Level]
		for i, e := range entries {
			if e.MinTXID == info.MinTXID && e.MaxTXID == info.MaxTXID {
				m.Levels[info.Level] = append(entries[:i], entries[i+1:]...)
				break
			}
		}
		if len(m.Levels[info.Level]) == 0 {
			delete(m.Levels, info.Level)
		}
	}
}

func (m *Manifest) EntriesForLevel(level int, seek ltx.TXID) []*ltx.FileInfo {
	entries := m.Levels[level]
	if len(entries) == 0 {
		return nil
	}

	var result []*ltx.FileInfo
	for _, e := range entries {
		if e.MaxTXID < seek {
			continue
		}
		result = append(result, &ltx.FileInfo{
			Level:     level,
			MinTXID:   e.MinTXID,
			MaxTXID:   e.MaxTXID,
			Size:      e.Size,
			CreatedAt: e.CreatedAt,
		})
	}
	return result
}

type manifestIterator struct {
	items []*ltx.FileInfo
	index int
}

func newManifestIterator(items []*ltx.FileInfo) *manifestIterator {
	return &manifestIterator{items: items, index: -1}
}

func (itr *manifestIterator) Close() error { return nil }

func (itr *manifestIterator) Next() bool {
	if itr.index+1 >= len(itr.items) {
		return false
	}
	itr.index++
	return true
}

func (itr *manifestIterator) Item() *ltx.FileInfo {
	if itr.index < 0 || itr.index >= len(itr.items) {
		return nil
	}
	return itr.items[itr.index]
}

func (itr *manifestIterator) Err() error { return nil }

func NewManifestIteratorForTest(items []*ltx.FileInfo) ltx.FileIterator {
	return newManifestIterator(items)
}
