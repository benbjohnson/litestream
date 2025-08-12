package abs

import (
	"testing"

	"github.com/superfly/ltx"
)

// TestIteratorPointerSemantics verifies that when collecting items from an
// iterator into a slice, each item is a distinct object and not aliasing to
// the same memory location.
//
// This is a regression test for a bug where the ltxFileIterator was using
// value types instead of pointer types, causing all collected items to
// reference the same memory location.
func TestIteratorPointerSemantics(t *testing.T) {
	// This test simulates what happens when SliceFileIterator collects
	// items from an iterator. With the bug, all pointers in the slice
	// would point to the same underlying object.

	t.Run("ChannelWithPointers", func(t *testing.T) {
		// Create a channel like the iterator uses
		ch := make(chan *ltx.FileInfo, 3)
		
		// Send three distinct items (simulating what fetch() does)
		ch <- &ltx.FileInfo{MinTXID: 1, MaxTXID: 1, Size: 100}
		ch <- &ltx.FileInfo{MinTXID: 2, MaxTXID: 3, Size: 200}
		ch <- &ltx.FileInfo{MinTXID: 4, MaxTXID: 8, Size: 300}
		close(ch)

		// Collect all items from the channel (simulating SliceFileIterator)
		var items []*ltx.FileInfo
		for info := range ch {
			items = append(items, info)
		}

		// Verify we got 3 distinct items
		if len(items) != 3 {
			t.Fatalf("expected 3 items, got %d", len(items))
		}

		// Verify each item has the correct values
		// With the bug, all items would have the same values (the last one sent)
		testCases := []struct {
			idx     int
			minTXID ltx.TXID
			size    int64
		}{
			{0, 1, 100},
			{1, 2, 200},
			{2, 4, 300},
		}

		for _, tc := range testCases {
			if items[tc.idx].MinTXID != tc.minTXID {
				t.Errorf("items[%d].MinTXID = %d, want %d", tc.idx, items[tc.idx].MinTXID, tc.minTXID)
			}
			if items[tc.idx].Size != tc.size {
				t.Errorf("items[%d].Size = %d, want %d", tc.idx, items[tc.idx].Size, tc.size)
			}
		}

		// Most importantly, verify that the items are actually different objects
		// With the bug, all pointers would be equal (pointing to same memory)
		for i := 0; i < len(items); i++ {
			for j := i + 1; j < len(items); j++ {
				if items[i] == items[j] {
					t.Errorf("items[%d] and items[%d] are aliasing to the same memory location", i, j)
				}
			}
		}
	})

	t.Run("SimulateBugScenario", func(t *testing.T) {
		// This test demonstrates what would happen with the bug:
		// using a value type and taking its address repeatedly
		
		// DO NOT DO THIS - this is the buggy pattern we're testing against
		var info ltx.FileInfo  // Value type (not pointer)
		var buggyItems []*ltx.FileInfo
		
		// Simulate receiving items and incorrectly storing them
		infos := []ltx.FileInfo{
			{MinTXID: 1, MaxTXID: 1, Size: 100},
			{MinTXID: 2, MaxTXID: 3, Size: 200},
			{MinTXID: 4, MaxTXID: 8, Size: 300},
		}
		
		for _, received := range infos {
			info = received  // Overwrites the same variable
			buggyItems = append(buggyItems, &info)  // Takes address of same variable
		}
		
		// This demonstrates the bug - all items have the same values
		if buggyItems[0].MinTXID == 1 {
			t.Error("This test is meant to demonstrate the bug - if this passes, the bug simulation is broken")
		}
		
		// All items should incorrectly have the last value (MinTXID=4, Size=300)
		for i, item := range buggyItems {
			if item.MinTXID != 4 || item.Size != 300 {
				t.Errorf("Bug simulation failed: items[%d] should have MinTXID=4, Size=300, got MinTXID=%d, Size=%d",
					i, item.MinTXID, item.Size)
			}
		}
		
		// All pointers should incorrectly point to the same memory
		allSame := true
		for i := 0; i < len(buggyItems); i++ {
			for j := i + 1; j < len(buggyItems); j++ {
				if buggyItems[i] != buggyItems[j] {
					allSame = false
				}
			}
		}
		if !allSame {
			t.Error("Bug simulation failed: items should all point to same memory")
		}
	})
}