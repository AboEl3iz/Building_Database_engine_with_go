package tests

import (
	"fmt"
	"math/rand"
	"minidb/internal/btree"
	"minidb/internal/buffer"
	"minidb/internal/disk"
	"os"
	"sort"
	"testing"
)

// ---- Test helpers ----

// newTestBTree creates a fresh B+ tree backed by a temp file.
// Returns the tree and a cleanup function.
func newTestBTree(t *testing.T) (*btree.BTree, *buffer.BufferPool, func()) {
	t.Helper()
	tmpFile, err := os.CreateTemp("", "minidb_btree_test_*.db")
	if err != nil {
		t.Fatalf("cannot create temp file: %v", err)
	}
	tmpFile.Close()

	dm, err := disk.NewDiskManager(tmpFile.Name())
	if err != nil {
		t.Fatalf("cannot create disk manager: %v", err)
	}

	bp := buffer.NewBufferPool(128, dm)

	tree, err := btree.NewBTree(bp)
	if err != nil {
		t.Fatalf("cannot create B+ tree: %v", err)
	}

	cleanup := func() {
		bp.FlushAll()
		dm.Close()
		os.Remove(tmpFile.Name())
	}

	return tree, bp, cleanup
}

// ---- Tests ----

// TestBTreeInsertSearch tests basic insert and point lookup.
func TestBTreeInsertSearch(t *testing.T) {
	tree, _, cleanup := newTestBTree(t)
	defer cleanup()

	// Insert 10 key-value pairs
	for i := int64(1); i <= 10; i++ {
		if err := tree.Insert(i, i*100); err != nil {
			t.Fatalf("Insert(%d) failed: %v", i, err)
		}
	}

	// Search for each one
	for i := int64(1); i <= 10; i++ {
		val, found := tree.Search(i)
		if !found {
			t.Errorf("Search(%d): expected found=true, got false", i)
		}
		if val != i*100 {
			t.Errorf("Search(%d): expected value=%d, got %d", i, i*100, val)
		}
	}

	// Search for non-existent key
	_, found := tree.Search(999)
	if found {
		t.Errorf("Search(999): expected found=false, got true")
	}
}

// TestBTreeInsert1000 tests inserting 1000 keys in order, verifying all are found.
func TestBTreeInsert1000(t *testing.T) {
	tree, _, cleanup := newTestBTree(t)
	defer cleanup()

	const N = 1000

	// Insert in ascending order
	for i := int64(1); i <= N; i++ {
		if err := tree.Insert(i, i*2); err != nil {
			t.Fatalf("Insert(%d) failed: %v", i, err)
		}
	}

	// Verify all found
	for i := int64(1); i <= N; i++ {
		val, found := tree.Search(i)
		if !found {
			t.Errorf("After inserting 1..%d, Search(%d) not found", N, i)
			continue
		}
		if val != i*2 {
			t.Errorf("Search(%d): expected %d, got %d", i, i*2, val)
		}
	}
}

// TestBTreeRandomInsert tests random-order inserts (exercises splits at all levels).
func TestBTreeRandomInsert(t *testing.T) {
	tree, _, cleanup := newTestBTree(t)
	defer cleanup()

	const N = 500
	keys := make([]int64, N)
	for i := range keys {
		keys[i] = int64(i + 1)
	}

	// Shuffle
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	for _, k := range keys {
		if err := tree.Insert(k, k*3); err != nil {
			t.Fatalf("Insert(%d) failed: %v", k, err)
		}
	}

	// Verify all found
	for _, k := range keys {
		val, found := tree.Search(k)
		if !found {
			t.Errorf("Search(%d) not found after random insert", k)
			continue
		}
		if val != k*3 {
			t.Errorf("Search(%d): expected %d, got %d", k, k*3, val)
		}
	}
}

// TestBTreeDuplicate verifies that inserting a duplicate key returns an error.
func TestBTreeDuplicate(t *testing.T) {
	tree, _, cleanup := newTestBTree(t)
	defer cleanup()

	if err := tree.Insert(42, 100); err != nil {
		t.Fatalf("First Insert(42) failed: %v", err)
	}

	if err := tree.Insert(42, 200); err == nil {
		t.Error("Second Insert(42) should have failed with duplicate error")
	}
}

// TestBTreeDelete tests basic deletion.
func TestBTreeDelete(t *testing.T) {
	tree, _, cleanup := newTestBTree(t)
	defer cleanup()

	// Insert 20 keys
	for i := int64(1); i <= 20; i++ {
		tree.Insert(i, i)
	}

	// Delete every other key
	for i := int64(1); i <= 20; i += 2 {
		if err := tree.Delete(i); err != nil {
			t.Errorf("Delete(%d) failed: %v", i, err)
		}
	}

	// Verify deleted keys are gone
	for i := int64(1); i <= 20; i += 2 {
		_, found := tree.Search(i)
		if found {
			t.Errorf("Key %d should be deleted but was found", i)
		}
	}

	// Verify remaining keys still exist
	for i := int64(2); i <= 20; i += 2 {
		_, found := tree.Search(i)
		if !found {
			t.Errorf("Key %d should exist but was not found", i)
		}
	}
}

// TestBTreeDeleteNonExistent verifies that deleting a missing key returns an error.
func TestBTreeDeleteNonExistent(t *testing.T) {
	tree, _, cleanup := newTestBTree(t)
	defer cleanup()

	tree.Insert(1, 1)

	if err := tree.Delete(999); err == nil {
		t.Error("Delete(999) should have failed (key doesn't exist)")
	}
}

// TestBTreeRangeScan tests range scans on the leaf linked list.
func TestBTreeRangeScan(t *testing.T) {
	tree, bp, cleanup := newTestBTree(t)
	defer cleanup()

	// Insert keys 1..200
	for i := int64(1); i <= 200; i++ {
		tree.Insert(i, i*10)
	}

	// Scan [50, 100]
	results, err := btree.Scan(bp, tree.RootPageID(), 50, 100)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Should have 51 results: 50, 51, ..., 100
	expected := 51
	if len(results) != expected {
		t.Errorf("Scan[50,100]: expected %d results, got %d", expected, len(results))
	}

	// Verify results are in ascending order and have correct values
	for i, r := range results {
		expectedKey := int64(50 + i)
		if r.Key != expectedKey {
			t.Errorf("result[%d]: expected key=%d, got %d", i, expectedKey, r.Key)
		}
		if r.Value != expectedKey*10 {
			t.Errorf("result[%d]: expected value=%d, got %d", i, expectedKey*10, r.Value)
		}
	}
}

// TestBTreeRangeScanFull tests scanning the entire tree.
func TestBTreeRangeScanFull(t *testing.T) {
	tree, bp, cleanup := newTestBTree(t)
	defer cleanup()

	const N = 100
	for i := int64(1); i <= N; i++ {
		tree.Insert(i, i)
	}

	results, err := btree.Scan(bp, tree.RootPageID(), 1, N)
	if err != nil {
		t.Fatalf("Full scan failed: %v", err)
	}

	if len(results) != N {
		t.Errorf("Full scan: expected %d results, got %d", N, len(results))
	}

	// Results should be sorted ascending
	for i := 1; i < len(results); i++ {
		if results[i].Key <= results[i-1].Key {
			t.Errorf("Scan not sorted at index %d: key[%d]=%d, key[%d]=%d",
				i, i-1, results[i-1].Key, i, results[i].Key)
		}
	}
}

// TestBTreeSplitAndSearch tests that after many splits, all data is still findable.
// Inserts enough keys to force multiple levels of splits.
func TestBTreeSplitAndSearch(t *testing.T) {
	tree, _, cleanup := newTestBTree(t)
	defer cleanup()

	// Insert enough to force splits at multiple levels
	// With Order=128, MaxKeys=255:
	//   Level 1 fill: 255 keys
	//   Level 2 fill: 255 * 128 = 32640 keys
	// We insert 1000 to ensure at least one internal-node split
	const N = 1000

	for i := int64(0); i < N; i++ {
		if err := tree.Insert(i, i); err != nil {
			t.Fatalf("Insert(%d) failed: %v", i, err)
		}
	}

	// Verify all 1000 keys are still accessible
	for i := int64(0); i < N; i++ {
		val, found := tree.Search(i)
		if !found {
			t.Errorf("After splits, Search(%d) not found", i)
		}
		if val != i {
			t.Errorf("After splits, Search(%d) = %d, want %d", i, val, i)
		}
	}
}

// TestBTreeIteratorEmpty tests iterating over an empty range.
func TestBTreeIteratorEmpty(t *testing.T) {
	tree, bp, cleanup := newTestBTree(t)
	defer cleanup()

	tree.Insert(10, 10)
	tree.Insert(20, 20)

	// Scan a range that has no keys
	results, err := btree.Scan(bp, tree.RootPageID(), 11, 19)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected empty scan, got %d results", len(results))
	}
}

// TestBTreeDescendingInsert tests inserting keys in descending order.
// This stresses the split logic differently than ascending insertion.
func TestBTreeDescendingInsert(t *testing.T) {
	tree, _, cleanup := newTestBTree(t)
	defer cleanup()

	const N = 300
	for i := int64(N); i >= 1; i-- {
		if err := tree.Insert(i, i); err != nil {
			t.Fatalf("Insert(%d) failed: %v", i, err)
		}
	}

	for i := int64(1); i <= N; i++ {
		_, found := tree.Search(i)
		if !found {
			t.Errorf("Search(%d) not found after descending insert", i)
		}
	}
}

// TestBTreeRandomDeleteAndSearch tests random inserts, random deletes, and verifies
// the remaining keys are still searchable.
func TestBTreeRandomDeleteAndSearch(t *testing.T) {
	tree, _, cleanup := newTestBTree(t)
	defer cleanup()

	const N = 200
	keys := make([]int64, N)
	for i := range keys {
		keys[i] = int64(i + 1)
	}

	// Insert all
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	for _, k := range keys {
		tree.Insert(k, k)
	}

	// Delete a random half
	deleteSet := make(map[int64]bool)
	for _, k := range keys[:N/2] {
		deleteSet[k] = true
		if err := tree.Delete(k); err != nil {
			t.Errorf("Delete(%d) failed: %v", k, err)
		}
	}

	// Verify: deleted keys not found, remaining keys found
	for _, k := range keys {
		_, found := tree.Search(k)
		if deleteSet[k] && found {
			t.Errorf("Key %d was deleted but still found", k)
		}
		if !deleteSet[k] && !found {
			t.Errorf("Key %d was not deleted but not found", k)
		}
	}
}

// TestBTreeScanSorted verifies that range scan always returns keys in ascending order,
// even after random insertions.
func TestBTreeScanSorted(t *testing.T) {
	tree, bp, cleanup := newTestBTree(t)
	defer cleanup()

	// Random insertions
	keys := make([]int64, 150)
	for i := range keys {
		keys[i] = int64(i + 1)
	}
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	for _, k := range keys {
		tree.Insert(k, k)
	}

	results, err := btree.Scan(bp, tree.RootPageID(), 1, 150)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Must be sorted ascending
	sortedKeys := make([]int64, len(results))
	for i, r := range results {
		sortedKeys[i] = r.Key
	}

	if !sort.SliceIsSorted(sortedKeys, func(i, j int) bool { return sortedKeys[i] < sortedKeys[j] }) {
		t.Error("Range scan results are not sorted in ascending order")
	}

	if len(results) != 150 {
		t.Errorf("Expected 150 results, got %d", len(results))
	}
}

// Benchmark: Insert performance
func BenchmarkBTreeInsert(b *testing.B) {
	tmpFile, _ := os.CreateTemp("", "minidb_bench_*.db")
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	dm, _ := disk.NewDiskManager(tmpFile.Name())
	defer dm.Close()

	bp := buffer.NewBufferPool(512, dm)
	tree, _ := btree.NewBTree(bp)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Insert(int64(i), int64(i))
	}
}

// Benchmark: Search performance
func BenchmarkBTreeSearch(b *testing.B) {
	tmpFile, _ := os.CreateTemp("", "minidb_bench_*.db")
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	dm, _ := disk.NewDiskManager(tmpFile.Name())
	defer dm.Close()

	bp := buffer.NewBufferPool(512, dm)
	tree, _ := btree.NewBTree(bp)

	// Pre-populate
	for i := int64(0); i < 10000; i++ {
		tree.Insert(i, i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Search(int64(i % 10000))
	}
}

// TestMain prints a summary
func TestMain(m *testing.M) {
	fmt.Println("=== MiniDB B+ Tree Tests ===")
	os.Exit(m.Run())
}
