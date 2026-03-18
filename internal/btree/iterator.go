package btree

import (
	"fmt"
	"minidb/internal/buffer"
	"minidb/internal/disk"
)

// Iterator performs a range scan over B+ Tree leaf nodes.
//
// CONCEPT: Why range scans are fast in B+ Trees
// Leaf nodes are linked in a sorted doubly-linked list.
// A range scan [low, high] only requires:
//  1. One O(log n) traversal to find the leaf containing `low`
//  2. Sequential scans of leaf pages until we exceed `high`
//
// This is extremely cache-friendly — we read consecutive pages from disk.
//
// Example scan for range [30, 70] with 3 items per leaf:
//
//	[10|20|30] → [40|50|60] → [70|80|90]
//	       ↑start here              ↑stop here
//	       read 30, walk → read 40,50,60 → read 70, stop
type Iterator struct {
	bp          *buffer.BufferPool
	currentNode *Node      // the leaf node we're currently scanning
	currentIdx  int        // position within currentNode
	endKey      Key        // stop when key > endKey
	done        bool       // true when we've passed the end
}

// NewIterator creates a range iterator starting at `startKey` and ending at `endKey` (inclusive).
//
// The iterator is lazy — it only loads pages as you call Next().
// After creation, call Next() to advance, and Key()/Value() to read the current entry.
func NewIterator(bp *buffer.BufferPool, rootPageID disk.PageID, startKey, endKey Key) (*Iterator, error) {
	if startKey > endKey {
		return nil, fmt.Errorf("iterator: startKey (%d) > endKey (%d)", startKey, endKey)
	}

	// Find the leaf that should contain startKey
	// (We inline findLeaf logic here to avoid circular dependency on BTree)
	leaf, err := findLeafPage(bp, rootPageID, startKey)
	if err != nil {
		return nil, fmt.Errorf("iterator: cannot find start leaf: %w", err)
	}

	// Find the first key >= startKey within the leaf
	startIdx := leaf.FindKeyIndex(startKey)

	iter := &Iterator{
		bp:          bp,
		currentNode: leaf,
		currentIdx:  startIdx,
		endKey:      endKey,
		done:        false,
	}

	// If startIdx is past the end of this leaf, advance to the next leaf
	if startIdx >= leaf.NumKeys() {
		if err := iter.advanceLeaf(); err != nil {
			return nil, err
		}
	}

	// Check if we're already past the end key
	if !iter.done && iter.currentNode.GetKey(iter.currentIdx) > endKey {
		iter.done = true
	}

	return iter, nil
}

// Valid returns true if the iterator is positioned on a valid entry.
// Call this before accessing Key() or Value().
func (it *Iterator) Valid() bool {
	return !it.done
}

// Key returns the key at the current iterator position.
// Panics if Valid() is false.
func (it *Iterator) Key() Key {
	if it.done {
		panic("iterator: Key() called on exhausted iterator")
	}
	return it.currentNode.GetKey(it.currentIdx)
}

// Value returns the value at the current iterator position.
// Panics if Valid() is false.
func (it *Iterator) Value() Value {
	if it.done {
		panic("iterator: Value() called on exhausted iterator")
	}
	return it.currentNode.GetValue(it.currentIdx)
}

// Next advances the iterator to the next entry.
//
// Algorithm:
//  1. Move currentIdx forward by 1
//  2. If we've reached the end of the current leaf:
//     a. Follow the nextLeaf pointer
//     b. Unpin the current leaf (we're done with it)
//     c. Load the next leaf from the buffer pool
//  3. Check if we've exceeded endKey
func (it *Iterator) Next() error {
	if it.done {
		return nil
	}

	it.currentIdx++

	// If we've consumed all entries in this leaf, move to the next leaf
	if it.currentIdx >= it.currentNode.NumKeys() {
		if err := it.advanceLeaf(); err != nil {
			return err
		}
		if it.done {
			return nil
		}
	}

	// Check if the current key exceeds our scan range
	if it.currentNode.GetKey(it.currentIdx) > it.endKey {
		it.done = true
	}

	return nil
}

// Close releases the current leaf page from the buffer pool.
// Must be called when you're done with the iterator (even if not fully consumed),
// otherwise the buffer pool will have a pinned page that can never be evicted.
//
// Pattern:
//   iter, _ := NewIterator(bp, rootID, 10, 100)
//   defer iter.Close()
//   for iter.Valid() {
//       fmt.Println(iter.Key(), iter.Value())
//       iter.Next()
//   }
func (it *Iterator) Close() {
	if it.currentNode != nil {
		it.bp.UnpinPage(it.currentNode.PageID(), false)
		it.currentNode = nil
	}
	it.done = true
}

// advanceLeaf moves the iterator to the first entry of the next leaf node.
// Sets it.done = true if there is no next leaf.
func (it *Iterator) advanceLeaf() error {
	nextID := it.currentNode.NextLeaf()

	// Unpin the current leaf before fetching the next one
	if err := it.bp.UnpinPage(it.currentNode.PageID(), false); err != nil {
		return fmt.Errorf("iterator: unpin failed: %w", err)
	}
	it.currentNode = nil

	if nextID == disk.InvalidPageID {
		it.done = true
		return nil
	}

	// Fetch the next leaf
	nextPage, err := it.bp.FetchPage(nextID)
	if err != nil {
		it.done = true
		return fmt.Errorf("iterator: cannot fetch next leaf %d: %w", nextID, err)
	}

	it.currentNode = NewNodeFromPage(nextPage)
	it.currentIdx = 0
	return nil
}

// findLeafPage is a standalone helper to find the leaf containing `key`,
// starting from `rootPageID`. Used by the Iterator to avoid depending on BTree.
func findLeafPage(bp *buffer.BufferPool, rootPageID disk.PageID, key Key) (*Node, error) {
	page, err := bp.FetchPage(rootPageID)
	if err != nil {
		return nil, fmt.Errorf("findLeaf: cannot fetch root page %d: %w", rootPageID, err)
	}
	node := NewNodeFromPage(page)

	for !node.IsLeaf() {
		idx := node.FindKeyIndex(key)
		if idx < node.NumKeys() && node.GetKey(idx) == key {
			idx++
		}
		childID := node.GetChild(idx)

		parentID := node.PageID()
		if err := bp.UnpinPage(parentID, false); err != nil {
			return nil, err
		}

		childPage, err := bp.FetchPage(childID)
		if err != nil {
			return nil, fmt.Errorf("findLeaf: cannot fetch child %d: %w", childID, err)
		}
		node = NewNodeFromPage(childPage)
	}

	return node, nil
}

// ---- Convenience: collect all results of a range scan ----

// ScanResult holds one key-value pair from a scan.
type ScanResult struct {
	Key   Key
	Value Value
}

// Scan returns all key-value pairs with keys in [startKey, endKey].
// Convenience wrapper around Iterator for when you want all results eagerly.
//
// For large ranges, prefer using the Iterator directly to avoid loading
// everything into memory at once.
func Scan(bp *buffer.BufferPool, rootPageID disk.PageID, startKey, endKey Key) ([]ScanResult, error) {
	iter, err := NewIterator(bp, rootPageID, startKey, endKey)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var results []ScanResult
	for iter.Valid() {
		results = append(results, ScanResult{
			Key:   iter.Key(),
			Value: iter.Value(),
		})
		if err := iter.Next(); err != nil {
			return results, err
		}
	}
	return results, nil
}
