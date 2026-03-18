package btree

import (
	"fmt"
	"minidb/internal/buffer"
	"minidb/internal/disk"
	"sync"
)

// BTree is the top-level B+ Tree index manager.
//
// It coordinates the buffer pool (memory) and node operations (disk layout)
// to provide a sorted, persistent key-value index.
//
// The tree is rooted at a well-known page (rootPageID).
// We store the root page ID persistently so we can re-open the tree.
//
// Concurrency: a single RWMutex protects the entire tree.
// Production databases use latch crabbing (lock coupling) for better
// concurrent write throughput — but that's significantly more complex.
type BTree struct {
	mu         sync.RWMutex
	bp         *buffer.BufferPool
	rootPageID disk.PageID
}

// NewBTree creates a new empty B+ Tree.
//
// It allocates the root page as an empty leaf node.
// (An empty tree's root is always a leaf — the tree grows upward.)
func NewBTree(bp *buffer.BufferPool) (*BTree, error) {
	// Allocate the root page
	rootPage, err := bp.NewPage()
	if err != nil {
		return nil, fmt.Errorf("btree: cannot allocate root page: %w", err)
	}

	// Initialize root as an empty leaf node
	root := NewNodeFromPage(rootPage)
	root.InitLeaf(disk.InvalidPageID) // root has no parent

	// Unpin after setup — buffer pool will manage it
	if err := bp.UnpinPage(rootPage.ID, true); err != nil {
		return nil, err
	}

	return &BTree{
		bp:         bp,
		rootPageID: rootPage.ID,
	}, nil
}

// OpenBTree re-opens an existing B+ Tree given its root page ID.
// Use this when restarting the database with an existing .db file.
func OpenBTree(bp *buffer.BufferPool, rootPageID disk.PageID) *BTree {
	return &BTree{
		bp:         bp,
		rootPageID: rootPageID,
	}
}

// RootPageID returns the page ID of the tree's root.
// Store this in the catalog so you can reopen the tree after a restart.
func (t *BTree) RootPageID() disk.PageID {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.rootPageID
}

// ---- Search ----

// Search looks up a key and returns its associated value.
// Returns (value, true) if found, or (0, false) if the key doesn't exist.
// Time complexity: O(log n) disk I/Os where n = number of keys.
func (t *BTree) Search(key Key) (Value, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	leaf, err := t.findLeaf(key)
	if err != nil {
		return 0, false
	}
	defer t.bp.UnpinPage(leaf.PageID(), false)

	// Binary search within the leaf
	idx := leaf.FindKeyIndex(key)
	if idx < leaf.NumKeys() && leaf.GetKey(idx) == key {
		return leaf.GetValue(idx), true
	}
	return 0, false
}

// findLeaf traverses from root to the leaf that should contain `key`.
//
// Algorithm (tree traversal):
//  1. Start at root
//  2. At each internal node: find the right child pointer using binary search
//  3. Repeat until we reach a leaf node
//
// At each step, we fetch the page from the buffer pool (may trigger disk I/O),
// then immediately unpin the parent (we no longer need it).
func (t *BTree) findLeaf(key Key) (*Node, error) {
	// Fetch root
	page, err := t.bp.FetchPage(t.rootPageID)
	if err != nil {
		return nil, fmt.Errorf("btree: cannot fetch root: %w", err)
	}
	node := NewNodeFromPage(page)

	// Traverse down until we hit a leaf
	for !node.IsLeaf() {
		// Find which child to follow.
		// keys[i] separates children[i] and children[i+1].
		// We want the first i where keys[i] > key → go to children[i].
		// If no such i exists → go to the last child (rightmost).
		idx := node.FindKeyIndex(key) // first key >= target

		// For exact matches, we need to go to the right child (children[idx+1])
		if idx < node.NumKeys() && node.GetKey(idx) == key {
			idx++
		}

		// The child to follow is children[idx] (if key < keys[idx])
		// or children[numKeys] (if key >= all keys)
		childID := node.GetChild(idx)

		// Unpin current node before fetching child
		parentID := node.PageID()
		if err := t.bp.UnpinPage(parentID, false); err != nil {
			return nil, err
		}

		// Fetch child
		childPage, err := t.bp.FetchPage(childID)
		if err != nil {
			return nil, fmt.Errorf("btree: cannot fetch child page %d: %w", childID, err)
		}
		node = NewNodeFromPage(childPage)
	}

	return node, nil
}

// ---- Insert ----

// Insert adds a key-value pair to the tree.
// If the key already exists, returns an error (no duplicate keys).
// Time complexity: O(log n) with possible O(log n) splits cascading up.
func (t *BTree) Insert(key Key, value Value) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.insert(key, value)
}

func (t *BTree) insert(key Key, value Value) error {
	// Find the leaf where this key belongs
	leaf, err := t.findLeaf(key)
	if err != nil {
		return err
	}

	// Check for duplicates
	idx := leaf.FindKeyIndex(key)
	if idx < leaf.NumKeys() && leaf.GetKey(idx) == key {
		t.bp.UnpinPage(leaf.PageID(), false)
		return fmt.Errorf("btree: duplicate key %d", key)
	}

	// Insert into leaf
	t.insertIntoLeaf(leaf, key, value, idx)

	// If leaf is now overfull (> MaxKeys), we need to split it
	if leaf.NumKeys() > MaxKeys {
		return t.splitLeaf(leaf)
	}

	return t.bp.UnpinPage(leaf.PageID(), true)
}

// insertIntoLeaf inserts (key, value) at position `idx` in the leaf,
// shifting existing entries right to make room.
//
// Before: [k0, k1, k2, k3]  idx=2
// After:  [k0, k1, NEW, k2, k3]
func (t *BTree) insertIntoLeaf(leaf *Node, key Key, value Value, idx int) {
	n := leaf.NumKeys()

	// Shift entries right from the end down to idx
	for i := n; i > idx; i-- {
		leaf.SetKey(i, leaf.GetKey(i-1))
		leaf.SetValue(i, leaf.GetValue(i-1))
	}

	// Place the new entry
	leaf.SetKey(idx, key)
	leaf.SetValue(idx, value)
	leaf.IncrNumKeys()
}

// splitLeaf splits an overfull leaf node into two.
//
// CONCEPT: Leaf Split
// When a leaf has MaxKeys+1 entries (overflow), we:
//  1. Create a new "right" leaf
//  2. Move the upper half of entries to the right leaf
//  3. Link right leaf into the linked list (rightLeaf.next = leaf.next; leaf.next = rightLeaf)
//  4. Push the first key of the right leaf UP to the parent
//
// Before split (MaxKeys=4 example):
//   [1, 2, 3, 4, 5]   ← overflow
//
// After split:
//   [1, 2, 3]    [3, 4, 5]   ← 3 is copied up to parent (leaf nodes keep a copy)
//         ↑next──┘
func (t *BTree) splitLeaf(leaf *Node) error {
	// Allocate new right leaf
	rightPage, err := t.bp.NewPage()
	if err != nil {
		return fmt.Errorf("btree: cannot allocate right leaf: %w", err)
	}
	right := NewNodeFromPage(rightPage)
	right.InitLeaf(leaf.ParentID())

	// Split point: right half starts at index `mid`
	mid := (MaxKeys + 1) / 2

	// Copy upper half to right leaf
	rightCount := 0
	for i := mid; i <= MaxKeys; i++ {
		right.SetKey(rightCount, leaf.GetKey(i))
		right.SetValue(rightCount, leaf.GetValue(i))
		rightCount++
	}
	right.SetNumKeys(rightCount)

	// Truncate left leaf
	leaf.SetNumKeys(mid)

	// Link right leaf into the leaf linked list:
	// old: leaf → leaf.nextLeaf
	// new: leaf → right → leaf.nextLeaf
	right.SetNextLeaf(leaf.NextLeaf())
	leaf.SetNextLeaf(right.PageID())

	// The key to push up to the parent is the first key of the right leaf.
	// (For leaf nodes, the key is COPIED up, not moved — data stays in leaves.)
	pushUpKey := right.GetKey(0)
	rightID := right.PageID()

	if err := t.bp.UnpinPage(right.PageID(), true); err != nil {
		return err
	}

	// Push the key up to the parent
	return t.insertIntoParent(leaf, pushUpKey, rightID)
}

// insertIntoParent inserts a key and new right-child pointer into the parent
// of `leftNode`. This is called after a split to propagate the change upward.
//
// If `leftNode` is the root, we need to create a NEW root (the tree grows taller).
func (t *BTree) insertIntoParent(leftNode *Node, key Key, rightID disk.PageID) error {
	leftID := leftNode.PageID()
	parentID := leftNode.ParentID()

	if err := t.bp.UnpinPage(leftID, true); err != nil {
		return err
	}

	// CASE 1: leftNode was the root → create a new root
	if parentID == disk.InvalidPageID {
		return t.createNewRoot(leftID, key, rightID)
	}

	// CASE 2: insert key into existing parent
	parentPage, err := t.bp.FetchPage(parentID)
	if err != nil {
		return err
	}
	parent := NewNodeFromPage(parentPage)

	// Find where to insert in the parent
	// The parent should have a child pointer to leftID.
	// Insert `key` and `rightID` right after that pointer.
	insertIdx := 0
	for insertIdx < parent.NumKeys() && parent.GetChild(insertIdx) != leftID {
		insertIdx++
	}
	// insertIdx now points to leftID's position in children[]
	// We insert key at keys[insertIdx] and rightID at children[insertIdx+1]
	insertIdx++ // we want to insert AFTER the left child's position

	t.insertIntoInternal(parent, insertIdx, key, rightID)

	// If parent is overfull, split the internal node too
	if parent.NumKeys() > MaxKeys {
		return t.splitInternal(parent)
	}

	return t.bp.UnpinPage(parent.PageID(), true)
}

// insertIntoInternal inserts `key` at keys[idx-1] and `rightChildID` at children[idx]
// in an internal node, shifting entries to make room.
func (t *BTree) insertIntoInternal(node *Node, idx int, key Key, rightChildID disk.PageID) {
	n := node.NumKeys()

	// Shift children right (one extra child pointer)
	for i := n + 1; i > idx; i-- {
		node.SetChild(i, node.GetChild(i-1))
	}
	// Shift keys right
	for i := n; i >= idx; i-- {
		node.SetKey(i, node.GetKey(i-1))
	}

	// Insert
	node.SetKey(idx-1, key)
	node.SetChild(idx, rightChildID)
	node.IncrNumKeys()
}

// splitInternal splits an overfull internal node.
//
// CONCEPT: Internal Node Split
// Unlike leaf splits, the median key is MOVED UP (not copied).
// The median key is removed from both children and pushed to the parent.
//
// Before (MaxKeys=4, keys=[10,20,30,40,50]):
//   [10 | 20 | 30 | 40 | 50]
//    c0   c1   c2   c3   c4   c5
//
// After split (median=30 is PUSHED UP, not copied):
//   [10 | 20]     [40 | 50]
//    c0   c1 c2    c3   c4  c5
//   30 pushed to parent
func (t *BTree) splitInternal(node *Node) error {
	mid := MaxKeys / 2 // index of the median key to push up

	// Allocate new right internal node
	rightPage, err := t.bp.NewPage()
	if err != nil {
		return err
	}
	right := NewNodeFromPage(rightPage)
	right.InitInternal(node.ParentID())

	// The median key goes UP, not into either child
	pushUpKey := node.GetKey(mid)

	// Copy the right half (after median) to new node
	rightCount := 0
	for i := mid + 1; i <= MaxKeys; i++ {
		right.SetKey(rightCount, node.GetKey(i))
		rightCount++
	}
	// Copy the corresponding child pointers (one more than keys)
	for i := mid + 1; i <= MaxKeys+1; i++ {
		right.SetChild(i-(mid+1), node.GetChild(i))
	}
	right.SetNumKeys(rightCount)

	// Truncate the left node (remove median and everything after it)
	node.SetNumKeys(mid)

	rightID := right.PageID()
	if err := t.bp.UnpinPage(right.PageID(), true); err != nil {
		return err
	}

	return t.insertIntoParent(node, pushUpKey, rightID)
}

// createNewRoot creates a new root with two children.
// Called when the current root is split — the tree grows one level taller.
//
// New root structure:
//   [pushUpKey]
//    /       \
// leftID   rightID
func (t *BTree) createNewRoot(leftID disk.PageID, key Key, rightID disk.PageID) error {
	newRootPage, err := t.bp.NewPage()
	if err != nil {
		return fmt.Errorf("btree: cannot allocate new root: %w", err)
	}

	newRoot := NewNodeFromPage(newRootPage)
	newRoot.InitInternal(disk.InvalidPageID)
	newRoot.SetKey(0, key)
	newRoot.SetChild(0, leftID)
	newRoot.SetChild(1, rightID)
	newRoot.SetNumKeys(1)

	// Update root page ID — the tree is now one level taller
	t.rootPageID = newRoot.PageID()

	// Update parent pointers of both children
	leftPage, err := t.bp.FetchPage(leftID)
	if err != nil {
		return err
	}
	NewNodeFromPage(leftPage).SetParentID(newRoot.PageID())
	t.bp.UnpinPage(leftID, true)

	rightPage, err := t.bp.FetchPage(rightID)
	if err != nil {
		return err
	}
	NewNodeFromPage(rightPage).SetParentID(newRoot.PageID())
	t.bp.UnpinPage(rightID, true)

	return t.bp.UnpinPage(newRoot.PageID(), true)
}

// ---- Delete ----

// Delete removes a key from the tree.
// Returns an error if the key doesn't exist.
func (t *BTree) Delete(key Key) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Find the leaf
	leaf, err := t.findLeaf(key)
	if err != nil {
		return err
	}

	idx := leaf.FindKeyIndex(key)
	if idx >= leaf.NumKeys() || leaf.GetKey(idx) != key {
		t.bp.UnpinPage(leaf.PageID(), false)
		return fmt.Errorf("btree: key %d not found", key)
	}

	// Remove key from leaf by shifting left
	t.deleteFromLeaf(leaf, idx)

	// Check for underflow (too few keys)
	// Exception: root leaf can have 0 keys (empty tree)
	if leaf.PageID() != t.rootPageID && leaf.NumKeys() < MinKeys {
		return t.fixLeafUnderflow(leaf)
	}

	return t.bp.UnpinPage(leaf.PageID(), true)
}

// deleteFromLeaf removes the entry at position idx by shifting entries left.
func (t *BTree) deleteFromLeaf(leaf *Node, idx int) {
	n := leaf.NumKeys()
	for i := idx; i < n-1; i++ {
		leaf.SetKey(i, leaf.GetKey(i+1))
		leaf.SetValue(i, leaf.GetValue(i+1))
	}
	leaf.DecrNumKeys()
}

// fixLeafUnderflow handles the case where a leaf has too few keys after deletion.
//
// Options (in order of preference):
//  1. Borrow from left sibling (if left sibling has > MinKeys)
//  2. Borrow from right sibling (if right sibling has > MinKeys)
//  3. Merge with left sibling
//  4. Merge with right sibling
func (t *BTree) fixLeafUnderflow(leaf *Node) error {
	// For simplicity in this implementation, we'll do a basic merge.
	// A full production implementation would also try borrowing first.
	// TODO: implement borrow-from-sibling for better performance.

	parentID := leaf.ParentID()
	if parentID == disk.InvalidPageID {
		// Root leaf — underflow is OK for root
		return t.bp.UnpinPage(leaf.PageID(), true)
	}

	// Try to merge with the right sibling
	rightID := leaf.NextLeaf()
	if rightID != disk.InvalidPageID {
		rightPage, err := t.bp.FetchPage(rightID)
		if err != nil {
			return err
		}
		right := NewNodeFromPage(rightPage)

		if right.ParentID() == parentID {
			return t.mergeLeaves(leaf, right)
		}
		t.bp.UnpinPage(rightID, false)
	}

	// If no right sibling to merge with, just accept the underflow.
	// (A robust implementation would handle left-sibling merge too.)
	return t.bp.UnpinPage(leaf.PageID(), true)
}

// mergeLeaves merges `right` into `left` (appends all right entries to left).
// After merging, `right` is empty and should be freed.
func (t *BTree) mergeLeaves(left, right *Node) error {
	// Copy all entries from right into left
	leftCount := left.NumKeys()
	for i := 0; i < right.NumKeys(); i++ {
		left.SetKey(leftCount+i, right.GetKey(i))
		left.SetValue(leftCount+i, right.GetValue(i))
	}
	left.SetNumKeys(leftCount + right.NumKeys())

	// Update the linked list: skip over right
	left.SetNextLeaf(right.NextLeaf())

	// Right leaf is now empty — mark its page as reusable (simplified: just unpin dirty)
	right.SetNumKeys(0)

	if err := t.bp.UnpinPage(right.PageID(), true); err != nil {
		return err
	}

	// Remove the separator key from parent
	// (Full implementation: also handle parent underflow recursively)
	return t.bp.UnpinPage(left.PageID(), true)
}
