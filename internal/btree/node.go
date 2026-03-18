// Package btree implements a B+ Tree index backed by the buffer pool.
//
// CONCEPT: B+ Tree vs B-Tree
// ┌──────────────────────────────────────────────────────────┐
// │ B-Tree:  data in EVERY node (internal + leaf)            │
// │ B+ Tree: data ONLY in leaf nodes; internal nodes = index │
// └──────────────────────────────────────────────────────────┘
//
// Why B+ Tree for databases?
//  1. Range scans are O(log n + k): find first leaf, walk linked list
//  2. Internal nodes only hold keys → higher fanout → fewer I/O levels
//  3. All rows are in leaf nodes → predictable row access cost
//
// Tree structure example (order t=2, max 3 keys per node):
//
//	       [30 | 70]              ← internal node (root)
//	      /     |      \
//	  [10|20]  [30|50]  [70|90]  ← internal nodes
//	  /\  \    / \  \   /  \  \
//	[.][.][.] [.][.][.] [.] [.][.] ← leaf nodes (hold actual data)
//	           ↑  linked list  ↑
//
// Leaf nodes form a sorted linked list via nextLeaf pointers.
// To scan range [25, 65]: find leaf containing 25, walk right until key > 65.
package btree

import (
	"encoding/binary"
	"minidb/internal/disk"
)

// Order is the minimum degree of the B+ Tree.
// A node holds between (Order-1) and (2*Order-1) keys.
// Exception: the root can have as few as 1 key.
//
// With PageSize=4096, 8-byte keys, 8-byte values/pointers:
//   Each entry takes 16 bytes.
//   Node header: 20 bytes.
//   Available: (4096 - 20) / 16 ≈ 254 max keys.
//   We set Order=127, so max keys = 253, min keys = 126.
const Order = 127

// MaxKeys is the maximum number of keys per node.
// When a node has MaxKeys+1 keys (overflow), it must be split.
const MaxKeys = 2*Order - 1 // 255

// MinKeys is the minimum number of keys per non-root node.
// When a node falls below MinKeys (underflow), it must merge or borrow.
const MinKeys = Order - 1 // 127

// Key is the type used for B+ Tree keys.
// We use int64 (8 bytes) for simplicity — represents an integer primary key.
// Extension: could become an interface{} to support composite/string keys.
type Key = int64

// Value is what is stored alongside a key in a leaf node.
// For now, Value is an int64 representing a row ID (RID) or inline row data.
// Extension: could store a full serialized row.
type Value = int64

// ---- Node Layout on Disk ----
//
// Every B+ Tree node occupies exactly one 4KB page.
// The binary layout (using encoding/binary LittleEndian) is:
//
// Bytes  0- 1: nodeType (1 = internal, 2 = leaf)
// Bytes  2- 3: numKeys (number of active keys)
// Bytes  4-11: nextLeaf (PageID of the next leaf, or InvalidPageID)
//              Only meaningful for leaf nodes.
// Bytes 12-15: parentID (PageID of parent node, or InvalidPageID)
// Bytes 16+:   keys[] — numKeys * 8 bytes
//              Then: values[] (leaf) or childPtrs[] (internal) — (numKeys+1 if internal, numKeys if leaf) * 8 bytes

// Offsets into page.Data for each field:
const (
	offsetNodeType  = 0  // 2 bytes
	offsetNumKeys   = 2  // 2 bytes
	offsetNextLeaf  = 4  // 8 bytes (PageID = uint32, but we use 8 bytes for alignment)
	offsetParentID  = 12 // 8 bytes
	offsetKeysStart = 20 // keys start here
)

// NodeType distinguishes internal nodes from leaf nodes.
type NodeType uint16

const (
	NodeTypeInternal NodeType = 1
	NodeTypeLeaf     NodeType = 2
)

// Node is an in-memory representation of a B+ Tree node page.
//
// We keep a *disk.Page reference so we can flush changes back to disk.
// All reads/writes go through the page's Data array using binary encoding.
//
// Instead of separate arrays, all data lives in the raw page bytes.
// This avoids serialization overhead — we parse directly from disk format.
type Node struct {
	page *disk.Page // the underlying disk page
}

// NewNodeFromPage wraps a disk page as a Node.
func NewNodeFromPage(page *disk.Page) *Node {
	return &Node{page: page}
}

// PageID returns this node's page ID.
func (n *Node) PageID() disk.PageID {
	return n.page.ID
}

// Page returns the underlying disk page (used when the buffer pool needs it).
func (n *Node) Page() *disk.Page {
	return n.page
}

// ---- Type and shape ----

// IsLeaf returns true if this is a leaf node.
func (n *Node) IsLeaf() bool {
	return n.getNodeType() == NodeTypeLeaf
}

// InitLeaf initializes this node as an empty leaf node.
// Call this right after allocating a new page for a leaf.
func (n *Node) InitLeaf(parentID disk.PageID) {
	n.setNodeType(NodeTypeLeaf)
	n.setNumKeys(0)
	n.setNextLeaf(disk.InvalidPageID)
	n.setParentID(parentID)
	n.page.MarkDirty()
}

// InitInternal initializes this node as an empty internal node.
func (n *Node) InitInternal(parentID disk.PageID) {
	n.setNodeType(NodeTypeInternal)
	n.setNumKeys(0)
	n.setNextLeaf(disk.InvalidPageID) // unused for internal, but zero it out
	n.setParentID(parentID)
	n.page.MarkDirty()
}

// ---- Key access ----

// NumKeys returns the number of active keys in this node.
func (n *Node) NumKeys() int {
	return int(n.getNumKeys())
}

// GetKey returns the key at position i (0-indexed).
func (n *Node) GetKey(i int) Key {
	offset := offsetKeysStart + i*8
	return int64(binary.LittleEndian.Uint64(n.page.Data[offset : offset+8]))
}

// SetKey writes a key at position i.
func (n *Node) SetKey(i int, key Key) {
	offset := offsetKeysStart + i*8
	binary.LittleEndian.PutUint64(n.page.Data[offset:offset+8], uint64(key))
	n.page.MarkDirty()
}

// ---- Value access (leaf nodes) ----
//
// For leaf nodes, values are stored AFTER all keys.
// Layout: [keys: (MaxKeys+1) * 8 bytes] [values: (MaxKeys+1) * 8 bytes]
//
// Leaf value offset calculation:
//   keys section size = (MaxKeys+1) * 8
//   values start at = offsetKeysStart + (MaxKeys+1) * 8

func leafValueOffset(i int) int {
	return offsetKeysStart + (MaxKeys+1)*8 + i*8
}

// GetValue returns the value at position i in a leaf node.
func (n *Node) GetValue(i int) Value {
	offset := leafValueOffset(i)
	return int64(binary.LittleEndian.Uint64(n.page.Data[offset : offset+8]))
}

// SetValue writes a value at position i in a leaf node.
func (n *Node) SetValue(i int, val Value) {
	offset := leafValueOffset(i)
	binary.LittleEndian.PutUint64(n.page.Data[offset:offset+8], uint64(val))
	n.page.MarkDirty()
}

// ---- Child pointer access (internal nodes) ----
//
// For internal nodes, child pointers are stored AFTER all keys.
// An internal node with k keys has k+1 child pointers.
// Layout: [keys: (MaxKeys+1) * 8 bytes] [children: (MaxKeys+2) * 8 bytes]
//
// Semantics: keys[i] separates children[i] and children[i+1]
//   - All keys in children[i] < keys[i]
//   - All keys in children[i+1] >= keys[i]

func internalChildOffset(i int) int {
	return offsetKeysStart + (MaxKeys+1)*8 + i*8
}

// GetChild returns the PageID of the i-th child pointer in an internal node.
func (n *Node) GetChild(i int) disk.PageID {
	offset := internalChildOffset(i)
	return disk.PageID(binary.LittleEndian.Uint64(n.page.Data[offset : offset+8]))
}

// SetChild writes the i-th child pointer in an internal node.
func (n *Node) SetChild(i int, childID disk.PageID) {
	offset := internalChildOffset(i)
	binary.LittleEndian.PutUint64(n.page.Data[offset:offset+8], uint64(childID))
	n.page.MarkDirty()
}

// ---- Leaf linked list ----

// NextLeaf returns the PageID of the next leaf node in the linked list.
// Returns disk.InvalidPageID if this is the last leaf.
func (n *Node) NextLeaf() disk.PageID {
	return n.getNextLeaf()
}

// SetNextLeaf sets the pointer to the next leaf node.
func (n *Node) SetNextLeaf(id disk.PageID) {
	n.setNextLeaf(id)
	n.page.MarkDirty()
}

// ---- Parent tracking ----

// ParentID returns the PageID of this node's parent, or InvalidPageID for root.
func (n *Node) ParentID() disk.PageID {
	return n.getParentID()
}

// SetParentID updates the parent pointer.
func (n *Node) SetParentID(id disk.PageID) {
	n.setParentID(id)
	n.page.MarkDirty()
}

// ---- Binary encoding helpers (raw read/write into page.Data) ----

func (n *Node) getNodeType() NodeType {
	return NodeType(binary.LittleEndian.Uint16(n.page.Data[offsetNodeType : offsetNodeType+2]))
}

func (n *Node) setNodeType(t NodeType) {
	binary.LittleEndian.PutUint16(n.page.Data[offsetNodeType:offsetNodeType+2], uint16(t))
}

func (n *Node) getNumKeys() uint16 {
	return binary.LittleEndian.Uint16(n.page.Data[offsetNumKeys : offsetNumKeys+2])
}

func (n *Node) setNumKeys(n_ uint16) {
	binary.LittleEndian.PutUint16(n.page.Data[offsetNumKeys:offsetNumKeys+2], n_)
}

func (n *Node) getNextLeaf() disk.PageID {
	return disk.PageID(binary.LittleEndian.Uint64(n.page.Data[offsetNextLeaf : offsetNextLeaf+8]))
}

func (n *Node) setNextLeaf(id disk.PageID) {
	binary.LittleEndian.PutUint64(n.page.Data[offsetNextLeaf:offsetNextLeaf+8], uint64(id))
}

func (n *Node) getParentID() disk.PageID {
	return disk.PageID(binary.LittleEndian.Uint64(n.page.Data[offsetParentID : offsetParentID+8]))
}

func (n *Node) setParentID(id disk.PageID) {
	binary.LittleEndian.PutUint64(n.page.Data[offsetParentID:offsetParentID+8], uint64(id))
}

// ---- Key search helpers ----

// FindKeyIndex returns the index i such that keys[i] is the first key >= target.
// Uses binary search for O(log n) within the node.
//
// For internal nodes, this tells us which child subtree to descend into:
//   - If keys[i] == target: go to children[i+1]
//   - If keys[i] > target: go to children[i]
//
// For leaf nodes, this tells us where the key lives (or should be inserted).
func (n *Node) FindKeyIndex(target Key) int {
	lo, hi := 0, n.NumKeys()
	for lo < hi {
		mid := (lo + hi) / 2
		if n.GetKey(mid) < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// IncrNumKeys increments the key count by 1.
func (n *Node) IncrNumKeys() {
	n.setNumKeys(n.getNumKeys() + 1)
	n.page.MarkDirty()
}

// DecrNumKeys decrements the key count by 1.
func (n *Node) DecrNumKeys() {
	if n.getNumKeys() > 0 {
		n.setNumKeys(n.getNumKeys() - 1)
		n.page.MarkDirty()
	}
}

// SetNumKeys directly sets the key count.
func (n *Node) SetNumKeys(count int) {
	n.setNumKeys(uint16(count))
	n.page.MarkDirty()
}
