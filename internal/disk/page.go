// Package disk handles the lowest layer of MiniDB: raw byte I/O on disk.
//
// CONCEPT: Why pages?
// Real databases never read/write individual bytes. They work in fixed-size
// "pages" (usually 4KB or 8KB) because:
//   - OS memory management works in page-sized units (mmap aligns to pages)
//   - Disk sectors are often 512B or 4KB — reading/writing a full page is atomic
//   - Buffer pool can track, cache, and evict pages as uniform units
//
// Think of pages as the "cells" of database storage.
package disk

import "fmt"

// PageSize is the fixed size of every page in bytes.
// 4096 = 4KB, chosen to match the typical OS virtual memory page size.
// This means one database page = one OS page = one disk sector (modern drives).
const PageSize = 4096

// PageID uniquely identifies a page within the database file.
// Page 0 is at byte offset 0, Page 1 at byte offset 4096, etc.
// Formula: file_offset = PageID * PageSize
type PageID uint32

// InvalidPageID represents "no page" / null pointer in page-level data structures.
// Used like a null pointer — e.g., a leaf node's nextLeaf = InvalidPageID means
// "this is the last leaf".
const InvalidPageID PageID = ^PageID(0) // 0xFFFFFFFF

// Page is the in-memory representation of one 4KB block of the database file.
//
// The raw Data array is what actually gets written to disk byte-for-byte.
// The other fields (ID, dirty, pinCount) are only used by the buffer pool
// to manage this page in memory — they are NOT persisted.
type Page struct {
	// ID is which page this is (its position in the file).
	// Set when the buffer pool loads or allocates this page.
	ID PageID

	// Data holds the raw 4096 bytes of this page.
	// Higher-level structures (B+ tree nodes, WAL records, etc.) read/write
	// into this array using encoding/binary. This is what gets flushed to disk.
	Data [PageSize]byte

	// dirty is true if Data has been modified since it was last written to disk.
	// The buffer pool checks this flag before evicting: if dirty == true,
	// it must flush the page first (write Data to disk) to avoid data loss.
	dirty bool

	// pinCount tracks how many goroutines/operations are currently using this page.
	// A page with pinCount > 0 cannot be evicted from the buffer pool.
	// Example: while a B+ tree split is in progress, all involved pages are pinned.
	pinCount int
}

// NewPage creates an empty in-memory page with the given ID.
// The Data array starts as all zeros — callers write their structures into it.
func NewPage(id PageID) *Page {
	return &Page{ID: id}
}

// IsDirty returns true if this page has unsaved changes.
// The buffer pool uses this to decide whether to flush before eviction.
func (p *Page) IsDirty() bool {
	return p.dirty
}

// MarkDirty flags this page as modified.
// Call this any time you write into p.Data.
// The buffer pool will then flush this page to disk before evicting it.
func (p *Page) MarkDirty() {
	p.dirty = true
}

// ClearDirty clears the dirty flag after the page has been written to disk.
// Called by the disk manager after a successful write.
func (p *Page) ClearDirty() {
	p.dirty = false
}

// Pin increments the pin count, preventing eviction.
// Call Pin() before you start using a page.
// IMPORTANT: always pair with Unpin() or you'll permanently prevent eviction.
func (p *Page) Pin() {
	p.pinCount++
}

// Unpin decrements the pin count.
// When pinCount reaches 0, the buffer pool is free to evict this page.
func (p *Page) Unpin() {
	if p.pinCount > 0 {
		p.pinCount--
	}
}

// PinCount returns the current pin count.
// Used by the buffer pool to check if a page is safe to evict.
func (p *Page) PinCount() int {
	return p.pinCount
}

// String provides a human-readable summary for debugging.
func (p *Page) String() string {
	return fmt.Sprintf("Page{ID: %d, dirty: %v, pinCount: %d}", p.ID, p.dirty, p.pinCount)
}
