// Package buffer implements the Buffer Pool Manager.
//
// CONCEPT: Why a buffer pool?
// Disk I/O is ~1,000,000x slower than memory access. A database that read from
// disk on every query would be unusably slow. The buffer pool is an in-memory
// cache that keeps "hot" pages in RAM and only goes to disk when necessary.
//
// Think of it like browser caching: the browser caches recently-visited pages
// so it doesn't re-download them. The buffer pool caches recently-used DB pages.
//
// Key operations:
//   - FetchPage(id): get a page into memory (from cache or disk)
//   - UnpinPage(id, dirty): say "I'm done using this page"
//   - FlushPage(id): force-write a specific page to disk
//
// Eviction policy: LRU (Least Recently Used)
// When the pool is full and we need a new frame, we evict the page that was
// accessed least recently (and isn't pinned by anyone).
package buffer

import (
	"container/list"
	"fmt"
	"minidb/internal/disk"
	"sync"
)

// FrameID identifies a slot (frame) in the buffer pool's memory array.
// The pool has a fixed number of frames; pages are loaded into frames.
//
//	Buffer Pool memory:
//	┌─────────┬─────────┬─────────┬─────────┐
//	│ Frame 0 │ Frame 1 │ Frame 2 │ Frame 3 │  (poolSize = 4)
//	│ Page 5  │ Page 12 │ <empty> │ Page 1  │
//	└─────────┴─────────┴─────────┴─────────┘
type FrameID int

// BufferPool manages a fixed set of in-memory frames holding disk pages.
//
// Fields:
//   - pages: the actual memory — a fixed array of Page pointers (one per frame)
//   - pageTable: maps PageID → FrameID so we can find pages in memory fast
//   - freeList: frame IDs that are currently empty (no page loaded)
//   - lruList: doubly-linked list ordered by recency (front=most recent)
//   - lruMap: maps FrameID → list element for O(1) access to LRU position
type BufferPool struct {
	mu        sync.RWMutex
	dm        *disk.DiskManager
	poolSize  int
	pages     []*disk.Page              // index = FrameID
	pageTable map[disk.PageID]FrameID   // PageID → FrameID
	freeList  []FrameID                 // available (empty) frames
	lruList   *list.List                // LRU order (front = most recently used)
	lruMap    map[FrameID]*list.Element // FrameID → LRU list element (for O(1) removal)
}

// NewBufferPool creates a buffer pool with `poolSize` frames backed by `dm`.
//
// poolSize is the maximum number of pages kept in memory simultaneously.
// A larger pool means fewer disk reads (better hit rate) but uses more RAM.
// Real databases use hundreds of MB to GBs for the buffer pool.
func NewBufferPool(poolSize int, dm *disk.DiskManager) *BufferPool {
	bp := &BufferPool{
		dm:        dm,
		poolSize:  poolSize,
		pages:     make([]*disk.Page, poolSize),
		pageTable: make(map[disk.PageID]FrameID),
		freeList:  make([]FrameID, poolSize),
		lruList:   list.New(),
		lruMap:    make(map[FrameID]*list.Element),
	}

	// Initially, all frames are free.
	// Initialize each frame slot to nil (no page loaded).
	for i := 0; i < poolSize; i++ {
		bp.freeList[i] = FrameID(i)
		bp.pages[i] = nil
	}

	return bp
}

// FetchPage returns the requested page, loading it from disk if necessary.
//
// Algorithm:
//  1. If the page is already in memory (cache hit) → return it, move to LRU front
//  2. If not in memory (cache miss):
//     a. Find a free frame OR evict the LRU page
//     b. If evicting a dirty page, flush it to disk first
//     c. Load the requested page from disk into the chosen frame
//     d. Update pageTable and LRU tracking
//
// The returned page is pinned (pinCount incremented). Callers MUST call
// UnpinPage when done, or the page can never be evicted (memory leak).
func (bp *BufferPool) FetchPage(id disk.PageID) (*disk.Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// --- Cache Hit ---
	// Page is already in memory: just update LRU and return it
	if frameID, ok := bp.pageTable[id]; ok {
		page := bp.pages[frameID]
		page.Pin()
		bp.moveToFront(frameID) // mark as recently used
		return page, nil
	}

	// --- Cache Miss ---
	// We need to load the page. First find a frame to use.
	frameID, err := bp.findFrame()
	if err != nil {
		return nil, fmt.Errorf("buffer: cannot find frame for page %d: %w", id, err)
	}

	// Load the page from disk into the chosen frame
	page := disk.NewPage(id)
	if err := bp.dm.ReadPage(id, page); err != nil {
		// Put the frame back into the free list since we didn't use it
		bp.freeList = append(bp.freeList, frameID)
		return nil, fmt.Errorf("buffer: cannot read page %d from disk: %w", id, err)
	}

	// Install the new page in the frame
	bp.pages[frameID] = page
	bp.pageTable[id] = frameID
	page.Pin()
	bp.addToFront(frameID) // most recently used

	return page, nil
}

// NewPage allocates a new page on disk and loads it into the buffer pool.
//
// Use this when you need a fresh empty page (e.g., allocating a new B+ tree node).
// The returned page has all bytes zeroed and is pinned.
func (bp *BufferPool) NewPage() (*disk.Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Find a free or evictable frame first, before allocating on disk.
	// We don't want to allocate a page ID and then fail to find a frame.
	frameID, err := bp.findFrame()
	if err != nil {
		return nil, fmt.Errorf("buffer: pool full, cannot allocate new page: %w", err)
	}

	// Allocate a new page ID on disk (extends the file with zeros)
	pageID, err := bp.dm.AllocatePage()
	if err != nil {
		bp.freeList = append(bp.freeList, frameID)
		return nil, fmt.Errorf("buffer: disk allocation failed: %w", err)
	}

	// Create the in-memory page (all zeros — ready to be written to)
	page := disk.NewPage(pageID)
	bp.pages[frameID] = page
	bp.pageTable[pageID] = frameID
	page.Pin()
	bp.addToFront(frameID)

	return page, nil
}

// UnpinPage decrements the pin count of a page.
//
// Call this when you're done using a page. If `isDirty` is true, the page
// will be flushed to disk before eviction.
//
// Forgetting to call UnpinPage is like forgetting free() in C — it causes
// the buffer pool to fill up and eventually no pages can be loaded.
func (bp *BufferPool) UnpinPage(id disk.PageID, isDirty bool) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	frameID, ok := bp.pageTable[id]
	if !ok {
		return fmt.Errorf("buffer: page %d is not in buffer pool", id)
	}

	page := bp.pages[frameID]
	if isDirty {
		page.MarkDirty()
	}
	page.Unpin()

	return nil
}

// FlushPage forces a page to be written to disk immediately.
//
// Use this when you need to guarantee durability before proceeding
// (e.g., after writing a WAL checkpoint).
func (bp *BufferPool) FlushPage(id disk.PageID) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	frameID, ok := bp.pageTable[id]
	if !ok {
		return fmt.Errorf("buffer: page %d not in pool", id)
	}

	page := bp.pages[frameID]
	if !page.IsDirty() {
		return nil // nothing to flush
	}

	if err := bp.dm.WritePage(page); err != nil {
		return fmt.Errorf("buffer: flush failed for page %d: %w", id, err)
	}

	return nil
}

// FlushAll writes all dirty pages to disk.
// Used during shutdown to ensure no data is lost.
func (bp *BufferPool) FlushAll() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for id, frameID := range bp.pageTable {
		page := bp.pages[frameID]
		if page.IsDirty() {
			if err := bp.dm.WritePage(page); err != nil {
				return fmt.Errorf("buffer: flush all failed for page %d: %w", id, err)
			}
		}
	}
	return nil
}

// PoolSize returns the total number of frames in the pool.
func (bp *BufferPool) PoolSize() int {
	return bp.poolSize
}

// FreeFrames returns how many frames are currently unused.
func (bp *BufferPool) FreeFrames() int {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return len(bp.freeList)
}

// ---- Internal helpers (no locking — callers hold bp.mu) ----

// findFrame returns a FrameID to use for loading a new page.
//
// Priority:
//  1. Use a free frame if available (no disk I/O needed)
//  2. Evict the LRU unpinned page (may require flushing if dirty)
//
// If all frames are pinned, returns an error.
func (bp *BufferPool) findFrame() (FrameID, error) {
	// Fast path: grab a free frame
	if len(bp.freeList) > 0 {
		frameID := bp.freeList[len(bp.freeList)-1]
		bp.freeList = bp.freeList[:len(bp.freeList)-1]
		return frameID, nil
	}

	// Slow path: evict the least recently used unpinned page
	return bp.evictLRU()
}

// evictLRU finds the LRU page that isn't pinned and evicts it.
//
// LRU list: front = most recently used, back = least recently used
// We scan from the back to find an evictable (pinCount == 0) page.
func (bp *BufferPool) evictLRU() (FrameID, error) {
	// Walk from least-recently-used (back) toward most-recently-used (front)
	for elem := bp.lruList.Back(); elem != nil; elem = elem.Prev() {
		frameID := elem.Value.(FrameID)
		page := bp.pages[frameID]

		if page == nil || page.PinCount() > 0 {
			continue // skip pinned pages
		}

		// Found an evictable page — flush if dirty
		if page.IsDirty() {
			if err := bp.dm.WritePage(page); err != nil {
				return 0, fmt.Errorf("buffer: eviction flush failed for page %d: %w", page.ID, err)
			}
		}

		// Remove from tracking structures
		delete(bp.pageTable, page.ID)
		bp.lruList.Remove(elem)
		delete(bp.lruMap, frameID)
		bp.pages[frameID] = nil

		return frameID, nil
	}

	return 0, fmt.Errorf("buffer: all %d frames are pinned, cannot evict", bp.poolSize)
}

// addToFront adds a frame to the front of the LRU list (most recently used).
func (bp *BufferPool) addToFront(frameID FrameID) {
	elem := bp.lruList.PushFront(frameID)
	bp.lruMap[frameID] = elem
}

// moveToFront moves an existing frame to the front of the LRU list.
// Called on cache hits to update recency.
func (bp *BufferPool) moveToFront(frameID FrameID) {
	if elem, ok := bp.lruMap[frameID]; ok {
		bp.lruList.MoveToFront(elem)
	}
}
