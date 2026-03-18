package disk

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

// DiskManager handles all raw file I/O for the database.
//
// CONCEPT: How database files work
// A .db file is just a flat binary file divided into fixed-size pages.
// Page 0 lives at byte offset 0..4095, Page 1 at 4096..8191, etc.
//
//	File layout:
//	┌──────────────┬──────────────┬──────────────┬────
//	│   Page 0     │   Page 1     │   Page 2     │ ...
//	│  (4096 B)    │  (4096 B)    │  (4096 B)    │
//	└──────────────┴──────────────┴──────────────┴────
//
// The DiskManager is the ONLY component that touches the actual file.
// Everything else (buffer pool, B+ tree) works through the DiskManager.
type DiskManager struct {
	mu          sync.Mutex // protects file access in concurrent scenarios
	file        *os.File   // the open .db file handle
	filePath    string     // path to the .db file (for error messages)
	numPages    uint32     // total number of pages currently allocated
	nextPageID  PageID     // the next page ID to allocate
}

// NewDiskManager opens (or creates) a database file and returns a DiskManager.
//
// If the file already exists, it reads the header to find how many pages
// are already allocated. If it's new, it starts empty.
func NewDiskManager(filePath string) (*DiskManager, error) {
	// os.O_RDWR = read+write, os.O_CREATE = create if not exists
	// 0666 = file permissions (owner/group/other can read+write)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("disk: cannot open file %q: %w", filePath, err)
	}

	// Determine how many pages already exist by checking file size.
	// fileSize / PageSize = number of complete pages on disk.
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("disk: cannot stat file %q: %w", filePath, err)
	}

	numPages := uint32(info.Size() / PageSize)

	dm := &DiskManager{
		file:       file,
		filePath:   filePath,
		numPages:   numPages,
		nextPageID: PageID(numPages), // next alloc continues from end of file
	}

	return dm, nil
}

// ReadPage reads a page from disk into the given Page struct.
//
// How it works:
//  1. Calculate the byte offset: offset = pageID * PageSize
//  2. Seek to that offset in the file
//  3. Read exactly PageSize (4096) bytes into page.Data
//
// Returns an error if the page doesn't exist yet (ID >= numPages).
func (dm *DiskManager) ReadPage(id PageID, page *Page) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if uint32(id) >= dm.numPages {
		return fmt.Errorf("disk: page %d does not exist (file has %d pages)", id, dm.numPages)
	}

	// Calculate byte offset in the file
	offset := int64(id) * PageSize

	// ReadAt reads len(buf) bytes starting at the given offset.
	// Unlike Read, it doesn't change the file's seek position — safe for concurrent use.
	n, err := dm.file.ReadAt(page.Data[:], offset)
	if err != nil {
		return fmt.Errorf("disk: error reading page %d: %w", id, err)
	}
	if n != PageSize {
		return fmt.Errorf("disk: short read on page %d: got %d bytes, want %d", id, n, PageSize)
	}

	page.ID = id
	return nil
}

// WritePage writes a page's Data to disk at the correct offset.
//
// After writing, it calls Sync to ensure the OS has flushed its write buffers
// to the actual storage device. Without Sync, a crash might lose the write.
//
// IMPORTANT: In production databases, calling Sync on every write is very slow.
// Real DBs batch writes and use the WAL to ensure durability instead.
// For MiniDB, we keep it simple with per-write Sync.
func (dm *DiskManager) WritePage(page *Page) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	offset := int64(page.ID) * PageSize

	// WriteAt writes at a specific offset, safe for concurrent use
	n, err := dm.file.WriteAt(page.Data[:], offset)
	if err != nil {
		return fmt.Errorf("disk: error writing page %d: %w", page.ID, err)
	}
	if n != PageSize {
		return fmt.Errorf("disk: short write on page %d: wrote %d bytes, want %d", page.ID, n, PageSize)
	}

	// Sync forces OS kernel buffers to flush to disk.
	// This ensures durability: if we return nil, the data is safely on disk.
	// (Expensive! ~1–10ms per call. Real DBs batch this with group commit.)
	if err := dm.file.Sync(); err != nil {
		return fmt.Errorf("disk: sync failed for page %d: %w", page.ID, err)
	}

	// Update page count if we wrote beyond the current end of file
	if uint32(page.ID) >= dm.numPages {
		dm.numPages = uint32(page.ID) + 1
	}

	page.ClearDirty() // page is now in sync with disk
	return nil
}

// AllocatePage reserves a new page ID and extends the file.
//
// CONCEPT: Logical vs Physical allocation
// AllocatePage only reserves the ID — it doesn't write anything to disk yet.
// The caller (buffer pool) will create an in-memory Page for this ID.
// The page only hits disk when the buffer pool flushes it (WritePage).
//
// This lazy approach avoids disk I/O on every allocation.
func (dm *DiskManager) AllocatePage() (PageID, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	id := dm.nextPageID
	dm.nextPageID++

	// Pre-extend the file with zeroed bytes so ReadPage can succeed later.
	// Without this, ReadPage on a new page would get an EOF error.
	offset := int64(id) * PageSize
	zeros := make([]byte, PageSize)
	_, err := dm.file.WriteAt(zeros, offset)
	if err != nil {
		return 0, fmt.Errorf("disk: cannot extend file for page %d: %w", id, err)
	}

	dm.numPages++
	return id, nil
}

// NumPages returns how many pages have been allocated in this database file.
func (dm *DiskManager) NumPages() uint32 {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.numPages
}

// Close flushes and closes the underlying file.
// Must be called when shutting down to avoid resource leaks.
func (dm *DiskManager) Close() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if err := dm.file.Sync(); err != nil {
		return fmt.Errorf("disk: sync on close failed: %w", err)
	}
	return dm.file.Close()
}

// ---- Page Header Helpers ----
// These helpers let higher-level code (B+ tree, WAL) store metadata in
// the first few bytes of a page. We use a simple convention:
//   - Bytes 0–1: page "type" tag (so we know what's in this page)
//   - Bytes 2–3: reserved
//   - Bytes 4+: payload written by the caller

// PageType identifies what kind of data is stored in a page.
type PageType uint16

const (
	PageTypeUnknown  PageType = 0
	PageTypeBTreeNode PageType = 1
	PageTypeWAL      PageType = 2
	PageTypeCatalog  PageType = 3
)

// WritePageType writes the page type tag into the first 2 bytes of page.Data.
// Call this right after AllocatePage to mark what this page is for.
func WritePageType(page *Page, t PageType) {
	binary.LittleEndian.PutUint16(page.Data[0:2], uint16(t))
}

// ReadPageType reads the page type tag from the first 2 bytes.
func ReadPageType(page *Page) PageType {
	return PageType(binary.LittleEndian.Uint16(page.Data[0:2]))
}
