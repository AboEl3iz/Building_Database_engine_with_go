package engine

import (
	"encoding/binary"
	"fmt"
	"minidb/internal/buffer"
	"minidb/internal/disk"
	"sync"
)

// RowStore stores var-len rows in a sequence of fixed-size buffer pool pages.
// It is an append-only heap.
// We use the first page as a header to track the last page and its free offset.
type RowStore struct {
	mu     sync.Mutex
	bp     *buffer.BufferPool
	header disk.PageID
}

// rowStoreHeader is the in-memory representation of the header page.
type rowStoreHeader struct {
	LastPageID disk.PageID // 4 bytes
	FreeOffset uint16      // 2 bytes (offset within LastPageID)
}

func readHeader(page *disk.Page) rowStoreHeader {
	data := page.Data
	hdr := rowStoreHeader{
		LastPageID: disk.PageID(binary.LittleEndian.Uint32(data[0:4])),
		FreeOffset: binary.LittleEndian.Uint16(data[4:6]),
	}
	return hdr
}

func writeHeader(page *disk.Page, hdr rowStoreHeader) {
	data := page.Data
	binary.LittleEndian.PutUint32(data[:4], uint32(hdr.LastPageID))
	binary.LittleEndian.PutUint16(data[4:6], hdr.FreeOffset)
	copy(page.Data[:4], data[:4])
	copy(page.Data[4:6], data[4:6])
}

// NewRowStore allocates a new row store (for a new table).
func NewRowStore(bp *buffer.BufferPool) (*RowStore, disk.PageID, error) {
	headerPage, err := bp.NewPage()
	if err != nil {
		return nil, disk.InvalidPageID, err
	}
	defer bp.UnpinPage(headerPage.ID, true)

	// Allocate the first data page
	firstData, err := bp.NewPage()
	if err != nil {
		return nil, disk.InvalidPageID, err
	}
	defer bp.UnpinPage(firstData.ID, true)

	hdr := rowStoreHeader{
		LastPageID: firstData.ID,
		FreeOffset: 0,
	}
	writeHeader(headerPage, hdr)

	rs := &RowStore{
		bp:     bp,
		header: headerPage.ID,
	}
	return rs, headerPage.ID, nil
}

// OpenRowStore opens an existing row store given its header page ID.
func OpenRowStore(bp *buffer.BufferPool, header disk.PageID) *RowStore {
	return &RowStore{
		bp:     bp,
		header: header,
	}
}

// Append writes a new row and returns its global pseudo-offset (int64).
// The returned offset encodes both the PageID and the position within the page.
// Format: [PageID (uint32)] [SlotOffset (uint16)]
func (rs *RowStore) Append(data []byte) (int64, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	headPage, err := rs.bp.FetchPage(rs.header)
	if err != nil {
		return 0, err
	}
	defer rs.bp.UnpinPage(rs.header, true)
	hdr := readHeader(headPage)

	dataLen := uint16(len(data))
	// 2 bytes for length prefix + data
	totalLen := 2 + dataLen

	// If it doesn't fit in the current page, allocate a new one.
	if hdr.FreeOffset+totalLen > disk.PageSize {
		// Unpin header temporarily to free up a frame if pool is small
		rs.bp.UnpinPage(rs.header, true)
		
		newPage, err := rs.bp.NewPage()
		if err != nil {
			// Re-pin header so defer works
			rs.bp.FetchPage(rs.header)
			return 0, err
		}
		
		// Re-pin header
		headPage, _ = rs.bp.FetchPage(rs.header)
		hdr.LastPageID = newPage.ID
		hdr.FreeOffset = 0
		writeHeader(headPage, hdr)
		
		// Unpin the newly created page since we will fetch it below
		rs.bp.UnpinPage(newPage.ID, true)
	}

	dataPage, err := rs.bp.FetchPage(hdr.LastPageID)
	if err != nil {
		return 0, err
	}
	defer rs.bp.UnpinPage(hdr.LastPageID, true)

	// Write length prefix and data
	binary.LittleEndian.PutUint16(dataPage.Data[hdr.FreeOffset:], dataLen)
	copy(dataPage.Data[hdr.FreeOffset+2:], data)

	// Combine PageID and Offset into a single int64
	offsetEncoded := (int64(hdr.LastPageID) << 32) | int64(hdr.FreeOffset)

	// Update header
	hdr.FreeOffset += totalLen
	writeHeader(headPage, hdr)

	return offsetEncoded, nil
}

// Read fetches the row bytes given its encoded offset.
func (rs *RowStore) Read(encoded int64) ([]byte, error) {
	pageID := disk.PageID(encoded >> 32)
	offset := uint16(encoded & 0xFFFF)

	page, err := rs.bp.FetchPage(pageID)
	if err != nil {
		return nil, err
	}
	defer rs.bp.UnpinPage(pageID, false)

	pageData := page.Data
	if offset+2 > disk.PageSize {
		return nil, fmt.Errorf("rowstore: invalid offset %d", offset)
	}
	dataLen := binary.LittleEndian.Uint16(pageData[offset:])
	
	if offset+2+dataLen > disk.PageSize {
		return nil, fmt.Errorf("rowstore: invalid length %d at offset %d", dataLen, offset)
	}

	// Make a copy so caller doesn't retain reference to buffer pool page
	rowBytes := make([]byte, dataLen)
	copy(rowBytes, pageData[offset+2:offset+2+dataLen])
	return rowBytes, nil
}
