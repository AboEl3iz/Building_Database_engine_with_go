// Package wal implements the Write-Ahead Log (WAL) for crash recovery.
//
// CONCEPT: Why Write-Ahead Logging?
// Without WAL, a crash mid-write could leave the database in an inconsistent
// state (e.g., a B+ tree split that completed for the left child but not the right).
//
// WAL guarantees ACID durability through this protocol:
//   "Before modifying any page, write a log record describing the change."
//
// On crash + restart, we:
//   1. REDO all committed transactions (replay log forward)
//   2. UNDO all uncommitted transactions (replay log backward)
//
// This is a simplified ARIES (Algorithm for Recovery and Isolation Exploiting
// Semantics) implementation. Real ARIES has additional complexity like
// compensation log records (CLRs) and the fuzzy checkpoint protocol.
package wal

import (
	"encoding/binary"
	"fmt"
	"minidb/internal/disk"
)

// LSN (Log Sequence Number) uniquely identifies a log record.
// LSNs are monotonically increasing — a higher LSN = a later record.
//
// Real databases store the LSN of the last log record that modified each page
// (pageLSN). During recovery, if pageLSN >= record LSN, the change is already
// applied and we skip the redo. For MiniDB, we simplify and always redo.
type LSN uint64

// InvalidLSN represents "no log record" (like a null pointer for LSNs).
const InvalidLSN LSN = 0

// TxID identifies a transaction.
// Multiple log records can share the same TxID — they're part of the same transaction.
type TxID uint64

// RecordType identifies what kind of operation this log record describes.
type RecordType uint8

const (
	// RecordBegin marks the start of a transaction.
	// Written when BEGIN is called. Used to identify active txns on crash.
	RecordBegin RecordType = 1

	// RecordInsert records an insert operation.
	// Contains: pageID, key, value (before: nothing, after: new entry)
	RecordInsert RecordType = 2

	// RecordUpdate records an update operation.
	// Contains: pageID, key, old value, new value (for both redo and undo)
	RecordUpdate RecordType = 3

	// RecordDelete records a delete operation.
	// Contains: pageID, key, old value (so we can undo the delete)
	RecordDelete RecordType = 4

	// RecordCommit marks a transaction as successfully completed.
	// Once this record is durably written to disk, the transaction is committed.
	// Data changes will be redone after a crash (even if pages weren't flushed).
	RecordCommit RecordType = 5

	// RecordAbort marks a transaction as rolled back.
	// The undo pass will reverse all changes made by this transaction.
	RecordAbort RecordType = 6

	// RecordCheckpoint captures a consistent snapshot of the database state.
	// Limits how far back recovery needs to scan the log.
	// Contains: list of active transactions and dirty pages at checkpoint time.
	RecordCheckpoint RecordType = 7
)

// recordTypeNames maps RecordType to a human-readable string for debugging.
var recordTypeNames = map[RecordType]string{
	RecordBegin:      "BEGIN",
	RecordInsert:     "INSERT",
	RecordUpdate:     "UPDATE",
	RecordDelete:     "DELETE",
	RecordCommit:     "COMMIT",
	RecordAbort:      "ABORT",
	RecordCheckpoint: "CHECKPOINT",
}

func (r RecordType) String() string {
	if s, ok := recordTypeNames[r]; ok {
		return s
	}
	return fmt.Sprintf("UNKNOWN(%d)", r)
}

// LogRecord is one entry in the write-ahead log.
//
// Binary format on disk:
//
//	┌──────┬──────┬──────┬──────────┬──────────┬──────────┬────────┐
//	│ Size │ LSN  │ TxID │  Type    │  PageID  │ PrevLSN  │  Data  │
//	│  4B  │  8B  │  8B  │   1B     │    4B    │   8B     │ variable│
//	└──────┴──────┴──────┴──────────┴──────────┴──────────┴────────┘
//
// Total fixed header = 4 + 8 + 8 + 1 + 4 + 8 = 33 bytes
// Then variable-length Data follows.
//
// Size field (first 4 bytes): total record size including the Size field itself.
// This lets us read records sequentially: read 4 bytes → know total record length.
type LogRecord struct {
	// LSN is assigned by the WAL when this record is appended.
	// It's the position (byte offset) of this record in the log file.
	LSN LSN

	// TxID identifies which transaction wrote this record.
	TxID TxID

	// Type describes the operation (BEGIN, INSERT, COMMIT, etc.)
	Type RecordType

	// PageID is the page that was modified (0 for BEGIN/COMMIT/ABORT).
	PageID disk.PageID

	// PrevLSN is the LSN of the previous log record for this same transaction.
	// Forms a per-transaction chain for fast undo traversal.
	// (During undo, we follow PrevLSN links backward through a transaction's records.)
	PrevLSN LSN

	// Data is operation-specific payload:
	//   INSERT: [key(8B) | value(8B)]
	//   UPDATE: [key(8B) | oldValue(8B) | newValue(8B)]
	//   DELETE: [key(8B) | oldValue(8B)]
	//   CHECKPOINT: [numActiveTxns(4B) | txnIDs... | numDirtyPages(4B) | pageIDs...]
	Data []byte
}

// HeaderSize is the fixed size of a log record header (without Data).
const HeaderSize = 4 + 8 + 8 + 1 + 4 + 8 // size + LSN + TxID + Type + PageID + PrevLSN = 33

// Serialize encodes a LogRecord into bytes for writing to the log file.
//
// Format: [totalSize(4B)] [LSN(8B)] [TxID(8B)] [Type(1B)] [PageID(4B)] [PrevLSN(8B)] [Data...]
func (r *LogRecord) Serialize() []byte {
	totalSize := HeaderSize + len(r.Data)
	buf := make([]byte, totalSize)

	offset := 0
	binary.LittleEndian.PutUint32(buf[offset:], uint32(totalSize))
	offset += 4

	binary.LittleEndian.PutUint64(buf[offset:], uint64(r.LSN))
	offset += 8

	binary.LittleEndian.PutUint64(buf[offset:], uint64(r.TxID))
	offset += 8

	buf[offset] = byte(r.Type)
	offset++

	binary.LittleEndian.PutUint32(buf[offset:], uint32(r.PageID))
	offset += 4

	binary.LittleEndian.PutUint64(buf[offset:], uint64(r.PrevLSN))
	offset += 8

	copy(buf[offset:], r.Data)

	return buf
}

// DeserializeRecord decodes a LogRecord from raw bytes.
// `buf` should start with the 4-byte size field.
func DeserializeRecord(buf []byte) (*LogRecord, error) {
	if len(buf) < HeaderSize {
		return nil, fmt.Errorf("wal: record too short: %d bytes (min %d)", len(buf), HeaderSize)
	}

	offset := 0
	totalSize := binary.LittleEndian.Uint32(buf[offset:])
	offset += 4

	if int(totalSize) > len(buf) {
		return nil, fmt.Errorf("wal: record claims size %d but buffer has %d bytes", totalSize, len(buf))
	}

	r := &LogRecord{}

	r.LSN = LSN(binary.LittleEndian.Uint64(buf[offset:]))
	offset += 8

	r.TxID = TxID(binary.LittleEndian.Uint64(buf[offset:]))
	offset += 8

	r.Type = RecordType(buf[offset])
	offset++

	r.PageID = disk.PageID(binary.LittleEndian.Uint32(buf[offset:]))
	offset += 4

	r.PrevLSN = LSN(binary.LittleEndian.Uint64(buf[offset:]))
	offset += 8

	dataLen := int(totalSize) - HeaderSize
	if dataLen > 0 {
		r.Data = make([]byte, dataLen)
		copy(r.Data, buf[offset:offset+dataLen])
	}

	return r, nil
}

// String returns a human-readable representation of the log record.
func (r *LogRecord) String() string {
	return fmt.Sprintf("LogRecord{LSN=%d, TxID=%d, Type=%s, PageID=%d, PrevLSN=%d, DataLen=%d}",
		r.LSN, r.TxID, r.Type, r.PageID, r.PrevLSN, len(r.Data))
}

// ---- Data payload helpers ----
// These helpers encode/decode the Data field for specific record types.

// NewInsertData encodes INSERT payload: key + value
func NewInsertData(key, value int64) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:], uint64(key))
	binary.LittleEndian.PutUint64(buf[8:], uint64(value))
	return buf
}

// ParseInsertData decodes INSERT payload.
func ParseInsertData(data []byte) (key, value int64, err error) {
	if len(data) < 16 {
		return 0, 0, fmt.Errorf("wal: INSERT data too short")
	}
	key = int64(binary.LittleEndian.Uint64(data[0:]))
	value = int64(binary.LittleEndian.Uint64(data[8:]))
	return key, value, nil
}

// NewUpdateData encodes UPDATE payload: key + oldValue + newValue
func NewUpdateData(key, oldValue, newValue int64) []byte {
	buf := make([]byte, 24)
	binary.LittleEndian.PutUint64(buf[0:], uint64(key))
	binary.LittleEndian.PutUint64(buf[8:], uint64(oldValue))
	binary.LittleEndian.PutUint64(buf[16:], uint64(newValue))
	return buf
}

// ParseUpdateData decodes UPDATE payload.
func ParseUpdateData(data []byte) (key, oldValue, newValue int64, err error) {
	if len(data) < 24 {
		return 0, 0, 0, fmt.Errorf("wal: UPDATE data too short")
	}
	key = int64(binary.LittleEndian.Uint64(data[0:]))
	oldValue = int64(binary.LittleEndian.Uint64(data[8:]))
	newValue = int64(binary.LittleEndian.Uint64(data[16:]))
	return key, oldValue, newValue, nil
}

// NewDeleteData encodes DELETE payload: key + old value (for undo)
func NewDeleteData(key, oldValue int64) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:], uint64(key))
	binary.LittleEndian.PutUint64(buf[8:], uint64(oldValue))
	return buf
}

// ParseDeleteData decodes DELETE payload.
func ParseDeleteData(data []byte) (key, oldValue int64, err error) {
	if len(data) < 16 {
		return 0, 0, fmt.Errorf("wal: DELETE data too short")
	}
	key = int64(binary.LittleEndian.Uint64(data[0:]))
	oldValue = int64(binary.LittleEndian.Uint64(data[8:]))
	return key, oldValue, nil
}
