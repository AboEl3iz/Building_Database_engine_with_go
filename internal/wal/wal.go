package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"minidb/internal/btree"
	"minidb/internal/disk"
	"os"
	"sync"
	"sync/atomic"
)

// WAL is the Write-Ahead Log manager.
//
// The WAL file is an APPEND-ONLY sequential file. Records are never overwritten
// or deleted during normal operation. Only during checkpointing can old records
// be truncated (not implemented here — we keep the full log for simplicity).
//
// ACID guarantees from WAL:
//   A (Atomicity):   ABORT records + undo pass ensures partial txns are rolled back
//   C (Consistency): enforced by application logic (constraints, etc.)
//   I (Isolation):   not handled by WAL (needs lock manager — future work)
//   D (Durability):  COMMIT record flushed to disk = transaction is durable
//
// WAL Rule 1 (Write-Ahead): Log record must be on disk BEFORE the modified page.
// WAL Rule 2 (Force-on-Commit): All log records for a txn must be on disk before
//             responding "committed" to the client.
type WAL struct {
	mu          sync.Mutex
	file        *os.File
	nextLSN     uint64   // used atomically — next LSN to assign
	lastFlushed LSN      // highest LSN safely on disk

	// txnTable tracks active transactions: TxID → last LSN for that txn
	// Used during recovery (analysis pass) and for building undo chains.
	txnTable map[TxID]LSN
}

// NewWAL opens (or creates) a WAL file.
// If the file exists, it reads through it to rebuild state (e.g., nextLSN).
func NewWAL(filePath string) (*WAL, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("wal: cannot open log file %q: %w", filePath, err)
	}

	w := &WAL{
		file:     file,
		txnTable: make(map[TxID]LSN),
	}

	// Scan the existing log to find the highest LSN (so nextLSN continues from there)
	maxLSN, err := w.scanForMaxLSN()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("wal: failed to scan existing log: %w", err)
	}

	// nextLSN starts after the last known LSN
	atomic.StoreUint64(&w.nextLSN, uint64(maxLSN+1))

	return w, nil
}

// ---- Writing ----

// Begin writes a BEGIN record for a new transaction.
// Returns the TxID assigned to this transaction.
func (w *WAL) Begin() (TxID, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	txID := TxID(atomic.AddUint64(&w.nextLSN, 0)) // use current nextLSN as TxID seed

	rec := &LogRecord{
		TxID:    txID,
		Type:    RecordBegin,
		PrevLSN: InvalidLSN,
	}

	lsn, err := w.appendRecord(rec)
	if err != nil {
		return 0, err
	}

	// Track this transaction's last LSN
	w.txnTable[txID] = lsn
	return txID, nil
}

// LogInsert writes an INSERT log record before the actual B+ tree insert.
// Call this BEFORE calling btree.Insert(key, value).
func (w *WAL) LogInsert(txID TxID, pageID disk.PageID, key, value int64) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &LogRecord{
		TxID:    txID,
		Type:    RecordInsert,
		PageID:  pageID,
		PrevLSN: w.txnTable[txID], // chain to previous record for this txn
		Data:    NewInsertData(key, value),
	}

	lsn, err := w.appendRecord(rec)
	if err != nil {
		return InvalidLSN, err
	}

	w.txnTable[txID] = lsn
	return lsn, nil
}

// LogUpdate writes an UPDATE log record.
// oldValue is needed for UNDO; newValue is needed for REDO.
func (w *WAL) LogUpdate(txID TxID, pageID disk.PageID, key, oldValue, newValue int64) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &LogRecord{
		TxID:    txID,
		Type:    RecordUpdate,
		PageID:  pageID,
		PrevLSN: w.txnTable[txID],
		Data:    NewUpdateData(key, oldValue, newValue),
	}

	lsn, err := w.appendRecord(rec)
	if err != nil {
		return InvalidLSN, err
	}

	w.txnTable[txID] = lsn
	return lsn, nil
}

// LogDelete writes a DELETE log record.
func (w *WAL) LogDelete(txID TxID, pageID disk.PageID, key, oldValue int64) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &LogRecord{
		TxID:    txID,
		Type:    RecordDelete,
		PageID:  pageID,
		PrevLSN: w.txnTable[txID],
		Data:    NewDeleteData(key, oldValue),
	}

	lsn, err := w.appendRecord(rec)
	if err != nil {
		return InvalidLSN, err
	}

	w.txnTable[txID] = lsn
	return lsn, nil
}

// Commit writes a COMMIT record and forces it to disk.
// After this returns successfully, the transaction is durably committed.
//
// WAL Rule 2: all log records for this txn must be on disk before we return.
// We call file.Sync() to ensure the OS has flushed to physical storage.
func (w *WAL) Commit(txID TxID) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &LogRecord{
		TxID:    txID,
		Type:    RecordCommit,
		PrevLSN: w.txnTable[txID],
	}

	lsn, err := w.appendRecord(rec)
	if err != nil {
		return err
	}

	// CRITICAL: Force the log to disk before returning "committed"
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: commit sync failed for txn %d: %w", txID, err)
	}

	w.lastFlushed = lsn

	// Remove from active transaction table — transaction is done
	delete(w.txnTable, txID)
	return nil
}

// Abort writes an ABORT record and rolls back the transaction's changes.
func (w *WAL) Abort(txID TxID, tree *btree.BTree) error {
	w.mu.Lock()

	rec := &LogRecord{
		TxID:    txID,
		Type:    RecordAbort,
		PrevLSN: w.txnTable[txID],
	}

	_, err := w.appendRecord(rec)
	w.mu.Unlock()

	if err != nil {
		return err
	}

	// Undo all changes made by this transaction
	return w.undoTransaction(txID, tree)
}

// ---- Reading ----

// ReadFrom reads all log records with LSN >= fromLSN.
// Used during recovery to replay the log.
func (w *WAL) ReadFrom(fromLSN LSN) ([]*LogRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to start of file (we scan from the beginning and filter by LSN)
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("wal: seek failed: %w", err)
	}

	var records []*LogRecord
	buf := make([]byte, HeaderSize)

	for {
		// Read the fixed header first to get the record size
		_, err := io.ReadFull(w.file, buf[:4]) // read size field
		if err == io.EOF {
			break // end of log
		}
		if err != nil {
			return nil, fmt.Errorf("wal: read size field failed: %w", err)
		}

		totalSize := binary.LittleEndian.Uint32(buf[:4])
		if totalSize < uint32(HeaderSize) {
			break // corrupted or truncated record
		}

		// Read the full record
		recBuf := make([]byte, totalSize)
		copy(recBuf[:4], buf[:4])
		if _, err := io.ReadFull(w.file, recBuf[4:]); err != nil {
			return nil, fmt.Errorf("wal: read record body failed: %w", err)
		}

		rec, err := DeserializeRecord(recBuf)
		if err != nil {
			return nil, fmt.Errorf("wal: deserialize failed: %w", err)
		}

		if rec.LSN >= fromLSN {
			records = append(records, rec)
		}
	}

	return records, nil
}

// ---- Recovery (ARIES simplified) ----

// Recover performs crash recovery using the WAL.
// Should be called at startup BEFORE accepting any new transactions.
//
// Three-pass ARIES recovery:
//
//  1. Analysis: scan log from start to find:
//     - Which transactions were active at crash (not committed/aborted)
//     - Which pages were dirty (modified but not flushed)
//
//  2. Redo: replay all log records from the oldest dirty page's LSN forward.
//     This re-applies changes that were in the log but maybe not on disk.
//
//  3. Undo: for each uncommitted transaction (active at crash), undo its
//     changes in reverse order (following PrevLSN chain backwards).
func (w *WAL) Recover(tree *btree.BTree) error {
	fmt.Println("WAL: Starting recovery...")

	// Read all log records
	allRecords, err := w.ReadFrom(InvalidLSN + 1)
	if err != nil {
		return fmt.Errorf("wal: recovery read failed: %w", err)
	}

	if len(allRecords) == 0 {
		fmt.Println("WAL: Empty log, nothing to recover.")
		return nil
	}

	// ---- Pass 1: Analysis ----
	// Find committed and active transactions at crash time.
	committed := make(map[TxID]bool)
	aborted := make(map[TxID]bool)
	activeTxns := make(map[TxID]LSN) // TxID → last LSN seen

	for _, rec := range allRecords {
		switch rec.Type {
		case RecordBegin:
			activeTxns[rec.TxID] = rec.LSN
		case RecordCommit:
			committed[rec.TxID] = true
			delete(activeTxns, rec.TxID)
		case RecordAbort:
			aborted[rec.TxID] = true
			delete(activeTxns, rec.TxID)
		default:
			activeTxns[rec.TxID] = rec.LSN // update last LSN for this txn
		}
	}

	fmt.Printf("WAL: Analysis — committed: %d, aborted: %d, active (to undo): %d\n",
		len(committed), len(aborted), len(activeTxns))

	// ---- Pass 2: Redo ----
	// Replay all INSERT/UPDATE/DELETE records regardless of commit status.
	// (We'll undo the uncommitted ones in Pass 3.)
	redoCount := 0
	for _, rec := range allRecords {
		switch rec.Type {
		case RecordInsert:
			key, value, err := ParseInsertData(rec.Data)
			if err != nil {
				continue
			}
			// Ignore error — key might already exist if page was flushed before crash
			tree.Insert(key, value)
			redoCount++

		case RecordDelete:
			key, _, err := ParseDeleteData(rec.Data)
			if err != nil {
				continue
			}
			tree.Delete(key)
			redoCount++

		case RecordUpdate:
			key, _, newValue, err := ParseUpdateData(rec.Data)
			if err != nil {
				continue
			}
			// For updates: delete old, insert new
			tree.Delete(key)
			tree.Insert(key, newValue)
			redoCount++
		}
	}

	fmt.Printf("WAL: Redo — replayed %d operations\n", redoCount)

	// ---- Pass 3: Undo ----
	// Reverse all changes from transactions that were active at crash time.
	undoCount := 0
	for txID, lastLSN := range activeTxns {
		// Walk backward through this transaction's log records using PrevLSN
		currentLSN := lastLSN
		for currentLSN != InvalidLSN {
			// Find the record with this LSN
			var rec *LogRecord
			for _, r := range allRecords {
				if r.LSN == currentLSN {
					rec = r
					break
				}
			}
			if rec == nil {
				break
			}

			// Undo this operation (apply its reverse)
			switch rec.Type {
			case RecordInsert:
				key, _, err := ParseInsertData(rec.Data)
				if err == nil {
					tree.Delete(key) // undo an insert = delete
					undoCount++
				}
			case RecordDelete:
				key, oldValue, err := ParseDeleteData(rec.Data)
				if err == nil {
					tree.Insert(key, oldValue) // undo a delete = re-insert
					undoCount++
				}
			case RecordUpdate:
				key, oldValue, _, err := ParseUpdateData(rec.Data)
				if err == nil {
					tree.Delete(key)
					tree.Insert(key, oldValue) // undo an update = restore old value
					undoCount++
				}
			}

			currentLSN = rec.PrevLSN // follow the chain backward
		}

		fmt.Printf("WAL: Undo txn %d (last LSN %d)\n", txID, lastLSN)
	}

	fmt.Printf("WAL: Undo — reversed %d operations\n", undoCount)
	fmt.Println("WAL: Recovery complete.")
	return nil
}

// ---- Internal helpers ----

// appendRecord assigns an LSN to the record, serializes it, and appends to the log file.
// The file position is always at the end (O_APPEND), so writes are sequential.
func (w *WAL) appendRecord(rec *LogRecord) (LSN, error) {
	// Assign the next LSN
	lsn := LSN(atomic.AddUint64(&w.nextLSN, 1) - 1)
	rec.LSN = lsn

	data := rec.Serialize()
	if _, err := w.file.Write(data); err != nil {
		return InvalidLSN, fmt.Errorf("wal: write failed: %w", err)
	}

	return lsn, nil
}

// undoTransaction reverses all changes made by a transaction.
// Used by Abort() and the Undo pass of recovery.
func (w *WAL) undoTransaction(txID TxID, tree *btree.BTree) error {
	// Read all records for this transaction
	allRecords, err := w.ReadFrom(InvalidLSN + 1)
	if err != nil {
		return err
	}

	// Collect records for this txn, in reverse order
	var txnRecords []*LogRecord
	for _, rec := range allRecords {
		if rec.TxID == txID {
			txnRecords = append(txnRecords, rec)
		}
	}

	// Undo in reverse order (last change first)
	for i := len(txnRecords) - 1; i >= 0; i-- {
		rec := txnRecords[i]
		switch rec.Type {
		case RecordInsert:
			key, _, _ := ParseInsertData(rec.Data)
			tree.Delete(key)
		case RecordDelete:
			key, oldValue, _ := ParseDeleteData(rec.Data)
			tree.Insert(key, oldValue)
		case RecordUpdate:
			key, oldValue, _, _ := ParseUpdateData(rec.Data)
			tree.Delete(key)
			tree.Insert(key, oldValue)
		}
	}

	return nil
}

// scanForMaxLSN reads through the log file to find the highest LSN used.
// Called on startup to continue LSN assignment without gaps.
func (w *WAL) scanForMaxLSN() (LSN, error) {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	var maxLSN LSN
	buf4 := make([]byte, 4)

	for {
		_, err := io.ReadFull(w.file, buf4)
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}

		totalSize := binary.LittleEndian.Uint32(buf4)
		if totalSize < uint32(HeaderSize) {
			break
		}

		recBuf := make([]byte, totalSize)
		copy(recBuf[:4], buf4)
		if _, err := io.ReadFull(w.file, recBuf[4:]); err != nil {
			break
		}

		rec, err := DeserializeRecord(recBuf)
		if err != nil {
			break
		}

		if rec.LSN > maxLSN {
			maxLSN = rec.LSN
		}
	}

	// Seek back to end for appending
	w.file.Seek(0, io.SeekEnd)
	return maxLSN, nil
}

// Close syncs and closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}
