package tests

import (
	"minidb/internal/buffer"
	"minidb/internal/btree"
	"minidb/internal/disk"
	"minidb/internal/wal"
	"os"
	"testing"
)

// newTestWAL creates a fresh WAL backed by a temp file.
func newTestWAL(t *testing.T) (*wal.WAL, func()) {
	t.Helper()
	tmpFile, err := os.CreateTemp("", "minidb_wal_test_*.log")
	if err != nil {
		t.Fatalf("cannot create temp WAL file: %v", err)
	}
	tmpFile.Close()

	w, err := wal.NewWAL(tmpFile.Name())
	if err != nil {
		t.Fatalf("cannot create WAL: %v", err)
	}

	cleanup := func() {
		w.Close()
		os.Remove(tmpFile.Name())
	}
	return w, cleanup
}

// newTestBTreeAndWAL creates a paired B+ tree + WAL for transaction tests.
func newTestBTreeAndWAL(t *testing.T) (*btree.BTree, *wal.WAL, func()) {
	t.Helper()

	// B+ tree
	dbFile, err := os.CreateTemp("", "minidb_wal_db_*.db")
	if err != nil {
		t.Fatal(err)
	}
	dbFile.Close()

	walFile, err := os.CreateTemp("", "minidb_wal_log_*.wal")
	if err != nil {
		t.Fatal(err)
	}
	walFile.Close()

	dm, _ := disk.NewDiskManager(dbFile.Name())
	bp := buffer.NewBufferPool(64, dm)
	tree, _ := btree.NewBTree(bp)
	w, _ := wal.NewWAL(walFile.Name())

	cleanup := func() {
		w.Close()
		bp.FlushAll()
		dm.Close()
		os.Remove(dbFile.Name())
		os.Remove(walFile.Name())
	}

	return tree, w, cleanup
}

// TestWALBeginCommit tests that a begin/commit cycle writes records to the log.
func TestWALBeginCommit(t *testing.T) {
	w, cleanup := newTestWAL(t)
	defer cleanup()

	txID, err := w.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if txID == 0 {
		t.Error("Begin returned zero TxID")
	}

	if err := w.Commit(txID); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Read back records and verify BEGIN + COMMIT are there
	records, err := w.ReadFrom(1)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	if len(records) < 2 {
		t.Fatalf("Expected at least 2 records (BEGIN + COMMIT), got %d", len(records))
	}

	foundBegin := false
	foundCommit := false
	for _, rec := range records {
		if rec.TxID == txID {
			switch rec.Type {
			case wal.RecordBegin:
				foundBegin = true
			case wal.RecordCommit:
				foundCommit = true
			}
		}
	}

	if !foundBegin {
		t.Error("BEGIN record not found in log")
	}
	if !foundCommit {
		t.Error("COMMIT record not found in log")
	}
}

// TestWALLogInsert tests that LogInsert writes a recoverable record.
func TestWALLogInsert(t *testing.T) {
	w, cleanup := newTestWAL(t)
	defer cleanup()

	txID, _ := w.Begin()
	lsn, err := w.LogInsert(txID, 42, 100, 999)
	if err != nil {
		t.Fatalf("LogInsert failed: %v", err)
	}
	if lsn == wal.InvalidLSN {
		t.Error("LogInsert returned InvalidLSN")
	}

	w.Commit(txID)

	records, _ := w.ReadFrom(1)
	var insertRec *wal.LogRecord
	for _, rec := range records {
		if rec.Type == wal.RecordInsert {
			insertRec = rec
			break
		}
	}

	if insertRec == nil {
		t.Fatal("INSERT record not found in log")
	}

	key, value, err := wal.ParseInsertData(insertRec.Data)
	if err != nil {
		t.Fatalf("ParseInsertData failed: %v", err)
	}
	if key != 100 {
		t.Errorf("Expected key=100, got %d", key)
	}
	if value != 999 {
		t.Errorf("Expected value=999, got %d", value)
	}
}

// TestWALMultipleTransactions tests that multiple concurrent transactions
// log independently and their records are distinguishable.
func TestWALMultipleTransactions(t *testing.T) {
	w, cleanup := newTestWAL(t)
	defer cleanup()

	txID1, _ := w.Begin()
	txID2, _ := w.Begin()

	w.LogInsert(txID1, 1, 10, 100)
	w.LogInsert(txID2, 2, 20, 200)
	w.LogInsert(txID1, 1, 11, 110)

	w.Commit(txID1)
	w.Commit(txID2)

	records, _ := w.ReadFrom(1)

	tx1Records := 0
	tx2Records := 0
	for _, rec := range records {
		if rec.TxID == txID1 {
			tx1Records++
		}
		if rec.TxID == txID2 {
			tx2Records++
		}
	}

	// txID1: BEGIN + 2 INSERTs + COMMIT = 4
	if tx1Records < 4 {
		t.Errorf("Expected at least 4 records for txn1, got %d", tx1Records)
	}
	// txID2: BEGIN + 1 INSERT + COMMIT = 3
	if tx2Records < 3 {
		t.Errorf("Expected at least 3 records for txn2, got %d", tx2Records)
	}
}

// TestWALRecordSerialization tests that records survive a serialize/deserialize round-trip.
func TestWALRecordSerialization(t *testing.T) {
	tests := []struct {
		name string
		rec  *wal.LogRecord
	}{
		{
			name: "BEGIN",
			rec:  &wal.LogRecord{LSN: 1, TxID: 100, Type: wal.RecordBegin},
		},
		{
			name: "INSERT",
			rec: &wal.LogRecord{
				LSN: 2, TxID: 100, Type: wal.RecordInsert,
				PageID: 5, PrevLSN: 1,
				Data: wal.NewInsertData(42, 999),
			},
		},
		{
			name: "UPDATE",
			rec: &wal.LogRecord{
				LSN: 3, TxID: 100, Type: wal.RecordUpdate,
				Data: wal.NewUpdateData(42, 999, 1000),
			},
		},
		{
			name: "DELETE",
			rec: &wal.LogRecord{
				LSN: 4, TxID: 100, Type: wal.RecordDelete,
				Data: wal.NewDeleteData(42, 999),
			},
		},
		{
			name: "COMMIT",
			rec:  &wal.LogRecord{LSN: 5, TxID: 100, Type: wal.RecordCommit},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			serialized := tc.rec.Serialize()
			deserialized, err := wal.DeserializeRecord(serialized)
			if err != nil {
				t.Fatalf("Deserialize failed: %v", err)
			}

			if deserialized.LSN != tc.rec.LSN {
				t.Errorf("LSN: got %d, want %d", deserialized.LSN, tc.rec.LSN)
			}
			if deserialized.TxID != tc.rec.TxID {
				t.Errorf("TxID: got %d, want %d", deserialized.TxID, tc.rec.TxID)
			}
			if deserialized.Type != tc.rec.Type {
				t.Errorf("Type: got %v, want %v", deserialized.Type, tc.rec.Type)
			}
			if deserialized.PageID != tc.rec.PageID {
				t.Errorf("PageID: got %d, want %d", deserialized.PageID, tc.rec.PageID)
			}
			if len(deserialized.Data) != len(tc.rec.Data) {
				t.Errorf("Data length: got %d, want %d", len(deserialized.Data), len(tc.rec.Data))
			}
		})
	}
}

// TestWALRecovery simulates a crash and verifies ARIES recovery.
//
// Scenario:
//  1. Insert keys 1..10 (committed txns)
//  2. Begin txn to insert keys 11..20 but DO NOT commit (simulate crash)
//  3. Run recovery
//  4. Keys 1..10 should be present (redo)
//  5. Keys 11..20 should be absent (undo)
func TestWALRecovery(t *testing.T) {
	dbFile, _ := os.CreateTemp("", "minidb_recovery_db_*.db")
	dbFile.Close()
	walFile, _ := os.CreateTemp("", "minidb_recovery_wal_*.wal")
	walFile.Close()
	defer os.Remove(dbFile.Name())
	defer os.Remove(walFile.Name())

	var rootPageID disk.PageID

	// --- Phase 1: Normal operation ---
	{
		dm, _ := disk.NewDiskManager(dbFile.Name())
		bp := buffer.NewBufferPool(64, dm)
		tree, _ := btree.NewBTree(bp)
		rootPageID = tree.RootPageID()
		w, _ := wal.NewWAL(walFile.Name())

		// Committed inserts: keys 1..10
		for i := int64(1); i <= 10; i++ {
			txID, _ := w.Begin()
			w.LogInsert(txID, tree.RootPageID(), i, i*10)
			tree.Insert(i, i*10)
			w.Commit(txID)
		}

		// Uncommitted inserts: keys 11..20 (crash before commit)
		txID, _ := w.Begin()
		for i := int64(11); i <= 20; i++ {
			w.LogInsert(txID, tree.RootPageID(), i, i*10)
			tree.Insert(i, i*10)
		}
		// ← simulated crash here: NO Commit() called

		w.Close()
		bp.FlushAll()
		dm.Close()
	}

	// --- Phase 2: Recovery ---
	{
		dm, _ := disk.NewDiskManager(dbFile.Name())
		bp := buffer.NewBufferPool(64, dm)
		// Reopen the tree at the known root page
		tree := btree.OpenBTree(bp, rootPageID)
		w, _ := wal.NewWAL(walFile.Name())

		if err := w.Recover(tree); err != nil {
			t.Fatalf("Recovery failed: %v", err)
		}

		// Keys 1..10 should be present (committed, redone)
		for i := int64(1); i <= 10; i++ {
			val, found := tree.Search(i)
			if !found {
				t.Errorf("After recovery: key %d should exist (was committed)", i)
				continue
			}
			if val != i*10 {
				t.Errorf("After recovery: key %d has wrong value %d (want %d)", i, val, i*10)
			}
		}

		// Keys 11..20 should be absent (uncommitted, undone)
		for i := int64(11); i <= 20; i++ {
			_, found := tree.Search(i)
			if found {
				t.Errorf("After recovery: key %d should not exist (was not committed)", i)
			}
		}

		w.Close()
		bp.FlushAll()
		dm.Close()
	}
}
