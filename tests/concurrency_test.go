package tests

import (
	"minidb/internal/buffer"
	"minidb/internal/catalog"
	"minidb/internal/disk"
	"minidb/internal/engine"
	"minidb/internal/lock"
	"minidb/internal/parser"
	"minidb/internal/txn"
	"minidb/internal/wal"
	"os"
	"testing"
)

// TestConcurrency_LockCompatibility verifies the LockMode compatibility matrix.
func TestConcurrency_LockCompatibility(t *testing.T) {
	lm := lock.NewLockManager()
	table := "users"

	// Txn 1 acquires SHARED lock
	if err := lm.Lock(1, table, lock.LockShared); err != nil {
		t.Fatalf("expected success for first SHARED lock, got %v", err)
	}
	if !lm.IsLocked(1, table) {
		t.Errorf("expected IsLocked to be true for Tx1")
	}

	// Txn 2 acquires SHARED lock (Compatible)
	if err := lm.Lock(2, table, lock.LockShared); err != nil {
		t.Fatalf("expected success for second SHARED lock, got %v", err)
	}

	// Txn 3 attempts EXCLUSIVE lock (Conflict with SHARED)
	if err := lm.Lock(3, table, lock.LockExclusive); err == nil {
		t.Fatalf("expected error acquiring EXCLUSIVE lock while SHARED is held")
	}

	// Release both SHARED locks
	lm.ReleaseAll(1)
	lm.ReleaseAll(2)

	// Txn 3 acquires EXCLUSIVE lock (Success)
	if err := lm.Lock(3, table, lock.LockExclusive); err != nil {
		t.Fatalf("expected success acquiring EXCLUSIVE lock after SHARED locks released, got %v", err)
	}

	// Txn 4 attempts SHARED lock (Conflict with EXCLUSIVE)
	if err := lm.Lock(4, table, lock.LockShared); err == nil {
		t.Fatalf("expected error acquiring SHARED lock while EXCLUSIVE is held")
	}

	// Txn 5 attempts EXCLUSIVE lock (Conflict with EXCLUSIVE)
	if err := lm.Lock(5, table, lock.LockExclusive); err == nil {
		t.Fatalf("expected error acquiring EXCLUSIVE lock while EXCLUSIVE is held")
	}

	// Clean up
	lm.ReleaseAll(3)
}

// TestConcurrency_LockUpgrade verifies that a transaction can upgrade its own lock.
func TestConcurrency_LockUpgrade(t *testing.T) {
	lm := lock.NewLockManager()
	table := "users"

	// Txn 1 acquires SHARED lock
	if err := lm.Lock(1, table, lock.LockShared); err != nil {
		t.Fatalf("expected success for SHARED lock, got %v", err)
	}

	// Txn 1 upgrades to EXCLUSIVE lock (Allowed since it's the only holder)
	if err := lm.Lock(1, table, lock.LockExclusive); err != nil {
		t.Fatalf("expected success upgrading to EXCLUSIVE lock, got %v", err)
	}
	if lm.GetLockMode(table) != lock.LockExclusive {
		t.Errorf("expected upgraded lock mode to be EXCLUSIVE")
	}

	lm.ReleaseAll(1)

	// Now try downgrade (Exclusive -> Shared) which is just a no-op
	if err := lm.Lock(2, table, lock.LockExclusive); err != nil {
		t.Fatalf("expected success for EXCLUSIVE lock, got %v", err)
	}
	if err := lm.Lock(2, table, lock.LockShared); err != nil {
		t.Fatalf("expected success re-acquiring as SHARED lock, got %v", err)
	}
	if lm.GetLockMode(table) != lock.LockExclusive {
		t.Errorf("expected lock mode to REMAIN EXCLUSIVE after downgrade attempt")
	}

	lm.ReleaseAll(2)
}

// setupConcurrentEngines creates two Executors (sessions) sharing the same
// BufferPool, Catalog, LockManager, and WAL for testing concurrency.
func setupConcurrentEngines(t *testing.T) (*engine.Executor, *engine.Executor, func()) {
	// 1. We need one of each underlying resource
	_, cleanup := newTestEngine(t)
	// We need to access its private fields, but we can't because they are lowercase.
	// We will use the eng1 instance as session 1, and we need another session.
	// Since we can't access eng1.bp, we must instantiate from scratch.
	cleanup() // discard newTestEngine

	importOS := "os"
	_ = importOS
	
	// Create temp files
	dbFile, _ := os.CreateTemp("", "minidb_engine_*.db")
	walFile, _ := os.CreateTemp("", "minidb_engine_*.wal")
	catalogFile, _ := os.CreateTemp("", "minidb_engine_*.catalog.json")

	dm, err := disk.NewDiskManager(dbFile.Name())
	if err != nil {
		t.Fatalf("DiskManager: %v", err)
	}
	bp := buffer.NewBufferPool(128, dm)
	w, err := wal.NewWAL(walFile.Name())
	if err != nil {
		t.Fatalf("WAL: %v", err)
	}
	cat, err := catalog.NewCatalog(catalogFile.Name())
	if err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	lm := lock.NewLockManager()

	// Create two TxManagers that share the LockManager
	txm1 := txn.NewTxManager(w, lm)
	txm2 := txn.NewTxManager(w, lm)

	// Create two executors (representing two sessions)
	exec1 := engine.NewExecutor(bp, cat, w, txm1)
	exec2 := engine.NewExecutor(bp, cat, w, txm2)

	clean := func() {
		w.Close()
		bp.FlushAll()
		dm.Close()
		os.Remove(walFile.Name())
		os.Remove(dbFile.Name())
		os.Remove(catalogFile.Name())
	}
	return exec1, exec2, clean
}

// TestConcurrency_2PL checks system integration with the executor and transaction manager.
func TestConcurrency_2PL(t *testing.T) {
	eng1, eng2, cleanup := setupConcurrentEngines(t)
	defer cleanup()

	// Setup: create table and insert initial data
	setupSQL := []string{
		"CREATE TABLE accounts (id INT, balance INT);",
		"INSERT INTO accounts VALUES (1, 100);",
		"INSERT INTO accounts VALUES (2, 200);",
	}
	for _, sql := range setupSQL {
		stmt, err := parser.ParseSQL(sql)
		if err != nil {
			t.Fatalf("Failed to parse setup SQL %q: %v", sql, err)
		}
		_, err = eng1.Execute(stmt)
		if err != nil {
			t.Fatalf("Failed to execute setup SQL %q: %v", sql, err)
		}
	}

	stmtBegin, _ := parser.ParseSQL("BEGIN;")
	
	// Test 1: Transaction 1 reads, Transaction 2 reads (SHOULD WORK)
	eng1.Execute(stmtBegin)
	eng2.Execute(stmtBegin)

	stmtSelect, _ := parser.ParseSQL("SELECT * FROM accounts;")
	
	// eng1 reads
	_, err := eng1.Execute(stmtSelect)
	if err != nil {
		t.Errorf("eng1 read failed: %v", err)
	}

	// eng2 reads concurrently
	_, err = eng2.Execute(stmtSelect)
	if err != nil {
		t.Errorf("eng2 concurrent read failed (expected success due to SHARED lock): %v", err)
	}

	stmtCommit, _ := parser.ParseSQL("COMMIT;")
	eng1.Execute(stmtCommit)
	eng2.Execute(stmtCommit)

	// Test 2: Transaction 1 writes, Transaction 2 writes (SECOND SHOULD FAIL)
	eng1.Execute(stmtBegin)
	eng2.Execute(stmtBegin)

	stmtUpdate1, _ := parser.ParseSQL("UPDATE accounts SET balance = 150 WHERE id = 1;")
	stmtUpdate2, _ := parser.ParseSQL("UPDATE accounts SET balance = 250 WHERE id = 2;")

	// eng1 acquires EXCLUSIVE lock
	_, err = eng1.Execute(stmtUpdate1)
	if err != nil {
		t.Fatalf("eng1 write failed: %v", err)
	}

	// eng2 attempts EXCLUSIVE lock and should fail
	_, err = eng2.Execute(stmtUpdate2)
	if err == nil {
		t.Errorf("eng2 write succeeded but should have failed due to conflicting EXCLUSIVE lock")
	}

	// eng1 finishes and releases lock
	stmtRollback, _ := parser.ParseSQL("ROLLBACK;")
	eng1.Execute(stmtCommit)
	eng2.Execute(stmtRollback) // Need to rollback failed txn

	// Test 3: Transaction 2 writes after Transaction 1 commits (SHOULD WORK)
	eng2.Execute(stmtBegin)
	_, err = eng2.Execute(stmtUpdate2)
	if err != nil {
		t.Errorf("eng2 write failed after eng1 COMMIT (lock should be released): %v", err)
	}
	eng2.Execute(stmtCommit)

	// Test 4: Rollback releases locks
	eng1.Execute(stmtBegin)
	_, err = eng1.Execute(stmtUpdate1) // Acquires EXCLUSIVE lock
	if err != nil {
		t.Fatalf("eng1 write failed: %v", err)
	}
	eng1.Execute(stmtRollback) // Releases lock

	eng2.Execute(stmtBegin)
	_, err = eng2.Execute(stmtUpdate2) // Should succeed now
	if err != nil {
		t.Errorf("eng2 write failed after eng1 ROLLBACK (lock should be released): %v", err)
	}
	eng2.Execute(stmtCommit)
}

// TestConcurrency_IndependentTables checks that locks on different tables don't conflict.
func TestConcurrency_IndependentTables(t *testing.T) {
	eng1, eng2, cleanup := setupConcurrentEngines(t)
	defer cleanup()

	// Setup: create two tables
	setupSQL := []string{
		"CREATE TABLE users (id INT, name TEXT);",
		"CREATE TABLE products (id INT, price INT);",
	}
	for _, sql := range setupSQL {
		stmt, _ := parser.ParseSQL(sql)
		eng1.Execute(stmt)
	}

	stmtBegin, _ := parser.ParseSQL("BEGIN;")
	eng1.Execute(stmtBegin)
	eng2.Execute(stmtBegin)

	stmtInsUsers, _ := parser.ParseSQL("INSERT INTO users VALUES (1, 'Alice');")
	stmtInsProds, _ := parser.ParseSQL("INSERT INTO products VALUES (1, 100);")

	// eng1 locks 'users'
	_, err := eng1.Execute(stmtInsUsers)
	if err != nil {
		t.Errorf("eng1 users insert failed: %v", err)
	}

	// eng2 locks 'products'
	_, err = eng2.Execute(stmtInsProds)
	if err != nil {
		t.Errorf("eng2 products insert failed (expected independent locks): %v", err)
	}

	stmtCommit, _ := parser.ParseSQL("COMMIT;")
	eng1.Execute(stmtCommit)
	eng2.Execute(stmtCommit)
}

