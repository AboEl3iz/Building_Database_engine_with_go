package tests

import (
	"minidb/internal/parser"
	"testing"
)

// ============================================================
// Transaction Tests
// ============================================================

// TestTransactionBeginCommit verifies that rows inserted inside an explicit
// BEGIN…COMMIT block are visible after the commit.
func TestTransactionBeginCommit(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE accts (id INT, bal INT)")

	// Open transaction, insert three rows, commit.
	execSQL(t, exec, "BEGIN")
	execSQL(t, exec, "INSERT INTO accts VALUES (1, 1000)")
	execSQL(t, exec, "INSERT INTO accts VALUES (2, 500)")
	execSQL(t, exec, "INSERT INTO accts VALUES (3, 250)")
	execSQL(t, exec, "COMMIT")

	rs := execSQL(t, exec, "SELECT * FROM accts")
	if rs.RowCount() != 3 {
		t.Errorf("TestTransactionBeginCommit: expected 3 rows after COMMIT, got %d", rs.RowCount())
	}
}

// TestTransactionRollback verifies that rows inserted in an aborted
// transaction are NOT visible after ROLLBACK.
func TestTransactionRollback(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE tmp (id INT, val INT)")

	// Pre-existing row (auto-committed)
	execSQL(t, exec, "INSERT INTO tmp VALUES (1, 100)")

	// Open transaction, insert a row, then roll back.
	execSQL(t, exec, "BEGIN")
	execSQL(t, exec, "INSERT INTO tmp VALUES (2, 999)")
	execSQL(t, exec, "ROLLBACK")

	rs := execSQL(t, exec, "SELECT * FROM tmp")
	if rs.RowCount() != 1 {
		t.Errorf("TestTransactionRollback: expected 1 row after ROLLBACK, got %d", rs.RowCount())
	}
}

// TestTransactionRollbackUpdate verifies that an UPDATE inside a rolled-back
// transaction restores the original value.
func TestTransactionRollbackUpdate(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE kv (id INT, val INT)")
	execSQL(t, exec, "INSERT INTO kv VALUES (1, 42)")

	execSQL(t, exec, "BEGIN")
	execSQL(t, exec, "UPDATE kv SET val = 999 WHERE id = 1")
	execSQL(t, exec, "ROLLBACK")

	rs := execSQL(t, exec, "SELECT * FROM kv WHERE id = 1")
	if rs.RowCount() != 1 {
		t.Fatalf("TestTransactionRollbackUpdate: expected 1 row, got %d", rs.RowCount())
	}
	// The value must have been restored to 42.
	val, ok := rs.Rows[0]["val"].(int64)
	if !ok || val != 42 {
		t.Errorf("TestTransactionRollbackUpdate: expected val=42 after ROLLBACK, got %v", rs.Rows[0]["val"])
	}
}

// TestTransactionRollbackDelete verifies that a DELETE inside a rolled-back
// transaction leaves the row present.
func TestTransactionRollbackDelete(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE items (id INT, name TEXT)")
	execSQL(t, exec, "INSERT INTO items VALUES (1, 'apple')")
	execSQL(t, exec, "INSERT INTO items VALUES (2, 'banana')")

	execSQL(t, exec, "BEGIN")
	execSQL(t, exec, "DELETE FROM items WHERE id = 1")
	execSQL(t, exec, "ROLLBACK")

	rs := execSQL(t, exec, "SELECT * FROM items")
	if rs.RowCount() != 2 {
		t.Errorf("TestTransactionRollbackDelete: expected 2 rows after ROLLBACK of DELETE, got %d", rs.RowCount())
	}
}

// TestTransactionMultiOp verifies that multiple DML operations
// (INSERT + UPDATE + DELETE) inside one committed transaction all take effect.
func TestTransactionMultiOp(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE ledger (id INT, amount INT)")
	execSQL(t, exec, "INSERT INTO ledger VALUES (1, 100)")
	execSQL(t, exec, "INSERT INTO ledger VALUES (2, 200)")

	execSQL(t, exec, "BEGIN")
	execSQL(t, exec, "INSERT INTO ledger VALUES (3, 300)")          // add
	execSQL(t, exec, "UPDATE ledger SET amount = 150 WHERE id = 1") // modify
	execSQL(t, exec, "DELETE FROM ledger WHERE id = 2")             // remove
	execSQL(t, exec, "COMMIT")

	rs := execSQL(t, exec, "SELECT * FROM ledger")
	// Expect rows: id=1 (amount=150), id=3 (amount=300) → 2 rows
	if rs.RowCount() != 2 {
		t.Errorf("TestTransactionMultiOp: expected 2 rows after COMMIT, got %d", rs.RowCount())
	}
}

// TestTransactionMultiOpRollback verifies that all operations from a
// multi-op transaction are reversed on ROLLBACK.
func TestTransactionMultiOpRollback(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE ledger2 (id INT, amount INT)")
	execSQL(t, exec, "INSERT INTO ledger2 VALUES (1, 100)")
	execSQL(t, exec, "INSERT INTO ledger2 VALUES (2, 200)")

	execSQL(t, exec, "BEGIN")
	execSQL(t, exec, "INSERT INTO ledger2 VALUES (3, 300)")
	execSQL(t, exec, "UPDATE ledger2 SET amount = 150 WHERE id = 1")
	execSQL(t, exec, "DELETE FROM ledger2 WHERE id = 2")
	execSQL(t, exec, "ROLLBACK")

	rs := execSQL(t, exec, "SELECT * FROM ledger2")
	// Should be back to original: id=1 (100), id=2 (200) → 2 rows
	if rs.RowCount() != 2 {
		t.Errorf("TestTransactionMultiOpRollback: expected 2 rows after ROLLBACK, got %d", rs.RowCount())
	}
}

// TestTransactionAutoCommit verifies that plain DML (without BEGIN) still
// works exactly as before — every statement auto-commits.
func TestTransactionAutoCommit(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE ac (id INT)")
	execSQL(t, exec, "INSERT INTO ac VALUES (1)")
	execSQL(t, exec, "INSERT INTO ac VALUES (2)")
	execSQL(t, exec, "DELETE FROM ac WHERE id = 1")

	rs := execSQL(t, exec, "SELECT * FROM ac")
	if rs.RowCount() != 1 {
		t.Errorf("TestTransactionAutoCommit: expected 1 row, got %d", rs.RowCount())
	}
}

// TestTransactionDoubleBegin verifies that a second BEGIN while a transaction
// is already open returns an error.
func TestTransactionDoubleBegin(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE t (id INT)")
	execSQL(t, exec, "BEGIN")

	stmt, _ := parser.ParseSQL("BEGIN")
	_, err := exec.Execute(stmt)
	if err == nil {
		t.Error("TestTransactionDoubleBegin: expected error on second BEGIN, got nil")
	}

	// Clean up by rolling back
	execSQL(t, exec, "ROLLBACK")
}

// TestTransactionCommitWithoutBegin verifies that COMMIT without an active
// transaction returns an error.
func TestTransactionCommitWithoutBegin(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	stmt, _ := parser.ParseSQL("COMMIT")
	_, err := exec.Execute(stmt)
	if err == nil {
		t.Error("TestTransactionCommitWithoutBegin: expected error on COMMIT with no active txn")
	}
}

// TestTransactionRollbackWithoutBegin verifies that ROLLBACK without an active
// transaction returns an error.
func TestTransactionRollbackWithoutBegin(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	stmt, _ := parser.ParseSQL("ROLLBACK")
	_, err := exec.Execute(stmt)
	if err == nil {
		t.Error("TestTransactionRollbackWithoutBegin: expected error on ROLLBACK with no active txn")
	}
}

// ============================================================
// Parser-level Transaction Tests
// ============================================================

// TestParseBegin verifies that BEGIN parses to a BeginStmt.
func TestParseBegin(t *testing.T) {
	for _, sql := range []string{"BEGIN", "BEGIN TRANSACTION", "BEGIN;"} {
		stmt, err := parser.ParseSQL(sql)
		if err != nil {
			t.Errorf("TestParseBegin: ParseSQL(%q) failed: %v", sql, err)
			continue
		}
		if _, ok := stmt.(*parser.BeginStmt); !ok {
			t.Errorf("TestParseBegin: ParseSQL(%q) returned %T, want *parser.BeginStmt", sql, stmt)
		}
	}
}

// TestParseCommit verifies that COMMIT parses to a CommitStmt.
func TestParseCommit(t *testing.T) {
	for _, sql := range []string{"COMMIT", "COMMIT TRANSACTION", "COMMIT;"} {
		stmt, err := parser.ParseSQL(sql)
		if err != nil {
			t.Errorf("TestParseCommit: ParseSQL(%q) failed: %v", sql, err)
			continue
		}
		if _, ok := stmt.(*parser.CommitStmt); !ok {
			t.Errorf("TestParseCommit: ParseSQL(%q) returned %T, want *parser.CommitStmt", sql, stmt)
		}
	}
}

// TestParseRollback verifies that ROLLBACK parses to a RollbackStmt.
func TestParseRollback(t *testing.T) {
	for _, sql := range []string{"ROLLBACK", "ROLLBACK TRANSACTION", "ROLLBACK;"} {
		stmt, err := parser.ParseSQL(sql)
		if err != nil {
			t.Errorf("TestParseRollback: ParseSQL(%q) failed: %v", sql, err)
			continue
		}
		if _, ok := stmt.(*parser.RollbackStmt); !ok {
			t.Errorf("TestParseRollback: ParseSQL(%q) returned %T, want *parser.RollbackStmt", sql, stmt)
		}
	}
}
