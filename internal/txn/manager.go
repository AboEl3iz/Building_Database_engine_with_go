// Package txn provides a session-level transaction manager for MiniDB.
//
// It wraps the WAL and provides explicit BEGIN / COMMIT / ROLLBACK semantics.
//
// State machine:
//
//	idle ──BEGIN──► active ──COMMIT──► idle
//	                        ──ROLLBACK► idle
//
// While in "active" state every DML operation (INSERT / UPDATE / DELETE)
// is logged under one shared WAL transaction ID.  We also accumulate an
// in-memory "undo log" so that ROLLBACK can reverse the changes without
// needing to re-read the WAL file.
//
// ACID guarantees:
//
//	A (Atomicity):   ROLLBACK applies every undo op in reverse order.
//	C (Consistency): Enforced by the executor (schema checks, type checks).
//	I (Isolation):   Single-threaded engine – no concurrent conflicts.
//	D (Durability):  COMMIT causes WAL.Commit() → file.Sync().
package txn

import (
	"fmt"
	"minidb/internal/btree"
	"minidb/internal/wal"
)

// UndoOpKind identifies what kind of inverse operation to apply on rollback.
type UndoOpKind int

const (
	// UndoInsert reverses an INSERT: delete the key that was inserted.
	UndoInsert UndoOpKind = iota
	// UndoDelete reverses a DELETE: re-insert the key+value that was deleted.
	UndoDelete
	// UndoUpdate reverses an UPDATE: restore the old encoded value.
	UndoUpdate
)

// UndoOp describes a single reversible data-mutation.
type UndoOp struct {
	Kind     UndoOpKind
	Tree     *btree.BTree // which B+ tree (i.e. which table)
	Key      int64
	OldValue int64 // only used for UndoDelete / UndoUpdate
}

// TxManager is a session-level transaction controller.
//
// Create one per executor session with NewTxManager.
// All mutation helpers (LogInsert, LogUpdate, LogDelete) are called by the
// Executor after it performs the actual B+ tree operation so that we can
// record what needs to be undone.
type TxManager struct {
	w          *wal.WAL
	activeTxID wal.TxID // 0 means no active transaction
	undoLog    []UndoOp // accumulated in the order operations happened
}

// NewTxManager creates a TxManager backed by the given WAL.
func NewTxManager(w *wal.WAL) *TxManager {
	return &TxManager{w: w}
}

// IsActive reports whether a user transaction is currently open.
func (tm *TxManager) IsActive() bool {
	return tm.activeTxID != 0
}

// ActiveTxID returns the current WAL transaction ID.
// Returns 0 if no transaction is active.
func (tm *TxManager) ActiveTxID() wal.TxID {
	return tm.activeTxID
}

// Begin opens a new explicit user transaction.
// Returns an error if a transaction is already open — nested BEGIN is not
// supported (PostgreSQL-style: you have to COMMIT or ROLLBACK first).
func (tm *TxManager) Begin() error {
	if tm.IsActive() {
		return fmt.Errorf("txn: there is already an active transaction; COMMIT or ROLLBACK it first")
	}
	txID, err := tm.w.Begin()
	if err != nil {
		return fmt.Errorf("txn: WAL Begin failed: %w", err)
	}
	tm.activeTxID = txID
	tm.undoLog = tm.undoLog[:0] // reset undo log
	return nil
}

// Commit finalises the current transaction durably.
// Calls WAL.Commit which writes the COMMIT record and calls file.Sync().
func (tm *TxManager) Commit() error {
	if !tm.IsActive() {
		return fmt.Errorf("txn: no active transaction to commit")
	}
	txID := tm.activeTxID
	tm.reset()
	if err := tm.w.Commit(txID); err != nil {
		return fmt.Errorf("txn: WAL Commit failed: %w", err)
	}
	return nil
}

// Rollback reverses all mutations recorded in the undo log (last-in, first-out)
// and writes an ABORT record to the WAL.
func (tm *TxManager) Rollback() error {
	if !tm.IsActive() {
		return fmt.Errorf("txn: no active transaction to roll back")
	}
	// Apply undo ops in reverse order (most recent first).
	for i := len(tm.undoLog) - 1; i >= 0; i-- {
		op := tm.undoLog[i]
		switch op.Kind {
		case UndoInsert:
			// Reverse an INSERT → delete the key.
			op.Tree.Delete(op.Key)
		case UndoDelete:
			// Reverse a DELETE → re-insert the row.
			op.Tree.Insert(op.Key, op.OldValue)
		case UndoUpdate:
			// Reverse an UPDATE → delete the new value, restore the old.
			op.Tree.Delete(op.Key)
			op.Tree.Insert(op.Key, op.OldValue)
		}
	}
	txID := tm.activeTxID
	tm.reset()
	// Write ABORT record to WAL (best-effort; don't shadow the rollback error).
	_ = tm.w.Abort(txID, nil) // nil tree — we already did undo above
	return nil
}

// RecordInsert appends an undo op for a newly inserted key.
// Call this AFTER tree.Insert() succeeds so we know the op is durable.
func (tm *TxManager) RecordInsert(tree *btree.BTree, key int64) {
	tm.undoLog = append(tm.undoLog, UndoOp{
		Kind: UndoInsert,
		Tree: tree,
		Key:  key,
	})
}

// RecordDelete appends an undo op for a deleted key (saves oldValue for re-insertion).
// Call this BEFORE tree.Delete().
func (tm *TxManager) RecordDelete(tree *btree.BTree, key, oldValue int64) {
	tm.undoLog = append(tm.undoLog, UndoOp{
		Kind:     UndoDelete,
		Tree:     tree,
		Key:      key,
		OldValue: oldValue,
	})
}

// RecordUpdate appends an undo op for an updated key (saves oldValue).
// Call this BEFORE the delete+insert that implements the update.
func (tm *TxManager) RecordUpdate(tree *btree.BTree, key, oldValue int64) {
	tm.undoLog = append(tm.undoLog, UndoOp{
		Kind:     UndoUpdate,
		Tree:     tree,
		Key:      key,
		OldValue: oldValue,
	})
}

// reset clears transaction state (called after commit or rollback).
func (tm *TxManager) reset() {
	tm.activeTxID = 0
	tm.undoLog = nil
}
