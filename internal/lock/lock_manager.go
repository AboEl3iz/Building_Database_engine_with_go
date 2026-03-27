// Package lock implements a table-level lock manager for Two-Phase Locking (2PL)
// concurrency control.
//
// CONCEPT: Two-Phase Locking (2PL)
//
// 2PL is the most common concurrency control protocol in databases.
// It guarantees serializability (transactions appear to execute one-at-a-time)
// using two simple rules:
//
//   Phase 1 – Growing:   A transaction may acquire locks, but may NOT release any.
//   Phase 2 – Shrinking: A transaction may release locks, but may NOT acquire new ones.
//
// In strict 2PL (which we implement), ALL locks are held until COMMIT or ROLLBACK.
// This prevents cascading aborts and makes recovery simpler.
//
// Lock compatibility matrix (table-level):
//
//	Existing \ Requested │ Shared    │ Exclusive
//	─────────────────────┼───────────┼──────────
//	None                 │ ✅ Grant   │ ✅ Grant
//	Shared (other txn)   │ ✅ Grant   │ ❌ Conflict
//	Exclusive (other txn)│ ❌ Conflict│ ❌ Conflict
//	Same txn (any mode)  │ ✅ Re-enter│ ✅ Upgrade
//
// For simplicity, we return an error on conflict instead of blocking.
// This avoids deadlock detection/timeout complexity.
package lock

import (
	"fmt"
	"sync"
)

// LockMode represents the type of lock requested.
type LockMode int

const (
	// LockShared allows concurrent reads — multiple txns can hold shared locks
	// on the same resource simultaneously.
	LockShared LockMode = iota

	// LockExclusive provides exclusive access — only one txn can hold an
	// exclusive lock, and no shared locks can coexist.
	LockExclusive
)

// String returns a human-readable name for a LockMode.
func (m LockMode) String() string {
	switch m {
	case LockShared:
		return "SHARED"
	case LockExclusive:
		return "EXCLUSIVE"
	default:
		return "UNKNOWN"
	}
}



// lockEntry tracks who holds a lock on a specific resource and in what mode.
type lockEntry struct {
	mode    LockMode
	holders map[uint64]bool // set of transaction IDs holding this lock
}


// LockManager manages table-level locks for concurrent transactions.
//
// Thread-safe: all operations are protected by a mutex.
// The lock granularity is per-table (coarse-grained), which is simple but
// limits concurrency. A production database would use row-level locks or
// predicate locks for better throughput.
type LockManager struct {
	mu    sync.Mutex
	locks map[string]*lockEntry // resource name (table) → lock entry
}

// NewLockManager creates an empty LockManager.
func NewLockManager() *LockManager {
	return &LockManager{
		locks: make(map[string]*lockEntry),
	}
}

// Lock acquires a lock on the given resource for a transaction.
//
// Rules:
//   - If no lock exists → grant immediately
//   - If the same txn already holds a compatible lock → re-entrant (no-op or upgrade)
//   - If another txn holds a conflicting lock → return error (no waiting)
//
// In 2PL, locks are acquired during the growing phase and never released
// until COMMIT/ROLLBACK (see ReleaseAll).
func (lm *LockManager) Lock(txID uint64, resource string, mode LockMode) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry, exists := lm.locks[resource]

	// Case 1: No lock on this resource → grant
	if !exists || len(entry.holders) == 0 {
		lm.locks[resource] = &lockEntry{
			mode:    mode,
			holders: map[uint64]bool{txID: true},
		}
		return nil
	}

	// Case 2: Same transaction already holds a lock on this resource
	if entry.holders[txID] {
		// If we already hold the lock at the same or higher mode, it's a no-op
		if entry.mode >= mode {
			return nil
		}
		// Upgrade from Shared to Exclusive: only allowed if we're the sole holder
		if mode == LockExclusive && len(entry.holders) == 1 {
			entry.mode = LockExclusive
			return nil
		}
		// Can't upgrade if other txns also hold the shared lock
		return fmt.Errorf("lock: cannot upgrade to EXCLUSIVE on %q — other transactions hold SHARED locks", resource)
	}

	// Case 3: Another transaction holds the lock — check compatibility
	if entry.mode == LockExclusive {
		// Exclusive lock held by another txn → conflict
		return fmt.Errorf("lock: resource %q is locked in EXCLUSIVE mode by another transaction", resource)
	}

	// Existing lock is Shared
	if mode == LockShared {
		// Shared + Shared = compatible → grant
		entry.holders[txID] = true
		return nil
	}

	// Requesting Exclusive but Shared locks are held by others → conflict
	return fmt.Errorf("lock: cannot acquire EXCLUSIVE lock on %q — SHARED locks are held by other transactions", resource)
}

// Unlock releases a specific lock held by a transaction on a resource.
// This is generally not called directly in strict 2PL; use ReleaseAll instead.
func (lm *LockManager) Unlock(txID uint64, resource string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry, exists := lm.locks[resource]
	if !exists {
		return fmt.Errorf("lock: no lock found on %q", resource)
	}

	if !entry.holders[txID] {
		return fmt.Errorf("lock: txn %d does not hold a lock on %q", txID, resource)
	}

	delete(entry.holders, txID)

	// If no holders remain, remove the lock entry entirely
	if len(entry.holders) == 0 {
		delete(lm.locks, resource)
	}

	return nil
}

// ReleaseAll releases all locks held by a transaction.
// This is called during COMMIT or ROLLBACK (the shrinking phase of 2PL).
func (lm *LockManager) ReleaseAll(txID uint64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for resource, entry := range lm.locks {
		if entry.holders[txID] {
			delete(entry.holders, txID)
			if len(entry.holders) == 0 {
				delete(lm.locks, resource)
			}
		}
	}
}

// IsLocked checks whether a transaction holds a lock on a resource.
func (lm *LockManager) IsLocked(txID uint64, resource string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry, exists := lm.locks[resource]
	if !exists {
		return false
	}
	return entry.holders[txID]
}

// GetLockMode returns the current lock mode for a resource, or -1 if not locked.
func (lm *LockManager) GetLockMode(resource string) LockMode {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry, exists := lm.locks[resource]
	if !exists || len(entry.holders) == 0 {
		return -1 // no lock
	}
	return entry.mode
}

// HolderCount returns how many transactions hold a lock on a resource.
func (lm *LockManager) HolderCount(resource string) int {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry, exists := lm.locks[resource]
	if !exists {
		return 0
	}
	return len(entry.holders)
}
