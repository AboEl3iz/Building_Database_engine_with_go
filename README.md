# MiniDB — A Storage Engine in Go

A mini SQLite/Postgres-style database engine built from scratch in Go.

## Components
| Component | Package | Description |
|-----------|---------|-------------|
| **REPL** | `cmd/minidb` | Interactive command-line interface. |
| **Parser** | `internal/parser` | Lexer & recursive descent parser that builds an Abstract Syntax Tree (AST). |
| **Executor** | `internal/engine` | Query planner/evaluator. Coordinates B+ tree searches, joins, inserting, and secondary index maintenance. |
| **Catalog** | `internal/catalog` | Manages schemas, tables, and secondary indexes. Persisted as JSON. |
| **B+ Tree** | `internal/btree` | O(log n) ordered key-value storage. Used for primary tables and secondary indexes. |
| **Buffer Pool** | `internal/buffer` | In-memory page cache with LRU eviction policy. |
| **Disk Manager** | `internal/disk` | Handles raw file I/O for 4KB pages. |
| **WAL** | `internal/wal` | Write-Ahead Log for durability and transaction rollback. |
| **Transaction Mgr** | `internal/txn` | Session-level `BEGIN` / `COMMIT` / `ROLLBACK` with in-memory undo log |

## Quick Start

```bash
# Build and run the REPL
go run cmd/minidb/main.go

# Or build first
go build -o minidb cmd/minidb/main.go
./minidb
```

## Example Session

### Basic CRUD + New Types
```sql
minidb> CREATE TABLE users (id INT, name TEXT, age INT);
minidb> INSERT INTO users VALUES (1, 'karim', 30);
minidb> INSERT INTO users VALUES (2, 'hassan', 25);
minidb> SELECT * FROM users WHERE age > 25;
minidb> UPDATE users SET age = 31 WHERE id = 1;
minidb> DELETE FROM users WHERE id = 2;

-- FLOAT and BOOL columns
minidb> CREATE TABLE products (id INT, name TEXT, price FLOAT, active BOOL);
minidb> INSERT INTO products VALUES (1, 'apple', 1.99, TRUE);
minidb> INSERT INTO products VALUES (2, 'candy', 0.50, FALSE);
minidb> SELECT * FROM products WHERE price > 1.0;
minidb> SELECT * FROM products WHERE active = TRUE;
```

### Transactions (ACID)
```sql
-- Create a table for the demo
minidb> CREATE TABLE accts (id INT, bal INT);
minidb> INSERT INTO accts VALUES (1, 1000);
minidb> INSERT INTO accts VALUES (2, 500);

-- Successful multi-statement transaction
minidb> BEGIN;
transaction started
minidb(txn)> INSERT INTO accts VALUES (3, 250);
1 row inserted
minidb(txn)> UPDATE accts SET bal = 900 WHERE id = 1;
1 row(s) updated
minidb(txn)> COMMIT;
transaction committed
minidb> SELECT * FROM accts;   -- 3 rows; id=1 has bal=900

-- Transaction that is rolled back
minidb> BEGIN;
transaction started
minidb(txn)> DELETE FROM accts WHERE id = 3;
1 row(s) deleted
minidb(txn)> ROLLBACK;
transaction rolled back
minidb> SELECT * FROM accts;   -- still 3 rows (DELETE was undone)
```

> **Note:** The prompt changes to `minidb(txn)>` while inside an active transaction. Without `BEGIN`, every statement auto-commits (backward-compatible).

### JOIN Queries
```sql
minidb> CREATE TABLE users (id INT, name TEXT);
minidb> CREATE TABLE orders (id INT, user_id INT, item TEXT);
minidb> INSERT INTO users VALUES (1, 'Alice');
minidb> INSERT INTO users VALUES (2, 'Bob');
minidb> INSERT INTO orders VALUES (1, 1, 'book');
minidb> INSERT INTO orders VALUES (2, 1, 'pen');
minidb> INSERT INTO orders VALUES (3, 99, 'ghost');  -- no matching user

-- INNER JOIN: only matched rows
minidb> SELECT * FROM orders INNER JOIN users ON orders.user_id = users.id;
+----------+-----------+-------------+----------+-----------+
| orders.id| orders.user_id | orders.item | users.id | users.name|
+----------+-----------+-------------+----------+-----------+
| 1        | 1         | book        | 1        | Alice     |
| 2        | 1         | pen         | 1        | Alice     |
+----------+-----------+-------------+----------+-----------+
2 row(s)

-- LEFT JOIN: all left rows, NULL where no match
minidb> SELECT * FROM orders LEFT JOIN users ON orders.user_id = users.id;
3 row(s)  -- ghost order appears with NULL user columns

minidb> \tables
minidb> \desc orders
minidb> \quit
```

## Running Tests

```bash
# All tests
go test ./tests/... -v

# Transaction (ACID) tests
go test ./tests/ -run TestTransaction -v   # all transaction tests
go test ./tests/ -run TestParseBegin       # parser: BEGIN
go test ./tests/ -run TestParseCommit      # parser: COMMIT
go test ./tests/ -run TestParseRollback    # parser: ROLLBACK

# JOIN tests
go test ./tests/ -run TestEngineInnerJoin
go test ./tests/ -run TestEngineLeftJoin
go test ./tests/ -run TestParseInnerJoin
go test ./tests/ -run TestParseLeftJoin

# Type tests
go test ./tests/ -run TestFloat
go test ./tests/ -run TestBool

# Legacy test suites
go test ./tests/ -run TestBTree
go test ./tests/ -run TestWAL
go test ./tests/ -run TestParser
go test ./tests/ -run TestEngine

# Benchmarks
go test ./tests/ -bench=. -benchmem
```

## Architecture Deep Dive

### Data Flow: INSERT INTO users VALUES (1, 'Alice', 30)

```
1. REPL reads line
2. Parser.ParseSQL() → InsertStmt AST
3. Executor.executeInsert():
   a. Look up "users" schema in Catalog
   b. Validate column count
   c. If TxManager.IsActive():
      → WAL.LogInsert(activeTxID, ...)   ← shared TxID from BEGIN
      → BTree.Insert(pk, encodedRow)
      → TxManager.RecordInsert()         ← undo op saved for possible ROLLBACK
   d. Else (auto-commit):
      → WAL.Begin() → new TxID
      → WAL.LogInsert() → BTree.Insert() → WAL.Commit()
4. ResultSet("1 row inserted") → REPL prints it
```

### Data Flow: ROLLBACK

```
1. REPL reads "ROLLBACK"
2. Parser.ParseSQL() → RollbackStmt AST
3. Executor.executeRollback():
   a. TxManager.Rollback()
   b. Walk undo log in reverse (last-in, first-out):
      - UndoInsert  → tree.Delete(key)
      - UndoDelete  → tree.Insert(key, oldValue)
      - UndoUpdate  → tree.Delete(key) + tree.Insert(key, oldValue)
   c. WAL.Abort(txID, nil)  ← writes ABORT record; undo already done in memory
4. ResultSet("transaction rolled back") → REPL prints it
```

### Data Flow: SELECT * FROM users WHERE age > 25

```
1. Parser.ParseSQL() → SelectStmt{Table:"users", Where: age>25}
2. Executor.executeSelect():
   a. Look up schema in Catalog → get rootPageID
   b. Decide: WHERE is "age > 25" (not a PK equality) → SeqScan
   c. btree.Scan(rootPageID, 0, MaxInt64) → iterator over all leaf nodes
   d. For each (key, encodedRow): decode row, evaluate WHERE predicate
   e. Collect matching rows, apply ORDER BY + LIMIT
   f. Apply column projection
3. Return ResultSet → REPL prints ASCII table
```

### WAL Recovery (crash safety)

If the process crashes mid-transaction:
1. **Analysis**: scan log → find active (uncommitted) transactions
2. **Redo**: replay all log records → restore state as of crash
3. **Undo**: reverse all uncommitted transaction changes

### B+ Tree Structure

```
                    [50 | 150]              ← root (internal)
                   /     |      \
            [20|30]    [80|100]  [180|200]  ← internal nodes
           /  |  \    /  |   \   ...
         [.] [.] [.][.] [.] [.]            ← leaf nodes (hold data)
              ↑─────────────────↑ linked list for range scans
```

## Supported SQL Syntax

```sql
-- Data Definition
CREATE TABLE name (col1 INT, col2 TEXT, col3 FLOAT, col4 BOOL);

-- Data Manipulation (DML)
INSERT INTO table_name VALUES (v1, v2, ...);
SELECT * FROM table_name [WHERE expr] [ORDER BY col [ASC|DESC]] [LIMIT n];
SELECT col1, col2 FROM table_name [WHERE expr];
UPDATE table_name SET col=val [WHERE expr];
DELETE FROM table_name [WHERE expr];

-- Joins
SELECT * FROM t1 INNER JOIN t2 ON t1.col = t2.col;
SELECT * FROM t1 LEFT JOIN t2 ON t1.col = t2.col;
SELECT * FROM t1 JOIN t2 ... -- defaults to INNER JOIN

-- Indexes
CREATE [UNIQUE] INDEX index_name ON table_name (column_name);
DROP INDEX index_name ON table_name;
SHOW INDEXES [FROM table_name];

-- Transactions (ACID)
BEGIN [TRANSACTION];
COMMIT [TRANSACTION];
ROLLBACK [TRANSACTION];

-- WHERE operators
=  !=  <  >  <=  >=  AND  OR  NOT

-- Data types
INT    -- 64-bit signed integer
TEXT   -- variable-length string
FLOAT  -- 64-bit IEEE 754 double (literals: 3.14, -0.5)
BOOL   -- boolean (literals: TRUE, FALSE)
```

## Key Design Decisions

1.  **Page size:** 4096 bytes (matches OS virtual memory page = efficient I/O).
2.  **Index structure:** B+ Tree (O(log n) point lookup + O(log n + k) range scan).
3.  **Cache policy:** LRU (simple, effective for most workloads).
4.  **Write-Ahead Log (WAL):** Ensures durability (so crash recovery is possible) and allows transactions to be rolled back.
5.  **No query optimizer (yet):** Queries are executed using simple heuristics (e.g. use an index if `WHERE primary_key = lit` or `WHERE indexed_col = lit`, otherwise sequential scan).
6.  **Secondary Indexes:** Stored as separate B+ trees where the key is the indexed column value and the value is the primary key. Planners transparently use them for O(log n) equality lookups.

## Extending MiniDB

### 1. Extensibility
Because it's written from scratch and highly modular, you can easily add:
- **String indexing** (B-tree needs a string comparator).
- **Secondary Indexes** (add a catalog entry mapping `colName -> BTreeRoot`, intercept `executeSelect` to use it).
- **Advanced joins** (Hash Join or Sort-Merge Join).

## Changelog

- **`COMMIT [TRANSACTION]`** — writes WAL COMMIT record + `fsync`, finalises transaction
- **`ROLLBACK [TRANSACTION]`** — applies undo log in reverse, writes WAL ABORT record
- Auto-commit preserved — every DML without `BEGIN` still auto-begins and auto-commits

#### Files Changed
| File | Change |
|---|---|
| `internal/txn/manager.go` | **[NEW]** `TxManager`: `Begin()`, `Commit()`, `Rollback()`, undo log, `RecordInsert/Update/Delete()` |
| `internal/parser/lexer.go` | 4 new tokens: `BEGIN`, `COMMIT`, `ROLLBACK`, `TRANSACTION` |
| `internal/parser/ast.go` | 3 new AST nodes: `BeginStmt`, `CommitStmt`, `RollbackStmt` |
| `internal/parser/parser.go` | `parseBegin`, `parseCommit`, `parseRollback`; dispatch in `parseStatement` |
| `internal/engine/executor.go` | `txm *TxManager` field; `executeBegin/Commit/Rollback`; dual auto-commit/explicit mode in INSERT/UPDATE/DELETE |
| `internal/wal/wal.go` | `Abort()` accepts `nil` tree (caller-managed undo); cleans `txnTable` on abort |
| `cmd/minidb/main.go` | Transaction-aware prompt `minidb(txn)>`; BEGIN/COMMIT/ROLLBACK in help + `looksIncomplete` |
| `tests/transaction_test.go` | **[NEW]** 12 tests: begin/commit, rollback insert/update/delete, multi-op, auto-commit, error cases, parser tests |

---

### v2.0 — JOIN + FLOAT/BOOL Types (2026-03-19)

#### New Data Types
| Type | Storage | Literals |
|---|---|---|
| `FLOAT` | 8-byte IEEE 754 (tag `2` in WAL) | `3.14`, `-0.5` |
| `BOOL` | 1 byte (tag `3` in WAL) | `TRUE`, `FALSE` |

#### New SQL Features
- **`INNER JOIN`** — returns only rows with matching ON condition (Nested Loop Join)
- **`LEFT JOIN`** — returns all left rows; unmatched right columns are `NULL`
- **Bare `JOIN`** — defaults to `INNER JOIN`
- **Qualified column references** — `table.column` syntax in SELECT lists and ON/WHERE clauses
- **Float literals** — `3.14`, `-1.5` parsed as `float64`
- **Boolean literals** — `TRUE` / `FALSE` parsed as `bool`

#### Files Changed
| File | Change |
|---|---|
| `internal/parser/lexer.go` | 10 new tokens: `FLOAT`, `BOOL`, `TRUE`, `FALSE`, `JOIN`, `ON`, `INNER`, `LEFT`, `FLOAT_LIT` |
| `internal/parser/ast.go` | `DataTypeFloat`, `DataTypeBool`, `JoinClause`, `QualifiedRef` nodes |
| `internal/parser/parser.go` | `parseDataType`, `parsePrimary`, `parseColumnList`, `parseSelect` extended |
| `internal/engine/executor.go` | `PackRowBytes`/`UnpackRowBytes` for FLOAT/BOOL; `executeJoinSelect` (NLJ) |
| `internal/engine/result.go` | `formatValue`, `valuesEqual`, `compareValues` for `float64`/`bool`; `qualifiedRefExpr` |
| `tests/parser_test.go` | 6 new tests for new types and JOIN parsing |
| `tests/engine_test.go` | 7 new integration tests for FLOAT/BOOL/JOIN execution |

---

## References

- [CMU 15-445 Database Systems](https://15445.cs.cmu.edu) — free lectures + labs
- *Database Internals* — Alex Petrov (O'Reilly)
- [BoltDB](https://github.com/etcd-io/bbolt) — real B+ tree in Go (study the source)
- *Designing Data-Intensive Applications* — Kleppmann
