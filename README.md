# MiniDB — A Storage Engine in Go

A mini SQLite/Postgres-style database engine built from scratch in Go.

## Components
| Component | File(s) | What it does |
|---|---|---|
| **Disk Manager** | `internal/disk/` | Raw page I/O — reads/writes 4KB blocks to a `.db` file |
| **Buffer Pool** | `internal/buffer/` | LRU in-memory page cache — avoids disk I/O on hot pages |
| **B+ Tree** | `internal/btree/` | Core index structure — sorted key-value storage with range scans |
| **WAL** | `internal/wal/` | Write-Ahead Log — crash recovery using ARIES (redo/undo) |
| **SQL Parser** | `internal/parser/` | Lexer + recursive descent parser → AST |
| **Query Engine** | `internal/engine/` | Executes AST against storage, handles IndexScan vs SeqScan |
| **Catalog** | `internal/catalog/` | Table and column schema metadata (persisted as JSON) |
| **REPL** | `cmd/minidb/` | Interactive SQL shell |

## Quick Start

```bash
# Build and run the REPL
go run cmd/minidb/main.go

# Or build first
go build -o minidb cmd/minidb/main.go
./minidb
```

## Example Session

```sql
minidb> CREATE TABLE users (id INT, name TEXT, age INT);
minidb> INSERT INTO users VALUES (1, 'karim', 30);
minidb> INSERT INTO users VALUES (2, 'hassan', 25);
minidb> INSERT INTO users VALUES (3, 'hello', 35);
minidb> SELECT * FROM users;
+----+-------+-----+
| id | name  | age |
+----+-------+-----+
| 1  | karim | 30  |
| 2  | mokh   | 25  |
| 3  | hello | 35  |
+----+-------+-----+
3 row(s)

minidb> SELECT * FROM users WHERE age > 25;
minidb> SELECT name FROM users ORDER BY age DESC;
minidb> UPDATE users SET age = 31 WHERE id = 1;
minidb> DELETE FROM users WHERE id = 2;
minidb> \tables
minidb> \desc users
minidb> \quit
```

## Running Tests

```bash
# All tests
go test ./tests/...

# Specific test file
go test ./tests/ -run TestBTree
go test ./tests/ -run TestWAL
go test ./tests/ -run TestParser
go test ./tests/ -run TestEngine

# With verbose output
go test ./tests/ -v -run TestBTreeInsert1000

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
   c. WAL.Begin() → get TxID
   d. WAL.LogInsert() → write log record to .wal file (BEFORE modifying tree!)
   e. BTree.Insert(pk=1, encodedRow) → find leaf, insert, split if needed
   f. WAL.Commit() → flush commit record to .wal file
4. ResultSet("1 row inserted") → REPL prints it
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

## Key Design Decisions

| Decision | Choice | Why |
|---|---|---|
| Page size | 4096 bytes | Matches OS virtual memory page = efficient I/O |
| Index structure | B+ Tree | O(log n) point lookup + O(log n + k) range scan |
| Cache policy | LRU | Simple, effective for most workloads |
| Recovery | ARIES (simplified) | Industry standard for WAL-based recovery |
| Parser | Recursive descent | Simple, readable, easy to extend |
| Row storage | In-memory map + int64 key | Simplified for learning; real DB uses heap pages |

## Extending MiniDB

- **Composite keys**: modify `btree.Key` to be a `[]byte` or struct
- **Heap files**: store full variable-length rows in separate pages
- **JOIN support**: add `HashJoin` or `NestedLoopJoin` plan node
- **Transactions**: add a lock manager for isolation (currently no locking)
- **FLOAT/BOOL types**: extend `DataType` enum and row codec
- **Index on non-PK**: build a secondary B+ tree per indexed column

## References

- [CMU 15-445 Database Systems](https://15445.cs.cmu.edu) — free lectures + labs
- *Database Internals* — Alex Petrov (O'Reilly)
- [BoltDB](https://github.com/etcd-io/bbolt) — real B+ tree in Go (study the source)
- *Designing Data-Intensive Applications* — Kleppmann
