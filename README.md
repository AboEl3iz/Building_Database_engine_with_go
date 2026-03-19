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
| **Query Engine** | `internal/engine/` | Executes AST against storage, IndexScan / SeqScan / NestedLoopJoin |
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

# Specific feature tests
go test ./tests/ -run TestFloat     # FLOAT type tests
go test ./tests/ -run TestBool      # BOOL type tests
go test ./tests/ -run TestJoin      # JOIN tests (general)
go test ./tests/ -run TestEngineInnerJoin
go test ./tests/ -run TestEngineLeftJoin
go test ./tests/ -run TestParseInnerJoin
go test ./tests/ -run TestParseLeftJoin

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

## Supported SQL Syntax

```sql
-- Data Definition
CREATE TABLE name (col1 INT, col2 TEXT, col3 FLOAT, col4 BOOL);

-- Data Manipulation
INSERT INTO name VALUES (1, 'text', 3.14, TRUE);
SELECT * FROM name [WHERE expr] [ORDER BY col [ASC|DESC]] [LIMIT n];
SELECT col1, col2 FROM name;
UPDATE name SET col = val [WHERE expr];
DELETE FROM name [WHERE expr];

-- JOIN queries
SELECT * FROM left_table INNER JOIN right_table ON left_table.col = right_table.col;
SELECT * FROM left_table LEFT JOIN right_table ON left_table.col = right_table.col;
SELECT * FROM left_table JOIN right_table ON left_table.col = right_table.col;  -- defaults to INNER

-- WHERE operators
=  !=  <  >  <=  >=  AND  OR  NOT

-- Data types
INT    -- 64-bit signed integer
TEXT   -- variable-length string
FLOAT  -- 64-bit IEEE 754 double (literals: 3.14, -0.5)
BOOL   -- boolean (literals: TRUE, FALSE)
```

## Key Design Decisions

| Decision | Choice | Why |
|---|---|---|
| Page size | 4096 bytes | Matches OS virtual memory page = efficient I/O |
| Index structure | B+ Tree | O(log n) point lookup + O(log n + k) range scan |
| Cache policy | LRU | Simple, effective for most workloads |
| Recovery | ARIES (simplified) | Industry standard for WAL-based recovery |
| Parser | Recursive descent | Simple, readable, easy to extend |
| Row storage | In-memory cache + binary WAL encoding | Full type fidelity (INT/TEXT/FLOAT/BOOL) |
| JOIN algorithm | Nested Loop Join | Simple O(n×m); sufficient for learning purposes |

## Extending MiniDB

- **Composite keys**: modify `btree.Key` to be a `[]byte` or struct
- **Heap files**: store full variable-length rows in separate pages
- **Hash/Merge Join**: replace Nested Loop Join for better performance on large tables
- **Transactions**: add a lock manager for isolation (currently no locking)
- **Index on non-PK**: build a secondary B+ tree per indexed column
- **DECIMAL type**: extend `DataType` with fixed-precision arithmetic

## Changelog

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
