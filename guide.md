# MiniDB — Storage Engine in Go
> A mini SQLite/Postgres-style database engine built from scratch in Go.
> Components: B+ Tree Index · WAL · SQL Parser · Query Engine

---

## Architecture Overview

```
┌─────────────────────────────────────────────────┐
│                  SQL Interface                   │
│            (REPL / TCP connection)               │
└────────────────────┬────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────┐
│                 SQL Parser                       │
│   Tokenizer → AST (SELECT/INSERT/UPDATE/DELETE)  │
└────────────────────┬────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────┐
│               Query Engine                       │
│   Planner → Optimizer → Executor                 │
└──────┬─────────────────────────┬────────────────┘
       │                         │
┌──────▼──────┐         ┌────────▼──────────────┐
│  B+ Tree    │         │    WAL (Write-Ahead    │
│  Index      │         │       Log)             │
│  Engine     │         │                        │
└──────┬──────┘         └────────┬───────────────┘
       │                         │
┌──────▼─────────────────────────▼───────────────┐
│              Page / Disk Manager                │
│        (fixed-size pages, file I/O)             │
└─────────────────────────────────────────────────┘
```

---

## Project Structure

```
storage-engine/
├── cmd/
│   └── minidb/
│       └── main.go              # Entry point (REPL)
├── internal/
│   ├── disk/
│   │   ├── page.go              # Page layout (4KB fixed size)
│   │   └── disk_manager.go      # Read/write pages from disk
│   ├── buffer/
│   │   └── buffer_pool.go       # Buffer pool manager (LRU cache)
│   ├── btree/
│   │   ├── btree.go             # B+ tree core logic
│   │   ├── node.go              # Internal / leaf node structures
│   │   └── iterator.go          # Range scan iterator
│   ├── wal/
│   │   ├── wal.go               # WAL writer + reader
│   │   └── record.go            # Log record format
│   ├── catalog/
│   │   └── catalog.go           # Table/column metadata store
│   ├── parser/
│   │   ├── lexer.go             # Tokenizer
│   │   ├── parser.go            # Recursive descent parser
│   │   └── ast.go               # AST node definitions
│   └── engine/
│       ├── planner.go           # Logical plan builder
│       ├── executor.go          # Plan executor
│       └── result.go            # ResultSet type
├── tests/
│   ├── btree_test.go
│   ├── wal_test.go
│   ├── parser_test.go
│   └── engine_test.go
├── guide.md                     # This file
└── go.mod
```

---

## Phase 1 — Disk & Page Manager

**Goal:** Everything is stored in fixed-size pages on disk (like real databases).

### Concepts
- **Page size:** 4096 bytes (4KB) — matches OS memory page size for efficiency.
- **Page ID:** `uint32` — offset into the file is `pageID * pageSize`.
- **Disk Manager:** opens a `.db` file, reads/writes raw pages.

### What to implement
- `page.go` — `Page` struct (raw `[4096]byte` + dirty/pin flags)
- `disk_manager.go` — `ReadPage(id)`, `WritePage(id, page)`, `AllocatePage() id`

### Key insight
Never directly write to disk mid-operation. Use a **buffer pool** that caches pages in memory and flushes dirty pages when evicted.

---

## Phase 2 — Buffer Pool Manager

**Goal:** Keep hot pages in memory, evict cold ones when full.

### Concepts
- **Frame:** A slot in memory that holds one page.
- **Page Table:** `map[PageID]FrameID` — tracks which pages are in memory.
- **Eviction policy:** LRU (Least Recently Used) — simplest correct policy.
- **Pin count:** A page with `pinCount > 0` cannot be evicted (it's in use).

### What to implement
- `NewBufferPool(poolSize int, dm DiskManager)` 
- `FetchPage(id PageID) *Page`
- `UnpinPage(id PageID, isDirty bool)`
- `FlushPage(id PageID)`
- LRU list (doubly-linked list or use `container/list`)

---

## Phase 3 — B+ Tree Index

**Goal:** The core data structure. All table data lives in a B+ tree (clustered index, like InnoDB).

### Why B+ Tree (not B-Tree)?
- **Leaf nodes are linked** → efficient range scans (`WHERE id BETWEEN 1 AND 100`)
- **Internal nodes only hold keys** → higher fanout → fewer I/O levels
- All data lives in leaf nodes

### Node Layout (on a page)
```
┌─────────────────────────────────────────────┐
│ isLeaf | numKeys | keys[] | values[]/ptrs[] │
│   1B   |   2B    |  n*8B  |     n*8B        │
└─────────────────────────────────────────────┘
```
- **Internal node:** `keys[i]` separates children pointers `ptrs[i]` and `ptrs[i+1]`
- **Leaf node:** `keys[i]` → `values[i]` (row data or RID), plus `nextLeaf` pointer

### Operations to implement
1. **Search** — traverse from root to leaf (`O(log n)`)
2. **Insert** — find leaf → insert → split if overflow → propagate up
3. **Delete** — find leaf → remove → merge/borrow if underflow → propagate up
4. **Range Scan** — find start leaf → walk `nextLeaf` chain

### Order (fanout)
- Choose order `t` such that a node fits in one page (4KB).
- A node holds between `t-1` and `2t-1` keys.
- For 8-byte keys and 8-byte pointers: `t ≈ 255`

### Splitting (insert)
```
[1, 2, 3, 4, 5]  ← overflow (max 4 keys)
       ↓ split at median (key=3)
    [3] pushed to parent
   /       \
[1,2]    [3,4,5]
```

---

## Phase 4 — Write-Ahead Log (WAL)

**Goal:** Crash safety. Before any page is modified, write a log record first.

### Why WAL?
- If the process crashes mid-write, the log lets us **redo committed** transactions and **undo uncommitted** ones.
- This is the foundation of ACID durability.

### Log Record Format
```
┌──────┬──────┬──────┬──────────┬────────┐
│ LSN  │ TxID │ Type │ PageID   │ Data   │
│  8B  │  8B  │  1B  │    4B    │ variable│
└──────┴──────┴──────┴──────────┴────────┘
```
- **LSN** (Log Sequence Number): monotonically increasing, identifies each record
- **Type:** `BEGIN`, `INSERT`, `UPDATE`, `DELETE`, `COMMIT`, `ABORT`, `CHECKPOINT`

### Recovery protocol (ARIES simplified)
1. **Analysis pass** — scan log to find active transactions at crash
2. **Redo pass** — replay all log records from last checkpoint forward
3. **Undo pass** — rollback uncommitted transactions in reverse order

### What to implement
- `WAL` struct — append-only file, sequential writes
- `AppendRecord(rec LogRecord) LSN`
- `ReadFrom(lsn LSN) []LogRecord` — for recovery
- Checkpoint support (write dirty page table to log)

---

## Phase 5 — SQL Parser

**Goal:** Turn raw SQL strings into an Abstract Syntax Tree (AST).

### Supported SQL Subset (start here)
```sql
CREATE TABLE users (id INT, name TEXT, age INT);
INSERT INTO users VALUES (1, 'Alice', 30);
SELECT * FROM users WHERE id = 1;
SELECT name, age FROM users WHERE age > 25 ORDER BY age;
UPDATE users SET age = 31 WHERE id = 1;
DELETE FROM users WHERE id = 1;
```

### Lexer (Tokenizer)
Turns `SELECT name FROM users` into:
```
[SELECT] [IDENT:name] [FROM] [IDENT:users] [EOF]
```
Token types:
```go
type TokenType int
const (
    SELECT, INSERT, UPDATE, DELETE, CREATE, FROM, WHERE,
    INTO, VALUES, SET, TABLE, ORDER, BY, ASC, DESC,
    IDENT, INT_LIT, STR_LIT, STAR,
    EQ, NEQ, LT, GT, LTE, GTE, AND, OR, NOT,
    LPAREN, RPAREN, COMMA, SEMICOLON, EOF,
    ...
)
```

### Parser — Recursive Descent
```go
func (p *Parser) ParseStatement() (Statement, error)
func (p *Parser) parseSelect() (*SelectStmt, error)
func (p *Parser) parseWhere() (Expr, error)
func (p *Parser) parseExpr() (Expr, error)  // handles AND/OR
func (p *Parser) parsePrimary() (Expr, error) // handles comparisons
```

### AST Nodes
```go
type SelectStmt struct {
    Columns  []string
    Table    string
    Where    Expr        // nil if no WHERE
    OrderBy  *OrderBy
    Limit    int
}

type BinaryExpr struct {
    Left  Expr
    Op    string   // "=", ">", "AND", "OR", ...
    Right Expr
}

type Literal struct {
    Value interface{}  // int64 or string
}

type ColumnRef struct {
    Name string
}
```

---

## Phase 6 — Query Engine

**Goal:** Execute AST statements against the storage engine.

### Pipeline
```
SQL String
    → Lexer → Tokens
    → Parser → AST
    → Planner → LogicalPlan
    → Executor → ResultSet
```

### Logical Plans (operators)
```go
type SeqScan struct { Table string; Filter Expr }         // full table scan
type IndexScan struct { Table string; Key interface{} }   // B+ tree lookup
type Filter struct { Child Plan; Predicate Expr }
type Projection struct { Child Plan; Columns []string }
```

### Executor
- **SeqScan:** iterate all leaf nodes of the B+ tree
- **IndexScan:** point lookup via `btree.Search(key)`
- **Filter:** evaluate `WHERE` expression row-by-row
- **Projection:** pick only requested columns

### Expression Evaluator
```go
func Eval(expr Expr, row Row) (Value, error) {
    switch e := expr.(type) {
    case *Literal:   return e.Value, nil
    case *ColumnRef: return row.Get(e.Name), nil
    case *BinaryExpr:
        l, _ := Eval(e.Left, row)
        r, _ := Eval(e.Right, row)
        return applyOp(e.Op, l, r), nil
    }
}
```

---

## Build Order (Recommended)

| Phase | Component         | Dependencies         | Est. Complexity |
|-------|-------------------|----------------------|-----------------|
| 1     | Disk Manager      | none                 | Low             |
| 2     | Buffer Pool       | Disk Manager         | Medium          |
| 3     | B+ Tree           | Buffer Pool          | **High**        |
| 4     | WAL               | Disk Manager         | Medium-High     |
| 5     | SQL Parser        | none (pure logic)    | Medium          |
| 6     | Query Engine      | B+ Tree + Parser     | Medium          |
| 7     | REPL / shell      | Query Engine         | Low             |

> **Tip:** You can build the Parser (Phase 5) early since it has no dependencies. It's a great confidence booster.

---

## Testing Strategy

### Unit tests per phase
```bash
go test ./internal/disk/...      # Phase 1
go test ./internal/buffer/...    # Phase 2
go test ./internal/btree/...     # Phase 3 (most critical)
go test ./internal/wal/...       # Phase 4
go test ./internal/parser/...    # Phase 5
go test ./internal/engine/...    # Phase 6
```

### Critical B+ Tree tests
- Insert 1..1000, verify all found
- Random insert/delete, verify tree invariants
- Range scan `[100, 200]`, verify correct order
- Split & merge corner cases (fill exactly to limit, then +1)

### WAL crash recovery test
- Write 100 rows, crash mid-way (kill process after 50 WAL records), restart, verify consistency

---

## Data Types (start simple)

```go
type DataType int
const (
    TypeInt  DataType = iota  // int64
    TypeText                   // string, max 255 bytes
)
```
Extend later with: `FLOAT`, `BOOL`, `BLOB`, `TIMESTAMP`

---

## Row Format (leaf node value)

```
┌────────────────────────────────────┐
│ colCount | col1_len | col1_data | … │
│    2B    |    2B    |  variable  |   │
└────────────────────────────────────┘
```
Keep it simple — variable-length rows in leaf pages.

---

## Key Milestones

- [ ] `go mod init minidb` + project skeleton
- [ ] Disk Manager: read/write pages from `.db` file
- [ ] Buffer Pool: LRU eviction, pin/unpin working
- [ ] B+ Tree: insert + search passing all tests  
- [ ] B+ Tree: delete + range scan working
- [ ] WAL: append records, recover from crash simulation
- [ ] Parser: lex + parse SELECT/INSERT/CREATE TABLE
- [ ] Parser: WHERE expressions with AND/OR
- [ ] Executor: SeqScan + IndexScan + Filter
- [ ] REPL: interactive SQL shell
- [ ] Integration test: full SQL round-trip

---

## References & Inspiration

- **CMU 15-445** — Database Systems course (free lectures + labs): https://15445.cs.cmu.edu
- **"Database Internals"** — Alex Petrov (O'Reilly) — best book on storage engines
- **"Designing Data-Intensive Applications"** — Kleppmann — broader context
- **BoltDB** (Go) — study its B+ tree source: https://github.com/etcd-io/bbolt
- **SQLite source** — specifically `btree.c` and `wal.c`
- **TiKV** — production B+ tree in Rust (for architecture inspiration)
- **LevelDB** — LSM-tree alternative (different approach, good contrast)

---

## Go-Specific Notes

- Use `encoding/binary` with `binary.LittleEndian` for all page serialization
- No CGo, no external DB libraries — pure Go only
- Use `sync.RWMutex` for buffer pool concurrency (later phases)
- `unsafe.Pointer` can help with zero-copy page parsing but start safe
- Keep interfaces minimal: `Tree interface { Get, Put, Delete, Scan }`

---

*Start with Phase 1. Each phase builds on the previous. The B+ Tree is the hardest part — give it the most time.*
