package engine

import (
	"encoding/binary"
	"fmt"
	"math"
	"minidb/internal/btree"
	"minidb/internal/buffer"
	"minidb/internal/catalog"
	"minidb/internal/lock"
	"minidb/internal/parser"
	"minidb/internal/txn"
	"minidb/internal/wal"
	"sort"
	"strings"
)

// Executor executes parsed SQL statements against the storage engine.
//
// It is the bridge between the high-level SQL world (AST) and the
// low-level storage world (B+ tree, buffer pool, WAL).
//
// Execution pipeline for a SELECT:
//  1. Look up table schema in catalog
//  2. Decide: use IndexScan (WHERE pk = ?) or SecondaryIndexScan or SeqScan
//  3. For each matching row: apply WHERE filter
//  4. Apply projection (pick only requested columns)
//  5. Apply ORDER BY + LIMIT
//  6. Return ResultSet
type Executor struct {
	bp      *buffer.BufferPool
	cat     *catalog.Catalog
	wal     *wal.WAL
	txm       *txn.TxManager          // session-level transaction manager
	trees     map[string]*btree.BTree // table name → B+ tree
	indexes   map[string]*btree.BTree // index name → secondary B+ tree
	rowStores map[string]*RowStore    // table name → heap row store
}

// NewExecutor creates an Executor with the given dependencies.
func NewExecutor(bp *buffer.BufferPool, cat *catalog.Catalog, w *wal.WAL, txm ...*txn.TxManager) *Executor {
	var tm *txn.TxManager
	if len(txm) > 0 && txm[0] != nil {
		tm = txm[0]
	} else {
		tm = txn.NewTxManager(w) // default: create one automatically
	}
	return &Executor{
		bp:      bp,
		cat:     cat,
		wal:       w,
		txm:       tm,
		trees:     make(map[string]*btree.BTree),
		indexes:   make(map[string]*btree.BTree),
		rowStores: make(map[string]*RowStore),
	}
}

// Execute runs a SQL statement and returns a ResultSet (or an error).
// This is the main entry point for the REPL.
func (e *Executor) Execute(stmt parser.Statement) (*ResultSet, error) {
	switch s := stmt.(type) {
	case *parser.CreateTableStmt:
		return e.executeCreate(s)
	case *parser.InsertStmt:
		return e.executeInsert(s)
	case *parser.SelectStmt:
		if s.Join != nil {
			return e.executeJoinSelect(s)
		}
		return e.executeSelect(s)
	case *parser.UpdateStmt:
		return e.executeUpdate(s)
	case *parser.DeleteStmt:
		return e.executeDelete(s)

	// ---- Transaction control ----
	case *parser.BeginStmt:
		return e.executeBegin()
	case *parser.CommitStmt:
		return e.executeCommit()
	case *parser.RollbackStmt:
		return e.executeRollback()

	// ---- Index control ----
	case *parser.CreateIndexStmt:
		return e.executeCreateIndex(s)
	case *parser.DropIndexStmt:
		return e.executeDropIndex(s)
	case *parser.ShowIndexesStmt:
		return e.executeShowIndexes(s)

	default:
		return nil, fmt.Errorf("executor: unsupported statement type %T", stmt)
	}
}

// ---- Transaction control helpers ----

func (e *Executor) executeBegin() (*ResultSet, error) {
	if err := e.txm.Begin(); err != nil {
		return nil, err
	}
	rs := NewResultSet([]string{"result"})
	rs.AddRow(Row{"result": "transaction started"})
	return rs, nil
}

func (e *Executor) executeCommit() (*ResultSet, error) {
	if err := e.txm.Commit(); err != nil {
		return nil, err
	}
	rs := NewResultSet([]string{"result"})
	rs.AddRow(Row{"result": "transaction committed"})
	return rs, nil
}

func (e *Executor) executeRollback() (*ResultSet, error) {
	if err := e.txm.Rollback(); err != nil {
		return nil, err
	}
	rs := NewResultSet([]string{"result"})
	rs.AddRow(Row{"result": "transaction rolled back"})
	return rs, nil
}

// IsInTransaction reports whether a user transaction is currently open.
// Used by the REPL to show a different prompt.
func (e *Executor) IsInTransaction() bool {
	return e.txm.IsActive()
}

// ---- CREATE TABLE ----

func (e *Executor) executeCreate(stmt *parser.CreateTableStmt) (*ResultSet, error) {
	if e.cat.TableExists(stmt.TableName) {
		return nil, fmt.Errorf("executor: table %q already exists", stmt.TableName)
	}

	// Create the primary B+ tree
	tree, err := btree.NewBTree(e.bp)
	if err != nil {
		return nil, fmt.Errorf("executor: cannot create B+ tree: %w", err)
	}

	// Create a new RowStore for this table
	rowStore, heapPageID, err := NewRowStore(e.bp)
	if err != nil {
		return nil, fmt.Errorf("executor: cannot create RowStore: %w", err)
	}

	// Register in catalog with the tree's root page ID and heap page ID
	if err := e.cat.CreateTable(stmt.TableName, stmt.Columns, tree.RootPageID(), heapPageID); err != nil {
		return nil, fmt.Errorf("executor: catalog error: %w", err)
	}

	// Cache the tree and row store
	e.trees[stmt.TableName] = tree
	e.rowStores[stmt.TableName] = rowStore

	rs := NewResultSet([]string{"result"})
	rs.AddRow(Row{"result": fmt.Sprintf("Table %q created", stmt.TableName)})
	return rs, nil
}

// ---- CREATE INDEX ----
//
// A secondary index is a separate B+ tree where:
//   key   = indexed column value (encoded as int64)
//   value = primary key of the matching row
//
// On SELECT WHERE indexed_col = value:
//   1. Look up key in secondary index tree → get pk
//   2. Search primary tree with pk → get full row
//
// This gives O(log n) lookup vs O(n) sequential scan.

func (e *Executor) executeCreateIndex(stmt *parser.CreateIndexStmt) (*ResultSet, error) {
	schema, err := e.cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Verify column exists
	colIdx, ok := schema.ColIndex[stmt.Column]
	if !ok {
		return nil, fmt.Errorf("executor: column %q does not exist in table %q", stmt.Column, stmt.TableName)
	}

	// Allocate a new B+ tree for this secondary index
	idxTree, err := btree.NewBTree(e.bp)
	if err != nil {
		return nil, fmt.Errorf("executor: cannot create B+ tree for index %q: %w", stmt.IndexName, err)
	}

	// Register in catalog
	if err := e.cat.CreateIndex(stmt.TableName, stmt.IndexName, stmt.Column, idxTree.RootPageID(), stmt.Unique); err != nil {
		return nil, fmt.Errorf("executor: %w", err)
	}

	// Cache the secondary index tree
	e.indexes[stmt.IndexName] = idxTree

	// Populate index from existing rows (full table scan)
	primaryTree, err := e.getTree(stmt.TableName, schema)
	if err != nil {
		return nil, err
	}
	scanResults, err := btree.Scan(e.bp, primaryTree.RootPageID(), 0, math.MaxInt64)
	if err != nil {
		return nil, fmt.Errorf("executor: index backfill scan failed: %w", err)
	}

	colSchema := schema.Columns[colIdx]
	
	rowStore := e.getRowStore(stmt.TableName, schema)
	
	for _, sr := range scanResults {
		rowBytes, err := rowStore.Read(sr.Value)
		if err != nil {
			return nil, fmt.Errorf("executor: row read failed: %w", err)
		}
		row, err := UnpackRowBytes(schema, rowBytes)
		if err != nil {
			return nil, fmt.Errorf("executor: row unpack failed: %w", err)
		}
		idxKey := indexKeyFromVal(row[colSchema.Name])
		pk := sr.Key

		if stmt.Unique {
			// Check for duplicate before inserting
			// For UNIQUE index, look for any entry matching idxKey
			startKey := compositeIndexKey(idxKey, 0)
			endKey := compositeIndexKey(idxKey, math.MaxInt32)
			matches, _ := btree.Scan(e.bp, idxTree.RootPageID(), startKey, endKey)
			if len(matches) > 0 {
				// Clean up: drop the index we partially built
				e.cat.DropIndex(stmt.TableName, stmt.IndexName)
				delete(e.indexes, stmt.IndexName)
				return nil, fmt.Errorf("executor: UNIQUE constraint violation on index %q: duplicate value for column %q",
					stmt.IndexName, stmt.Column)
			}
		}
		
		compKey := compositeIndexKey(idxKey, pk)
		if err := idxTree.Insert(compKey, pk); err != nil {
			return nil, fmt.Errorf("executor: index backfill insert failed: %w", err)
		}
		// Update catalog if root changed during backfill
		newRoot := idxTree.RootPageID()
		if idxSch, _ := e.cat.GetIndex(stmt.TableName, stmt.IndexName); idxSch != nil && newRoot != idxSch.RootPageID {
			e.cat.UpdateIndexRootPageID(stmt.TableName, stmt.IndexName, newRoot)
		}
	}

	rs := NewResultSet([]string{"result"})
	rs.AddRow(Row{"result": fmt.Sprintf("Index %q created on %s(%s)", stmt.IndexName, stmt.TableName, stmt.Column)})
	return rs, nil
}

// compositeIndexKey packs a secondary index value (hash or direct) and a primary key into an int64.
// This allows a single secondary value to map to multiple primary keys in the BTree.
func compositeIndexKey(idxKey int64, pk int64) int64 {
	return (idxKey << 32) | (pk & 0xFFFFFFFF)
}

// ---- DROP INDEX ----

func (e *Executor) executeDropIndex(stmt *parser.DropIndexStmt) (*ResultSet, error) {
	if err := e.cat.DropIndex(stmt.TableName, stmt.IndexName); err != nil {
		return nil, fmt.Errorf("executor: %w", err)
	}
	delete(e.indexes, stmt.IndexName)
	rs := NewResultSet([]string{"result"})
	rs.AddRow(Row{"result": fmt.Sprintf("Index %q dropped", stmt.IndexName)})
	return rs, nil
}

// ---- SHOW INDEXES ----

func (e *Executor) executeShowIndexes(stmt *parser.ShowIndexesStmt) (*ResultSet, error) {
	cols := []string{"index_name", "table_name", "column", "unique"}
	rs := NewResultSet(cols)

	tables := e.cat.ListTables()
	for _, tname := range tables {
		if stmt.TableName != "" && tname != stmt.TableName {
			continue
		}
		schema, err := e.cat.GetTable(tname)
		if err != nil {
			continue
		}
		for _, idx := range schema.Indexes {
			rs.AddRow(Row{
				"index_name": idx.Name,
				"table_name": idx.TableName,
				"column":     idx.Column,
				"unique":     idx.Unique,
			})
		}
	}
	return rs, nil
}

// ---- Index helpers ----

// indexKeyFromVal encodes a column value as an int64 key for the secondary B+ tree.
// INT/FLOAT/BOOL are losslessly encoded; TEXT uses a stable hash (same as encodeRow).
func indexKeyFromVal(v Value) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case float64:
		return int64(math.Float64bits(val))
	case bool:
		if val {
			return 1
		}
		return 0
	case string:
		hash := int64(0)
		for j, c := range val {
			hash = hash*31 + int64(c)*int64(j+1)
		}
		return hash
	}
	return 0
}

// loadIndexTree returns the in-memory B+ tree for a secondary index.
// If it's not cached yet (e.g., after an engine restart), it opens it from the catalog's root page.
func (e *Executor) loadIndexTree(idx catalog.IndexSchema) *btree.BTree {
	if tree, ok := e.indexes[idx.Name]; ok {
		return tree
	}
	// Open from the persisted root page
	tree := btree.OpenBTree(e.bp, idx.RootPageID)
	e.indexes[idx.Name] = tree
	return tree
}

// findSecondaryIndexScan checks whether the WHERE clause is a simple
// `col = literal` equality on a column that has a secondary index.
// Returns the matching IndexSchema, the encoded lookup key, and true if found.
func (e *Executor) findSecondaryIndexScan(where parser.Expr, schema *catalog.TableSchema) (*catalog.IndexSchema, int64, bool) {
	if where == nil || len(schema.Indexes) == 0 {
		return nil, 0, false
	}
	bin, ok := where.(*parser.BinaryExpr)
	if !ok || bin.Op != "=" {
		return nil, 0, false
	}
	col, ok1 := bin.Left.(*parser.ColumnRef)
	lit, ok2 := bin.Right.(*parser.Literal)
	if !ok1 || !ok2 {
		return nil, 0, false
	}
	for i := range schema.Indexes {
		if schema.Indexes[i].Column == col.Name {
			idxKey := indexKeyFromVal(lit.Value)
			return &schema.Indexes[i], idxKey, true
		}
	}
	return nil, 0, false
}
// fmt.Printf("DEBUG executeInsert: inserted pk=%d rowEncoded=%d\n", pk, rowEncoded)

// ---- INSERT ----
//
// Row encoding: we encode the full row as a value in the B+ tree.
// The key is the first INT column (treated as primary key).
//
// Row binary format stored in the B+ tree value:
//   [col1_value(8B)] [col2_len(2B) col2_data(N B)] ...
//
// For simplicity, we store the entire row serialized into a single int64 hash
// for the B+ tree value field. A full implementation would use a separate
// heap file or inline row storage.
//
// SIMPLIFIED APPROACH: We use a Row Store pattern:
//   Key = value of first INT column (primary key)
//   Value = encoded row (all columns packed into 8 bytes or a page reference)
//
// For MiniDB, we pack all column values into a deterministic encoding
// and store them with the key. For TEXT, we store a hash (simplified).

func (e *Executor) executeInsert(stmt *parser.InsertStmt) (*ResultSet, error) {
	// 2PL: acquire EXCLUSIVE lock on the table (writes need exclusive access)
	if err := e.txm.AcquireLock(stmt.Table, lock.LockExclusive); err != nil {
		return nil, fmt.Errorf("executor: %w", err)
	}

	schema, err := e.cat.GetTable(stmt.Table)
	if err != nil {
		return nil, err
	}

	if len(stmt.Values) != len(schema.Columns) {
		return nil, fmt.Errorf("executor: INSERT has %d values but table has %d columns",
			len(stmt.Values), len(schema.Columns))
	}

	// Evaluate all literal values
	row := make(Row)
	for i, col := range schema.Columns {
		val, err := evalLiteral(stmt.Values[i])
		if err != nil {
			return nil, fmt.Errorf("executor: cannot evaluate value %d: %w", i, err)
		}
		row[col.Name] = val
	}

	// Find primary key (first INT column)
	pk, err := extractPrimaryKey(schema, row)
	if err != nil {
		return nil, err
	}

	// Encode row as bytes and append to row store
	rowStore := e.getRowStore(stmt.Table, schema)
	rowBytes := PackRowBytes(schema, row)
	rowEncoded, err := rowStore.Append(rowBytes)
	if err != nil {
		return nil, fmt.Errorf("executor: row append failed: %w", err)
	}

	// Get (or rebuild) the B+ tree for this table
	tree, err := e.getTree(stmt.Table, schema)
	if err != nil {
		return nil, err
	}

	if e.txm.IsActive() {
		// ---- Explicit transaction mode ----
		// Log to WAL under the shared transaction ID.
		if _, err = e.wal.LogInsert(e.txm.ActiveTxID(), schema.RootPageID, pk, rowEncoded); err != nil {
			return nil, fmt.Errorf("executor: WAL log failed: %w", err)
		}
		if err := tree.Insert(pk, rowEncoded); err != nil {
			return nil, fmt.Errorf("executor: insert failed: %w", err)
		}
		// Register undo op so ROLLBACK can reverse this insert.
		e.txm.RecordInsert(tree, pk)
	} else {
		// ---- Auto-commit mode (backward compatible) ----
		txID, err := e.wal.Begin()
		if err != nil {
			return nil, fmt.Errorf("executor: WAL begin failed: %w", err)
		}
		if _, err = e.wal.LogInsert(txID, schema.RootPageID, pk, rowEncoded); err != nil {
			return nil, fmt.Errorf("executor: WAL log failed: %w", err)
		}
		if err := tree.Insert(pk, rowEncoded); err != nil {
			e.wal.Abort(txID, tree)
			return nil, fmt.Errorf("executor: insert failed: %w", err)
		}
		if err := e.wal.Commit(txID); err != nil {
			return nil, fmt.Errorf("executor: WAL commit failed: %w", err)
		}
	}

	// If the root page changed (due to a split), update the catalog
	newRoot := tree.RootPageID()
	if newRoot != schema.RootPageID {
		e.cat.UpdateRootPageID(stmt.Table, newRoot)
	}

	// ---- Maintain secondary indexes ----
	for _, idx := range schema.Indexes {
		idxTree := e.loadIndexTree(idx)
		if idxTree == nil {
			continue
		}
		idxKey := indexKeyFromVal(row[idx.Column])
		// UNIQUE check
		if idx.Unique {
			startKey := compositeIndexKey(idxKey, 0)
			endKey := compositeIndexKey(idxKey, math.MaxInt32)
			matches, _ := btree.Scan(e.bp, idxTree.RootPageID(), startKey, endKey)
			if len(matches) > 0 {
				// Roll back the primary insert
				tree.Delete(pk)
				return nil, fmt.Errorf("executor: UNIQUE constraint violation on index %q: duplicate value %v for column %q",
					idx.Name, row[idx.Column], idx.Column)
			}
		}
		
		compKey := compositeIndexKey(idxKey, pk)
		_ = idxTree.Insert(compKey, pk)
		// Update catalog if index root changed
		if newIdxRoot := idxTree.RootPageID(); newIdxRoot != idx.RootPageID {
			e.cat.UpdateIndexRootPageID(idx.TableName, idx.Name, newIdxRoot)
			idx.RootPageID = newIdxRoot
		}
	}

	rs := NewResultSet([]string{"result"})
	rs.AddRow(Row{"result": "1 row inserted"})
	return rs, nil
}

// ---- SELECT ----

func (e *Executor) executeSelect(stmt *parser.SelectStmt) (*ResultSet, error) {
	// 2PL: acquire SHARED lock on the table (reads allow concurrent readers)
	if err := e.txm.AcquireLock(stmt.Table, lock.LockShared); err != nil {
		return nil, fmt.Errorf("executor: %w", err)
	}

	schema, err := e.cat.GetTable(stmt.Table)
	if err != nil {
		return nil, err
	}

	tree, err := e.getTree(stmt.Table, schema)
	if err != nil {
		return nil, err
	}

	// Decide execution strategy:
	// 1. WHERE pk_col = lit   → Primary Index Scan       O(log n)
	// 2. WHERE idx_col = lit  → Secondary Index Scan     O(log n)
	// 3. Otherwise            → Sequential Scan          O(n)

	var rows []Row

	rowStore := e.getRowStore(stmt.Table, schema)

	if pkVal, ok := extractIndexScanKey(stmt.Where, schema); ok {
		// ---- Primary Index Scan ----
		rowEncoded, found := tree.Search(pkVal)
		if found {
			rowBytes, _ := rowStore.Read(rowEncoded)
			row, _ := UnpackRowBytes(schema, rowBytes)
			if e.evalWhere(stmt.Where, row) {
				rows = append(rows, row)
			}
		}
	} else if idxSch, idxKey, ok := e.findSecondaryIndexScan(stmt.Where, schema); ok {
		// ---- Secondary Index Scan ----
		idxTree := e.loadIndexTree(*idxSch)
		if idxTree != nil {
			startKey := compositeIndexKey(idxKey, 0)
			endKey := compositeIndexKey(idxKey, math.MaxInt32)
			matches, _ := btree.Scan(e.bp, idxTree.RootPageID(), startKey, endKey)
			for _, match := range matches {
				pkStored := match.Value
				rowEncoded, found2 := tree.Search(pkStored)
				if found2 {
					rowBytes, _ := rowStore.Read(rowEncoded)
					row, _ := UnpackRowBytes(schema, rowBytes)
					if e.evalWhere(stmt.Where, row) {
						rows = append(rows, row)
					}
				}
			}
		} else {
			// Index tree not in memory — fall back to seq scan
			scanResults, err := btree.Scan(e.bp, tree.RootPageID(), 0, math.MaxInt64)
			if err != nil {
				return nil, fmt.Errorf("executor: seq scan (index fallback) failed: %w", err)
			}
			for _, sr := range scanResults {
				rowBytes, _ := rowStore.Read(sr.Value)
				row, _ := UnpackRowBytes(schema, rowBytes)
				if e.evalWhere(stmt.Where, row) {
					rows = append(rows, row)
				}
			}
		}
	} else {
		// ---- Sequential Scan ----
		scanResults, err := btree.Scan(e.bp, tree.RootPageID(), 0, math.MaxInt64)
		if err != nil {
			return nil, fmt.Errorf("executor: seq scan failed: %w", err)
		}
		for _, sr := range scanResults {
			rowBytes, _ := rowStore.Read(sr.Value)
			row, _ := UnpackRowBytes(schema, rowBytes)
			if e.evalWhere(stmt.Where, row) {
				rows = append(rows, row)
			}
		}
	}

	// ORDER BY
	if stmt.OrderBy != nil {
		col := stmt.OrderBy.Column
		asc := stmt.OrderBy.Asc
		sort.Slice(rows, func(i, j int) bool {
			a := rows[i].Get(col)
			b := rows[j].Get(col)
			cmp := compareValues(a, b)
			if asc {
				return cmp < 0
			}
			return cmp > 0
		})
	}

	// LIMIT
	if stmt.Limit > 0 && len(rows) > stmt.Limit {
		rows = rows[:stmt.Limit]
	}

	// Projection: determine output columns
	outputCols := resolveColumns(stmt.Columns, schema)
	rs := NewResultSet(outputCols)
	for _, row := range rows {
		projected := make(Row)
		for _, col := range outputCols {
			projected[col] = row.Get(col)
		}
		rs.AddRow(projected)
	}

	return rs, nil
}

// ---- UPDATE ----

func (e *Executor) executeUpdate(stmt *parser.UpdateStmt) (*ResultSet, error) {
	// 2PL: acquire EXCLUSIVE lock on the table (writes need exclusive access)
	if err := e.txm.AcquireLock(stmt.Table, lock.LockExclusive); err != nil {
		return nil, fmt.Errorf("executor: %w", err)
	}

	schema, err := e.cat.GetTable(stmt.Table)
	if err != nil {
		return nil, err
	}

	tree, err := e.getTree(stmt.Table, schema)
	if err != nil {
		return nil, err
	}

	// Scan all rows that match WHERE
	scanResults, err := btree.Scan(e.bp, tree.RootPageID(), 0, math.MaxInt64)
	if err != nil {
		return nil, err
	}

	rowStore := e.getRowStore(stmt.Table, schema)

	updateCount := 0
	for _, sr := range scanResults {
		rowBytes, _ := rowStore.Read(sr.Value)
		row, _ := UnpackRowBytes(schema, rowBytes)
		if !e.evalWhere(stmt.Where, row) {
			continue
		}

		pk := sr.Key
		oldEncoded := sr.Value

		// Apply SET assignments
		for _, assign := range stmt.Assignments {
			val, err := evalLiteral(assign.Value)
			if err != nil {
				return nil, err
			}
			row[assign.Column] = val
		}

		// Encode new row and append to row store
		newRowBytes := PackRowBytes(schema, row)
		newEncoded, err := rowStore.Append(newRowBytes)
		if err != nil {
			return nil, err
		}

		if e.txm.IsActive() {
			// ---- Explicit transaction mode ----
			e.wal.LogUpdate(e.txm.ActiveTxID(), schema.RootPageID, pk, oldEncoded, newEncoded)
			e.txm.RecordUpdate(tree, pk, oldEncoded) // save for potential rollback
			tree.Delete(pk)
			tree.Insert(pk, newEncoded)
		} else {
			// ---- Auto-commit mode ----
			txID, _ := e.wal.Begin()
			e.wal.LogUpdate(txID, schema.RootPageID, pk, oldEncoded, newEncoded)
			tree.Delete(pk)
			tree.Insert(pk, newEncoded)
			e.wal.Commit(txID)
		}

		// ---- Maintain secondary indexes ----
		// We read the old row to get the old indexed column values.
		oldRowBytes, _ := rowStore.Read(oldEncoded)
		oldRow, _ := UnpackRowBytes(schema, oldRowBytes)
		for _, idx := range schema.Indexes {
			idxTree := e.loadIndexTree(idx)
			if idxTree == nil {
				continue
			}
			oldIdxKey := indexKeyFromVal(oldRow[idx.Column])
			newIdxKey := indexKeyFromVal(row[idx.Column])
			
			// Remove old entry
			oldCompKey := compositeIndexKey(oldIdxKey, pk)
			idxTree.Delete(oldCompKey)
			
			// Insert new entry
			newCompKey := compositeIndexKey(newIdxKey, pk)
			_ = idxTree.Insert(newCompKey, pk)
			
			// Update catalog if index root changed
			if newIdxRoot := idxTree.RootPageID(); newIdxRoot != idx.RootPageID {
				e.cat.UpdateIndexRootPageID(idx.TableName, idx.Name, newIdxRoot)
			}
		}
		updateCount++
	}

	rs := NewResultSet([]string{"result"})
	rs.AddRow(Row{"result": fmt.Sprintf("%d row(s) updated", updateCount)})
	return rs, nil
}

// ---- DELETE ----

func (e *Executor) executeDelete(stmt *parser.DeleteStmt) (*ResultSet, error) {
	// 2PL: acquire EXCLUSIVE lock on the table (writes need exclusive access)
	if err := e.txm.AcquireLock(stmt.Table, lock.LockExclusive); err != nil {
		return nil, fmt.Errorf("executor: %w", err)
	}

	schema, err := e.cat.GetTable(stmt.Table)
	if err != nil {
		return nil, err
	}

	tree, err := e.getTree(stmt.Table, schema)
	if err != nil {
		return nil, err
	}

	scanResults, err := btree.Scan(e.bp, tree.RootPageID(), 0, math.MaxInt64)
	if err != nil {
		return nil, err
	}

	rowStore := e.getRowStore(stmt.Table, schema)

	deleteCount := 0
	for _, sr := range scanResults {
		rowBytes, _ := rowStore.Read(sr.Value)
		row, _ := UnpackRowBytes(schema, rowBytes)
		if !e.evalWhere(stmt.Where, row) {
			continue
		}

		pk := sr.Key
		oldEncoded := sr.Value

		if e.txm.IsActive() {
			// ---- Explicit transaction mode ----
			e.wal.LogDelete(e.txm.ActiveTxID(), schema.RootPageID, pk, oldEncoded)
			e.txm.RecordDelete(tree, pk, oldEncoded) // save for potential rollback
			tree.Delete(pk)
		} else {
			// ---- Auto-commit mode ----
			txID, _ := e.wal.Begin()
			e.wal.LogDelete(txID, schema.RootPageID, pk, oldEncoded)
			tree.Delete(pk)
			e.wal.Commit(txID)
		}

		// ---- Maintain secondary indexes ----
		for _, idx := range schema.Indexes {
			idxTree := e.loadIndexTree(idx)
			if idxTree == nil {
				continue
			}
			idxKey := indexKeyFromVal(row[idx.Column])
			compKey := compositeIndexKey(idxKey, pk)
			idxTree.Delete(compKey)
		}
		deleteCount++
	}

	rs := NewResultSet([]string{"result"})
	rs.AddRow(Row{"result": fmt.Sprintf("%d row(s) deleted", deleteCount)})
	return rs, nil
}

// ---- Helpers ----

// getTree returns the B+ tree for a table, loading it from the catalog if needed.
func (e *Executor) getTree(tableName string, schema *catalog.TableSchema) (*btree.BTree, error) {
	if tree, ok := e.trees[tableName]; ok {
		return tree, nil
	}
	tree := btree.OpenBTree(e.bp, schema.RootPageID)
	e.trees[tableName] = tree
	return tree, nil
}

// getRowStore returns the RowStore for a table, loading it from the catalog if needed.
func (e *Executor) getRowStore(tableName string, schema *catalog.TableSchema) *RowStore {
	if rs, ok := e.rowStores[tableName]; ok {
		return rs
	}
	rs := OpenRowStore(e.bp, schema.HeapPageID)
	e.rowStores[tableName] = rs
	return rs
}

// evalWhere evaluates the WHERE clause against a row.
// Returns true if the row matches (or if there is no WHERE clause).
func (e *Executor) evalWhere(where parser.Expr, row Row) bool {
	if where == nil {
		return true // no WHERE = match everything
	}
	expr := convertExpr(where)
	result, err := EvalExpr(expr, row)
	if err != nil {
		return false
	}
	b, ok := result.(bool)
	return ok && b
}

// convertExpr converts a parser.Expr AST node to the internal evaluator types.
func convertExpr(e parser.Expr) interface{} {
	switch expr := e.(type) {
	case *parser.Literal:
		return &literalExpr{value: expr.Value}
	case *parser.ColumnRef:
		return &columnRefExpr{name: expr.Name}
	case *parser.QualifiedRef:
		return &qualifiedRefExpr{table: expr.Table, col: expr.Column}
	case *parser.BinaryExpr:
		return &binaryExpr{
			left:  convertExpr(expr.Left),
			right: convertExpr(expr.Right),
			op:    expr.Op,
		}
	case *parser.UnaryExpr:
		return &unaryExpr{
			operand: convertExpr(expr.Operand),
			op:      expr.Op,
		}
	}
	return nil
}

// evalLiteral evaluates a literal expression node to a Go value.
func evalLiteral(expr parser.Expr) (Value, error) {
	lit, ok := expr.(*parser.Literal)
	if !ok {
		return nil, fmt.Errorf("executor: expected literal, got %T", expr)
	}
	return lit.Value, nil
}

// extractPrimaryKey finds the primary key value (first INT or FLOAT column) from a row.
func extractPrimaryKey(schema *catalog.TableSchema, row Row) (int64, error) {
	for _, col := range schema.Columns {
		switch col.Type {
		case parser.DataTypeInt:
			val, ok := row[col.Name].(int64)
			if !ok {
				return 0, fmt.Errorf("executor: primary key column %q is not an integer", col.Name)
			}
			return val, nil
		case parser.DataTypeFloat:
			val, ok := row[col.Name].(float64)
			if !ok {
				return 0, fmt.Errorf("executor: primary key column %q is not a float", col.Name)
			}
			return int64(val * 1e6), nil // scale to preserve some decimal precision
		}
	}
	return 0, fmt.Errorf("executor: table has no INT or FLOAT primary key column")
}

// extractIndexScanKey checks if the WHERE clause is a simple `pkCol = value`
// and returns the key value for an index scan.
//
// Returns (key, true) if we can use IndexScan.
// Returns (0, false) if we need a SeqScan.
func extractIndexScanKey(where parser.Expr, schema *catalog.TableSchema) (int64, bool) {
	if where == nil {
		return 0, false
	}
	bin, ok := where.(*parser.BinaryExpr)
	if !ok || bin.Op != "=" {
		return 0, false
	}

	// Check that left side is the primary key column
	col, ok1 := bin.Left.(*parser.ColumnRef)
	lit, ok2 := bin.Right.(*parser.Literal)
	if !ok1 || !ok2 {
		return 0, false
	}

	// Is this column the first INT column (primary key)?
	if len(schema.Columns) > 0 && schema.Columns[0].Name == col.Name &&
		schema.Columns[0].Type == parser.DataTypeInt {
		if val, ok := lit.Value.(int64); ok {
			return val, true
		}
	}
	return 0, false
}

// resolveColumns returns the actual output column names.
// ["*"] → all column names; otherwise the list as-is.
func resolveColumns(cols []string, schema *catalog.TableSchema) []string {
	if len(cols) == 1 && cols[0] == "*" {
		names := make([]string, len(schema.Columns))
		for i, col := range schema.Columns {
			names[i] = col.Name
		}
		return names
	}
	return cols
}

// PackRowBytes encodes a row as a proper binary byte slice for persistence.
// Format: [col1_type(1B) col1_data...] [col2_type(1B) col2_data...] ...
// Type tags: 0=INT (8B), 1=TEXT (2B len + data), 2=FLOAT (8B), 3=BOOL (1B)
// This is used for WAL records that need to be durable.
func PackRowBytes(schema *catalog.TableSchema, row Row) []byte {
	var parts [][]byte

	for _, col := range schema.Columns {
		val := row[col.Name]
		var colBytes []byte

		if val == nil {
			colBytes = []byte{4} // NULL type tag
		} else {
			switch col.Type {
			case parser.DataTypeInt:
				b := make([]byte, 9) // type(1) + value(8)
				b[0] = 0             // INT type tag
				v, _ := val.(int64)
				binary.LittleEndian.PutUint64(b[1:], uint64(v))
				colBytes = b
	
			case parser.DataTypeText:
				s, _ := val.(string)
				sBytes := []byte(s)
				b := make([]byte, 3+len(sBytes)) // type(1) + len(2) + data
				b[0] = 1                         // TEXT type tag
				binary.LittleEndian.PutUint16(b[1:], uint16(len(sBytes)))
				copy(b[3:], sBytes)
				colBytes = b
	
			case parser.DataTypeFloat:
				b := make([]byte, 9) // type(1) + value(8)
				b[0] = 2             // FLOAT type tag
				v, _ := val.(float64)
				binary.LittleEndian.PutUint64(b[1:], math.Float64bits(v))
				colBytes = b
	
			case parser.DataTypeBool:
				b := make([]byte, 2) // type(1) + value(1)
				b[0] = 3             // BOOL type tag
				if v, _ := val.(bool); v {
					b[1] = 1
				}
				colBytes = b
			}
		}

		parts = append(parts, colBytes)
	}

	var result []byte
	for _, p := range parts {
		result = append(result, p...)
	}
	return result
}

// UnpackRowBytes decodes a binary row back into a Row map.
func UnpackRowBytes(schema *catalog.TableSchema, data []byte) (Row, error) {
	row := make(Row)
	offset := 0

	for _, col := range schema.Columns {
		if offset >= len(data) {
			return nil, fmt.Errorf("executor: row data truncated at column %q", col.Name)
		}

		typeTag := data[offset]
		offset++

		switch typeTag {
		case 0: // INT
			if offset+8 > len(data) {
				return nil, fmt.Errorf("executor: not enough bytes for INT column %q", col.Name)
			}
			v := int64(binary.LittleEndian.Uint64(data[offset:]))
			row[col.Name] = v
			offset += 8

		case 1: // TEXT
			if offset+2 > len(data) {
				return nil, fmt.Errorf("executor: not enough bytes for TEXT length in column %q", col.Name)
			}
			strLen := int(binary.LittleEndian.Uint16(data[offset:]))
			offset += 2
			if offset+strLen > len(data) {
				return nil, fmt.Errorf("executor: not enough bytes for TEXT data in column %q", col.Name)
			}
			row[col.Name] = string(data[offset : offset+strLen])
			offset += strLen

		case 2: // FLOAT
			if offset+8 > len(data) {
				return nil, fmt.Errorf("executor: not enough bytes for FLOAT column %q", col.Name)
			}
			bits := binary.LittleEndian.Uint64(data[offset:])
			row[col.Name] = math.Float64frombits(bits)
			offset += 8

		case 3: // BOOL
			if offset+1 > len(data) {
				return nil, fmt.Errorf("executor: not enough bytes for BOOL column %q", col.Name)
			}
			row[col.Name] = data[offset] != 0
			offset++

		case 4: // NULL
			row[col.Name] = nil

		default:
			return nil, fmt.Errorf("executor: unknown type tag %d for column %q", typeTag, col.Name)
		}
	}

	return row, nil
}

// DescribeTable returns a human-readable schema description.
func (e *Executor) DescribeTable(tableName string) (string, error) {
	schema, err := e.cat.GetTable(tableName)
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Table: %s\n", schema.Name))
	sb.WriteString(fmt.Sprintf("Root PageID: %d\n", schema.RootPageID))
	sb.WriteString("Columns:\n")
	for i, col := range schema.Columns {
		sb.WriteString(fmt.Sprintf("  [%d] %s %s\n", i, col.Name, col.Type))
	}
	return sb.String(), nil
}

// ---- JOIN ----

// executeJoinSelect executes a SELECT with a JOIN clause using a Nested Loop Join.
//
// Algorithm:
//  1. SeqScan the left (base) table → leftRows
//  2. SeqScan the right (joined) table → rightRows
//  3. For each leftRow × each rightRow:
//     a. Merge into a combined row with "table.col" qualified keys
//     b. Evaluate the ON condition
//     c. If it matches, append to result
//  4. For LEFT JOIN: also emit non-matched left rows with NULLs for right columns
//
// Performance: O(n × m) — fine for small tables. A real DB would use a HashJoin.
func (e *Executor) executeJoinSelect(stmt *parser.SelectStmt) (*ResultSet, error) {
	// 2PL: acquire SHARED locks on both tables involved in the JOIN
	if err := e.txm.AcquireLock(stmt.Table, lock.LockShared); err != nil {
		return nil, fmt.Errorf("executor: %w", err)
	}
	if err := e.txm.AcquireLock(stmt.Join.Table, lock.LockShared); err != nil {
		return nil, fmt.Errorf("executor: %w", err)
	}

	// Fetch left table
	leftSchema, err := e.cat.GetTable(stmt.Table)
	if err != nil {
		return nil, err
	}
	leftTree, err := e.getTree(stmt.Table, leftSchema)
	if err != nil {
		return nil, err
	}
	leftScan, err := btree.Scan(e.bp, leftTree.RootPageID(), 0, math.MaxInt64)
	if err != nil {
		return nil, fmt.Errorf("executor: JOIN left table scan failed: %w", err)
	}
	leftRowStore := e.getRowStore(stmt.Table, leftSchema)

	// Fetch right table
	rightSchema, err := e.cat.GetTable(stmt.Join.Table)
	if err != nil {
		return nil, err
	}
	rightTree, err := e.getTree(stmt.Join.Table, rightSchema)
	if err != nil {
		return nil, err
	}
	rightScan, err := btree.Scan(e.bp, rightTree.RootPageID(), 0, math.MaxInt64)
	if err != nil {
		return nil, fmt.Errorf("executor: JOIN right table scan failed: %w", err)
	}
	rightRowStore := e.getRowStore(stmt.Join.Table, rightSchema)

	// Decode all rows from both sides, storing them with qualified "table.col" keys
	var leftRows []Row
	for _, sr := range leftScan {
		leftRowBytes, _ := leftRowStore.Read(sr.Value)
		raw, _ := UnpackRowBytes(leftSchema, leftRowBytes)
		qRow := make(Row)
		for _, col := range leftSchema.Columns {
			qRow[stmt.Table+"."+col.Name] = raw[col.Name]
			qRow[col.Name] = raw[col.Name] // also plain name for convenience
		}
		leftRows = append(leftRows, qRow)
	}

	var rightRows []Row
	for _, sr := range rightScan {
		rightRowBytes, _ := rightRowStore.Read(sr.Value)
		raw, _ := UnpackRowBytes(rightSchema, rightRowBytes)
		qRow := make(Row)
		for _, col := range rightSchema.Columns {
			qRow[stmt.Join.Table+"."+col.Name] = raw[col.Name]
			qRow[col.Name] = raw[col.Name]
		}
		rightRows = append(rightRows, qRow)
	}

	// Nested loop join
	var joinedRows []Row
	for _, left := range leftRows {
		matched := false
		for _, right := range rightRows {
			// Build merged row
			merged := make(Row)
			for k, v := range left {
				merged[k] = v
			}
			for k, v := range right {
				merged[k] = v
			}
			// Evaluate ON condition
			if e.evalWhere(stmt.Join.On, merged) {
				// Apply optional WHERE filter on the merged row
				if e.evalWhere(stmt.Where, merged) {
					joinedRows = append(joinedRows, merged)
				}
				matched = true
			}
		}
		// LEFT JOIN: emit non-matching left rows with NULLs for right columns
		if !matched && stmt.Join.Type == "LEFT" {
			merged := make(Row)
			for k, v := range left {
				merged[k] = v
			}
			for _, col := range rightSchema.Columns {
				merged[stmt.Join.Table+"."+col.Name] = nil
				merged[col.Name] = nil
			}
			if e.evalWhere(stmt.Where, merged) {
				joinedRows = append(joinedRows, merged)
			}
		}
	}

	// ORDER BY
	if stmt.OrderBy != nil {
		col := stmt.OrderBy.Column
		asc := stmt.OrderBy.Asc
		sort.Slice(joinedRows, func(i, j int) bool {
			a := joinedRows[i].Get(col)
			b := joinedRows[j].Get(col)
			cmp := compareValues(a, b)
			if asc {
				return cmp < 0
			}
			return cmp > 0
		})
	}

	// LIMIT
	if stmt.Limit > 0 && len(joinedRows) > stmt.Limit {
		joinedRows = joinedRows[:stmt.Limit]
	}

	// Projection: resolve output columns ("*" → all columns from both tables)
	var outputCols []string
	if len(stmt.Columns) == 1 && stmt.Columns[0] == "*" {
		for _, col := range leftSchema.Columns {
			outputCols = append(outputCols, stmt.Table+"."+col.Name)
		}
		for _, col := range rightSchema.Columns {
			outputCols = append(outputCols, stmt.Join.Table+"."+col.Name)
		}
	} else {
		outputCols = stmt.Columns
	}

	rs := NewResultSet(outputCols)
	for _, row := range joinedRows {
		projected := make(Row)
		for _, col := range outputCols {
			projected[col] = row.Get(col)
		}
		rs.AddRow(projected)
	}
	return rs, nil
}
