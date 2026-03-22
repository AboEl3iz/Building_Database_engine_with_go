package engine

import (
	"encoding/binary"
	"fmt"
	"math"
	"minidb/internal/btree"
	"minidb/internal/buffer"
	"minidb/internal/catalog"
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
//  2. Decide: use IndexScan (WHERE pk = ?) or SeqScan (everything else)
//  3. For each matching row: apply WHERE filter
//  4. Apply projection (pick only requested columns)
//  5. Apply ORDER BY + LIMIT
//  6. Return ResultSet
type Executor struct {
	bp    *buffer.BufferPool
	cat   *catalog.Catalog
	wal   *wal.WAL
	txm   *txn.TxManager      // session-level transaction manager
	trees map[string]*btree.BTree // table name → B+ tree
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
		bp:    bp,
		cat:   cat,
		wal:   w,
		txm:   tm,
		trees: make(map[string]*btree.BTree),
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

	// Allocate a new B+ tree for this table
	tree, err := btree.NewBTree(e.bp)
	if err != nil {
		return nil, fmt.Errorf("executor: cannot create B+ tree for %q: %w", stmt.TableName, err)
	}

	// Register in catalog with the tree's root page ID
	if err := e.cat.CreateTable(stmt.TableName, stmt.Columns, tree.RootPageID()); err != nil {
		return nil, fmt.Errorf("executor: catalog error: %w", err)
	}

	// Cache the tree
	e.trees[stmt.TableName] = tree

	rs := NewResultSet([]string{"result"})
	rs.AddRow(Row{"result": fmt.Sprintf("Table %q created", stmt.TableName)})
	return rs, nil
}

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

	// Encode row as bytes, pack into an int64 representation
	rowEncoded, err := e.insertRowWithCache(schema, row)
	if err != nil {
		return nil, fmt.Errorf("executor: row encoding failed: %w", err)
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

	rs := NewResultSet([]string{"result"})
	rs.AddRow(Row{"result": "1 row inserted"})
	return rs, nil
}

// ---- SELECT ----

func (e *Executor) executeSelect(stmt *parser.SelectStmt) (*ResultSet, error) {
	schema, err := e.cat.GetTable(stmt.Table)
	if err != nil {
		return nil, err
	}

	tree, err := e.getTree(stmt.Table, schema)
	if err != nil {
		return nil, err
	}

	// Decide execution strategy:
	// If WHERE clause is a simple equality on the primary key → IndexScan
	// Otherwise → SeqScan (scan all leaf nodes)

	var rows []Row

	if pkVal, ok := extractIndexScanKey(stmt.Where, schema); ok {
		// ---- Index Scan: point lookup ----
		// O(log n) — use B+ tree Search directly
		rowEncoded, found := tree.Search(pkVal)
		if found {
			row := decodeRow(schema, rowEncoded)
			if e.evalWhere(stmt.Where, row) {
				rows = append(rows, row)
			}
		}
	} else {
		// ---- Sequential Scan: walk all leaf nodes ----
		// O(n) — iterate every entry in the tree
		scanResults, err := btree.Scan(e.bp, tree.RootPageID(), 0, math.MaxInt64) // int64 max
		if err != nil {
			return nil, fmt.Errorf("executor: seq scan failed: %w", err)
		}
		for _, sr := range scanResults {
			row := decodeRow(schema, sr.Value)
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

	updateCount := 0
	for _, sr := range scanResults {
		row := decodeRow(schema, sr.Value)
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

		newEncoded := encodeRow(schema, row)
		storeRowInCache(schema, row, newEncoded)

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
		updateCount++
	}

	rs := NewResultSet([]string{"result"})
	rs.AddRow(Row{"result": fmt.Sprintf("%d row(s) updated", updateCount)})
	return rs, nil
}

// ---- DELETE ----

func (e *Executor) executeDelete(stmt *parser.DeleteStmt) (*ResultSet, error) {
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

	deleteCount := 0
	for _, sr := range scanResults {
		row := decodeRow(schema, sr.Value)
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

// ---- Row Encoding / Decoding ----
//
// We need to store a full row (multiple columns) as a single int64 in the B+ tree.
//
// SIMPLIFIED ENCODING:
// Since our B+ tree stores int64 keys and int64 values, we pack the row
// into a page-level "row store" and return a page+offset reference.
//
// For this implementation, we use a compact in-memory encoding:
//   - For tables with only INT columns: XOR-fold all values into one int64
//   - For TEXT columns: store a simple hash
//
// A REAL implementation would:
//   1. Store variable-length rows in a separate "heap file" (like PostgreSQL's)
//   2. The B+ tree value = (pageID, slotID) pointing into the heap file
//   3. Or: inline short rows directly in the leaf node (like SQLite)
//
// For learning purposes, we use a simple encoding sufficient to demonstrate
// all the database concepts without heap file complexity.

// encodeRow packs a row into an int64 for B+ tree storage.
// NOTE: This is a simplification — a real DB would store rows in heap pages.
func encodeRow(schema *catalog.TableSchema, row Row) int64 {
	var encoded int64
	for i, col := range schema.Columns {
		if i >= 8 { // we only support 8 columns in this simplified encoding
			break
		}
		val := row[col.Name]
		switch v := val.(type) {
		case int64:
			encoded ^= v << (uint(i) * 7 % 56)
		case float64:
			// Treat float bits as int64 for XOR encoding
			encoded ^= int64(math.Float64bits(v)) << (uint(i) * 7 % 56)
		case bool:
			if v {
				encoded ^= int64(1) << (uint(i) * 7 % 56)
			}
		case string:
			hash := int64(0)
			for j, c := range v {
				hash = hash*31 + int64(c)*int64(j+1)
			}
			encoded ^= hash << (uint(i) * 7 % 56)
		}
	}
	return encoded
}

// decodeRow reconstructs a Row from an encoded int64 and schema.
//
// NOTE: Because encodeRow uses a lossy encoding (XOR-folding), this
// reconstruction is APPROXIMATE for TEXT columns and multi-column tables.
// In a real database, we'd store the full row bytes.
//
// For learning/demo purposes, we store the original values separately
// using a global row cache (in-memory; lost on restart).
var rowCache = make(map[int64]map[string]Value) // encoded → original values

func decodeRow(schema *catalog.TableSchema, encoded int64) Row {
	// Check our in-memory cache for the original row
	if cached, ok := rowCache[encoded]; ok {
		row := make(Row, len(cached))
		for k, v := range cached {
			row[k] = v
		}
		return row
	}

	// If not cached (e.g., after restart), return a placeholder row with the key
	row := make(Row)
	for _, col := range schema.Columns {
		if col.Type == parser.DataTypeInt {
			row[col.Name] = encoded // best approximation
		} else {
			row[col.Name] = "(data lost on restart)"
		}
	}
	return row
}

// storeRowInCache saves the original row values before encoding.
// Called by executeInsert to enable later decoding.
func storeRowInCache(schema *catalog.TableSchema, row Row, encoded int64) {
	cached := make(map[string]Value)
	for _, col := range schema.Columns {
		cached[col.Name] = row[col.Name]
	}
	rowCache[encoded] = cached
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

		default:
			return nil, fmt.Errorf("executor: unknown type tag %d for column %q", typeTag, col.Name)
		}
	}

	return row, nil
}

// ---- Enhanced INSERT with proper row caching ----
// We override the simplified encodeRow to also populate the cache.

func (e *Executor) insertRowWithCache(schema *catalog.TableSchema, row Row) (int64, error) {
	_, err := extractPrimaryKey(schema, row)
	if err != nil {
		return 0, err
	}

	encoded := encodeRow(schema, row)
	storeRowInCache(schema, row, encoded)
	return encoded, nil
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

	// Decode all rows from both sides, storing them with qualified "table.col" keys
	var leftRows []Row
	for _, sr := range leftScan {
		raw := decodeRow(leftSchema, sr.Value)
		qRow := make(Row)
		for _, col := range leftSchema.Columns {
			qRow[stmt.Table+"."+col.Name] = raw[col.Name]
			qRow[col.Name] = raw[col.Name] // also plain name for convenience
		}
		leftRows = append(leftRows, qRow)
	}

	var rightRows []Row
	for _, sr := range rightScan {
		raw := decodeRow(rightSchema, sr.Value)
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
