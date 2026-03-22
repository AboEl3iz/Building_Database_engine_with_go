package parser

import (
	"fmt"
	"strconv"
)

// ---- AST (Abstract Syntax Tree) Node Definitions ----
//
// CONCEPT: What is an AST?
// An AST represents the structure and meaning of a SQL statement as a tree.
// Unlike raw SQL text, the AST has already been parsed — no more string scanning.
//
// Example: "SELECT name FROM users WHERE age > 25 AND age < 30"
//
//   SelectStmt
//   ├── Columns: ["name"]
//   ├── Table: "users"
//   └── Where: BinaryExpr(AND)
//       ├── BinaryExpr(>)
//       │   ├── ColumnRef("age")
//       │   └── Literal(25)
//       └── BinaryExpr(<)
//           ├── ColumnRef("age")
//           └── Literal(30)
//
// The Query Engine walks this tree to build an execution plan.

// Statement is the interface all top-level SQL statements implement.
// A Statement represents one complete SQL command (SELECT, INSERT, etc.).
type Statement interface {
	statementNode() // marker method — ensures only statements implement this interface
	String() string // human-readable form (for debugging/EXPLAIN)
}

// Expr is the interface all SQL expressions implement.
// An expression produces a value when evaluated against a row.
// Examples: `age > 25`, `name`, `42`, `age > 25 AND id < 100`
type Expr interface {
	exprNode()      // marker method
	String() string // human-readable form
}

// DataType represents a column's data type (INT or TEXT).
type DataType int

const (
	DataTypeInt   DataType = iota // 64-bit integer
	DataTypeText                  // variable-length string
	DataTypeFloat                 // 64-bit IEEE 754 float
	DataTypeBool                  // boolean (true/false)
)

func (dt DataType) String() string {
	switch dt {
	case DataTypeInt:
		return "INT"
	case DataTypeText:
		return "TEXT"
	case DataTypeFloat:
		return "FLOAT"
	case DataTypeBool:
		return "BOOL"
	default:
		return "UNKNOWN"
	}
}

// ColumnDef defines a column in a CREATE TABLE statement.
type ColumnDef struct {
	Name string
	Type DataType
}

// OrderBy defines an ORDER BY clause.
type OrderBy struct {
	Column string
	Asc    bool // true = ASC, false = DESC
}

// ============================
// Statement AST Nodes
// ============================

// CreateTableStmt represents: CREATE TABLE users (id INT, name TEXT, age INT)
type CreateTableStmt struct {
	TableName string
	Columns   []ColumnDef
}

func (s *CreateTableStmt) statementNode() {}
func (s *CreateTableStmt) String() string {
	cols := ""
	for i, col := range s.Columns {
		if i > 0 {
			cols += ", "
		}
		cols += fmt.Sprintf("%s %s", col.Name, col.Type)
	}
	return fmt.Sprintf("CREATE TABLE %s (%s)", s.TableName, cols)
}

// JoinClause represents a JOIN expression: [INNER|LEFT] JOIN table ON expr
type JoinClause struct {
	Type  string // "INNER" or "LEFT"
	Table string // the right-hand table name
	On    Expr   // the join condition
}

// SelectStmt represents: SELECT col1, col2 FROM table [JOIN ...] WHERE expr ORDER BY col LIMIT n
type SelectStmt struct {
	Columns []string  // ["*"] for SELECT *, or specific column names
	Table   string
	Join    *JoinClause // nil if no JOIN
	Where   Expr        // nil if no WHERE clause
	OrderBy *OrderBy    // nil if no ORDER BY
	Limit   int         // 0 means no limit
}

func (s *SelectStmt) statementNode() {}
func (s *SelectStmt) String() string {
	sql := fmt.Sprintf("SELECT %s FROM %s", joinStrings(s.Columns, ", "), s.Table)
	if s.Join != nil {
		sql += fmt.Sprintf(" %s JOIN %s ON %s", s.Join.Type, s.Join.Table, s.Join.On.String())
	}
	if s.Where != nil {
		sql += " WHERE " + s.Where.String()
	}
	if s.OrderBy != nil {
		dir := "ASC"
		if !s.OrderBy.Asc {
			dir = "DESC"
		}
		sql += fmt.Sprintf(" ORDER BY %s %s", s.OrderBy.Column, dir)
	}
	if s.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", s.Limit)
	}
	return sql
}

// InsertStmt represents: INSERT INTO users VALUES (1, 'Alice', 30)
type InsertStmt struct {
	Table  string
	Values []Expr // one Expr per column value (Literal nodes)
}

func (s *InsertStmt) statementNode() {}
func (s *InsertStmt) String() string {
	vals := make([]string, len(s.Values))
	for i, v := range s.Values {
		vals[i] = v.String()
	}
	return fmt.Sprintf("INSERT INTO %s VALUES (%s)", s.Table, joinStrings(vals, ", "))
}

// UpdateStmt represents: UPDATE users SET age = 31, name = 'Bob' WHERE id = 1
type UpdateStmt struct {
	Table      string
	Assignments []Assignment // SET col = value pairs
	Where       Expr         // nil if no WHERE
}

// Assignment is one col = value pair in an UPDATE SET clause.
type Assignment struct {
	Column string
	Value  Expr
}

func (s *UpdateStmt) statementNode() {}
func (s *UpdateStmt) String() string {
	sets := make([]string, len(s.Assignments))
	for i, a := range s.Assignments {
		sets[i] = fmt.Sprintf("%s = %s", a.Column, a.Value.String())
	}
	sql := fmt.Sprintf("UPDATE %s SET %s", s.Table, joinStrings(sets, ", "))
	if s.Where != nil {
		sql += " WHERE " + s.Where.String()
	}
	return sql
}

// DeleteStmt represents: DELETE FROM users WHERE id = 1
type DeleteStmt struct {
	Table string
	Where Expr // nil means delete all rows!
}

func (s *DeleteStmt) statementNode() {}
func (s *DeleteStmt) String() string {
	sql := "DELETE FROM " + s.Table
	if s.Where != nil {
		sql += " WHERE " + s.Where.String()
	}
	return sql
}

// ---- Transaction control statements ----

// BeginStmt represents: BEGIN [TRANSACTION]
type BeginStmt struct{}

func (s *BeginStmt) statementNode() {}
func (s *BeginStmt) String() string    { return "BEGIN" }

// CommitStmt represents: COMMIT [TRANSACTION]
type CommitStmt struct{}

func (s *CommitStmt) statementNode() {}
func (s *CommitStmt) String() string    { return "COMMIT" }

// RollbackStmt represents: ROLLBACK [TRANSACTION]
type RollbackStmt struct{}

func (s *RollbackStmt) statementNode() {}
func (s *RollbackStmt) String() string    { return "ROLLBACK" }

// ============================
// Expression AST Nodes
// ============================

// Literal represents a constant value in SQL: 42, 'Alice', 3.14, TRUE
//
// The Value field holds:
//   - int64   for integer literals
//   - string  for string literals
//   - float64 for float literals
//   - bool    for TRUE/FALSE literals
type Literal struct {
	Value interface{} // int64, string, float64, or bool
}

func (e *Literal) exprNode() {}
func (e *Literal) String() string {
	switch v := e.Value.(type) {
	case int64:
		return fmt.Sprintf("%d", v)
	case string:
		return fmt.Sprintf("'%s'", v)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ColumnRef represents a column name reference in an expression.
// Example: in "WHERE age > 25", `age` is a ColumnRef.
type ColumnRef struct {
	Name string
}

func (e *ColumnRef) exprNode() {}
func (e *ColumnRef) String() string {
	return e.Name
}

// QualifiedRef represents a table-qualified column reference: table.column.
// Example: in "ON orders.user_id = users.id", `orders.user_id` is a QualifiedRef.
type QualifiedRef struct {
	Table  string
	Column string
}

func (e *QualifiedRef) exprNode() {}
func (e *QualifiedRef) String() string {
	return e.Table + "." + e.Column
}

// BinaryExpr represents a two-operand expression.
// Examples: `age > 25`, `id = 1`, `age > 18 AND age < 65`
//
// Op values: "=", "!=", "<", ">", "<=", ">=", "AND", "OR"
type BinaryExpr struct {
	Left  Expr
	Op    string
	Right Expr
}

func (e *BinaryExpr) exprNode() {}
func (e *BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", e.Left.String(), e.Op, e.Right.String())
}

// UnaryExpr represents a single-operand expression.
// Example: `NOT active`
type UnaryExpr struct {
	Op      string // "NOT"
	Operand Expr
}

func (e *UnaryExpr) exprNode() {}
func (e *UnaryExpr) String() string {
	return fmt.Sprintf("(%s %s)", e.Op, e.Operand.String())
}

// ---- helpers ----

func joinStrings(ss []string, sep string) string {
	result := ""
	for i, s := range ss {
		if i > 0 {
			result += sep
		}
		result += s
	}
	return result
}
