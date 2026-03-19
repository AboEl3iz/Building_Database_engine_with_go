// Package engine implements the query planner and executor.
//
// CONCEPT: Query Execution Pipeline
//
//   SQL String
//     ↓  [Lexer + Parser]
//   AST (SelectStmt, InsertStmt, ...)
//     ↓  [Planner]
//   Logical Plan (tree of operators)
//     ↓  [Executor]
//   ResultSet (rows of data)
//
// Operators:
//   SeqScan    — reads all leaf nodes of a B+ tree (full table scan)
//   IndexScan  — point lookup via B+ tree Search(key)
//   Filter     — applies WHERE predicate row-by-row
//   Projection — selects only requested columns
package engine

import (
	"fmt"
	"strconv"
	"strings"
)


// Value is any scalar value a column can hold.
// We use interface{} to support both int64 and string.
type Value interface{}

// Row is one record in the database — a map from column name to value.
//
// Using a map allows flexible column access: row["age"] → 25
// A production database would use a flat byte slice (more compact and faster),
// but maps are easier to understand and work with.
type Row map[string]Value

// Get returns the value of a column, or nil if the column doesn't exist.
func (r Row) Get(col string) Value {
	return r[col]
}

// Set sets the value of a column.
func (r Row) Set(col string, val Value) {
	r[col] = val
}

// Clone creates a deep copy of a row (so modifications don't affect the original).
func (r Row) Clone() Row {
	clone := make(Row, len(r))
	for k, v := range r {
		clone[k] = v
	}
	return clone
}

// ResultSet is the output of a query — an ordered list of rows with column names.
type ResultSet struct {
	Columns []string // column names in display order
	Rows    []Row    // the actual data rows
}

// NewResultSet creates an empty ResultSet with the given columns.
func NewResultSet(columns []string) *ResultSet {
	return &ResultSet{
		Columns: columns,
		Rows:    nil,
	}
}

// AddRow appends a row to the result set.
func (rs *ResultSet) AddRow(row Row) {
	rs.Rows = append(rs.Rows, row)
}

// RowCount returns the number of rows in the result.
func (rs *ResultSet) RowCount() int {
	return len(rs.Rows)
}

// Print displays the ResultSet as an ASCII table — for the REPL.
//
// Example output:
//   +------+---------+-----+
//   | id   | name    | age |
//   +------+---------+-----+
//   | 1    | Alice   | 30  |
//   | 2    | Bob     | 25  |
//   +------+---------+-----+
//   2 rows
func (rs *ResultSet) Print() string {
	if len(rs.Columns) == 0 {
		return "(empty result)\n"
	}

	// Calculate column widths
	widths := make([]int, len(rs.Columns))
	for i, col := range rs.Columns {
		widths[i] = len(col)
	}
	for _, row := range rs.Rows {
		for i, col := range rs.Columns {
			s := formatValue(row.Get(col))
			if len(s) > widths[i] {
				widths[i] = len(s)
			}
		}
	}

	var sb strings.Builder

	// Separator line
	writeSep := func() {
		sb.WriteString("+")
		for _, w := range widths {
			sb.WriteString(strings.Repeat("-", w+2))
			sb.WriteString("+")
		}
		sb.WriteString("\n")
	}

	// Header
	writeSep()
	sb.WriteString("|")
	for i, col := range rs.Columns {
		sb.WriteString(fmt.Sprintf(" %-*s |", widths[i], col))
	}
	sb.WriteString("\n")
	writeSep()

	// Rows
	for _, row := range rs.Rows {
		sb.WriteString("|")
		for i, col := range rs.Columns {
			s := formatValue(row.Get(col))
			sb.WriteString(fmt.Sprintf(" %-*s |", widths[i], s))
		}
		sb.WriteString("\n")
	}

	writeSep()
	sb.WriteString(fmt.Sprintf("%d row(s)\n", len(rs.Rows)))
	return sb.String()
}

// formatValue converts a Value to a string for display.
func formatValue(v Value) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case int64:
		return fmt.Sprintf("%d", val)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}

// ---- Expression Evaluation ----
//
// The evaluator walks the AST expression tree and computes a result value
// given a concrete row.
//
// Example:
//   Expr: BinaryExpr{ Left: ColumnRef("age"), Op: ">", Right: Literal(25) }
//   Row:  {"age": 30, "name": "Alice"}
//   Result: true (30 > 25)

// EvalExpr evaluates an AST expression against a row.
// Returns the result value (int64, string, float64, or bool).
func EvalExpr(expr interface{}, row Row) (Value, error) {
	// We import parser types by name to avoid circular dependency.
	// In a larger project, expressions would be in their own package.
	// Here we use a type switch on the interface.
	switch e := expr.(type) {
	case *literalExpr:
		return e.value, nil

	case *columnRefExpr:
		val := row.Get(e.name)
		return val, nil

	case *qualifiedRefExpr:
		// Try "table.col" first (used in JOIN results), fall back to plain "col"
		key := e.table + "." + e.col
		if val, ok := row[key]; ok {
			return val, nil
		}
		return row.Get(e.col), nil

	case *binaryExpr:
		left, err := EvalExpr(e.left, row)
		if err != nil {
			return nil, err
		}
		right, err := EvalExpr(e.right, row)
		if err != nil {
			return nil, err
		}
		return applyBinaryOp(e.op, left, right)

	case *unaryExpr:
		operand, err := EvalExpr(e.operand, row)
		if err != nil {
			return nil, err
		}
		return applyUnaryOp(e.op, operand)
	}

	return nil, fmt.Errorf("eval: unknown expression type %T", expr)
}

// applyBinaryOp applies a binary operator to two values.
// Supports: =, !=, <, >, <=, >=, AND, OR
func applyBinaryOp(op string, left, right Value) (Value, error) {
	switch op {
	case "AND":
		l, ok1 := left.(bool)
		r, ok2 := right.(bool)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("eval: AND requires boolean operands")
		}
		return l && r, nil

	case "OR":
		l, ok1 := left.(bool)
		r, ok2 := right.(bool)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("eval: OR requires boolean operands")
		}
		return l || r, nil

	case "=":
		return valuesEqual(left, right), nil
	case "!=":
		return !valuesEqual(left, right), nil
	case "<":
		return compareValues(left, right) < 0, nil
	case ">":
		return compareValues(left, right) > 0, nil
	case "<=":
		return compareValues(left, right) <= 0, nil
	case ">=":
		return compareValues(left, right) >= 0, nil
	}

	return nil, fmt.Errorf("eval: unknown binary operator %q", op)
}

func applyUnaryOp(op string, operand Value) (Value, error) {
	if op == "NOT" {
		b, ok := operand.(bool)
		if !ok {
			return nil, fmt.Errorf("eval: NOT requires boolean operand")
		}
		return !b, nil
	}
	return nil, fmt.Errorf("eval: unknown unary operator %q", op)
}

// valuesEqual compares two values for equality.
func valuesEqual(a, b Value) bool {
	if a == nil && b == nil {
		return true
	}
	switch av := a.(type) {
	case int64:
		switch bv := b.(type) {
		case int64:
			return av == bv
		case float64:
			return float64(av) == bv
		}
	case float64:
		switch bv := b.(type) {
		case float64:
			return av == bv
		case int64:
			return av == float64(bv)
		}
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	}
	return false
}

// compareValues returns -1, 0, or +1 for ordering comparisons.
func compareValues(a, b Value) int {
	toFloat := func(v Value) (float64, bool) {
		switch x := v.(type) {
		case int64:
			return float64(x), true
		case float64:
			return x, true
		}
		return 0, false
	}

	fa, aIsNum := toFloat(a)
	fb, bIsNum := toFloat(b)
	if aIsNum && bIsNum {
		if fa < fb {
			return -1
		} else if fa > fb {
			return 1
		}
		return 0
	}

	switch av := a.(type) {
	case string:
		if bv, ok := b.(string); ok {
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		}
	case bool:
		if bv, ok := b.(bool); ok {
			// false < true
			if !av && bv {
				return -1
			} else if av && !bv {
				return 1
			}
			return 0
		}
	}
	return 0
}

// ---- Internal expression types (bridge between parser AST and evaluator) ----
// These are simple wrappers used by the executor to avoid importing parser in result.go

type literalExpr struct{ value Value }
type columnRefExpr struct{ name string }
type qualifiedRefExpr struct{ table, col string }
type binaryExpr struct {
	left, right interface{}
	op          string
}
type unaryExpr struct {
	operand interface{}
	op      string
}
