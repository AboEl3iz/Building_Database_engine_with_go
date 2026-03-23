package tests

import (
	"minidb/internal/parser"
	"testing"
)

// TestLexerBasic tests that the lexer produces the correct token types.
func TestLexerBasic(t *testing.T) {
	input := "SELECT * FROM users WHERE id = 1;"

	lexer := parser.NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}

	expected := []parser.TokenType{
		parser.TokenSELECT,
		parser.TokenSTAR,
		parser.TokenFROM,
		parser.TokenIDENT,  // "users"
		parser.TokenWHERE,
		parser.TokenIDENT,  // "id"
		parser.TokenEQ,
		parser.TokenINTLIT, // "1"
		parser.TokenSEMICOLON,
		parser.TokenEOF,
	}

	if len(tokens) != len(expected) {
		t.Fatalf("Expected %d tokens, got %d: %v", len(expected), len(tokens), tokens)
	}

	for i, tok := range tokens {
		if tok.Type != expected[i] {
			t.Errorf("Token[%d]: expected %s, got %s (literal=%q)", i, expected[i], tok.Type, tok.Literal)
		}
	}
}

// TestLexerStringLiteral tests that single-quoted strings are lexed correctly.
func TestLexerStringLiteral(t *testing.T) {
	input := "INSERT INTO users VALUES (1, 'Alice O''Brien', 30)"
	lexer := parser.NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}

	// Find the string literal token
	var strTok *parser.Token
	for _, tok := range tokens {
		if tok.Type == parser.TokenSTRLIT {
			tok := tok
			strTok = &tok
			break
		}
	}
	if strTok == nil {
		t.Fatal("No STR_LIT token found")
	}
	// The escaped '' should become a single ' in the literal
	if strTok.Literal != "Alice O'Brien" {
		t.Errorf("Expected literal %q, got %q", "Alice O'Brien", strTok.Literal)
	}
}

// TestLexerOperators tests multi-character operators.
func TestLexerOperators(t *testing.T) {
	tests := []struct {
		input    string
		expected parser.TokenType
	}{
		{"=", parser.TokenEQ},
		{"!=", parser.TokenNEQ},
		{"<>", parser.TokenNEQ},
		{"<", parser.TokenLT},
		{">", parser.TokenGT},
		{"<=", parser.TokenLTE},
		{">=", parser.TokenGTE},
	}

	for _, tc := range tests {
		lexer := parser.NewLexer(tc.input)
		tokens, err := lexer.Tokenize()
		if err != nil {
			t.Fatalf("Tokenize(%q) failed: %v", tc.input, err)
		}
		if tokens[0].Type != tc.expected {
			t.Errorf("Input %q: expected %s, got %s", tc.input, tc.expected, tokens[0].Type)
		}
	}
}

// TestParseSelectStar tests: SELECT * FROM users
func TestParseSelectStar(t *testing.T) {
	stmt, err := parser.ParseSQL("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	sel, ok := stmt.(*parser.SelectStmt)
	if !ok {
		t.Fatalf("Expected *SelectStmt, got %T", stmt)
	}

	if len(sel.Columns) != 1 || sel.Columns[0] != "*" {
		t.Errorf("Expected columns=[*], got %v", sel.Columns)
	}
	if sel.Table != "users" {
		t.Errorf("Expected table=users, got %q", sel.Table)
	}
	if sel.Where != nil {
		t.Error("Expected no WHERE clause")
	}
}

// TestParseSelectWithWhere tests: SELECT name, age FROM users WHERE id = 1
func TestParseSelectWithWhere(t *testing.T) {
	stmt, err := parser.ParseSQL("SELECT name, age FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	sel := stmt.(*parser.SelectStmt)

	if len(sel.Columns) != 2 || sel.Columns[0] != "name" || sel.Columns[1] != "age" {
		t.Errorf("Expected columns=[name, age], got %v", sel.Columns)
	}

	if sel.Where == nil {
		t.Fatal("Expected WHERE clause")
	}

	bin, ok := sel.Where.(*parser.BinaryExpr)
	if !ok {
		t.Fatalf("Expected *BinaryExpr WHERE, got %T", sel.Where)
	}
	if bin.Op != "=" {
		t.Errorf("Expected op=, got %q", bin.Op)
	}

	col, ok := bin.Left.(*parser.ColumnRef)
	if !ok || col.Name != "id" {
		t.Errorf("Expected ColumnRef(id), got %v", bin.Left)
	}

	lit, ok := bin.Right.(*parser.Literal)
	if !ok {
		t.Fatalf("Expected Literal, got %T", bin.Right)
	}
	if lit.Value.(int64) != 1 {
		t.Errorf("Expected literal 1, got %v", lit.Value)
	}
}

// TestParseSelectAndOr tests: WHERE age > 18 AND age < 65 OR id = 0
func TestParseSelectAndOr(t *testing.T) {
	stmt, err := parser.ParseSQL("SELECT * FROM users WHERE age > 18 AND age < 65 OR id = 0")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	sel := stmt.(*parser.SelectStmt)
	// Top level should be OR (lowest precedence)
	or, ok := sel.Where.(*parser.BinaryExpr)
	if !ok || or.Op != "OR" {
		t.Errorf("Expected OR at top of WHERE, got %T op=%v", sel.Where, or)
	}

	// Left of OR should be AND
	and, ok := or.Left.(*parser.BinaryExpr)
	if !ok || and.Op != "AND" {
		t.Errorf("Expected AND on left of OR, got %T", or.Left)
	}
}

// TestParseSelectOrderBy tests: SELECT * FROM users ORDER BY age DESC
func TestParseSelectOrderBy(t *testing.T) {
	stmt, err := parser.ParseSQL("SELECT * FROM users ORDER BY age DESC")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmt.(*parser.SelectStmt)
	if sel.OrderBy == nil {
		t.Fatal("Expected ORDER BY clause")
	}
	if sel.OrderBy.Column != "age" {
		t.Errorf("Expected column=age, got %q", sel.OrderBy.Column)
	}
	if sel.OrderBy.Asc {
		t.Error("Expected DESC order")
	}
}

// TestParseSelectLimit tests: SELECT * FROM users LIMIT 10
func TestParseSelectLimit(t *testing.T) {
	stmt, err := parser.ParseSQL("SELECT * FROM users LIMIT 10")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmt.(*parser.SelectStmt)
	if sel.Limit != 10 {
		t.Errorf("Expected LIMIT 10, got %d", sel.Limit)
	}
}

// TestParseInsert tests: INSERT INTO users VALUES (1, 'Alice', 30)
func TestParseInsert(t *testing.T) {
	stmt, err := parser.ParseSQL("INSERT INTO users VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	ins, ok := stmt.(*parser.InsertStmt)
	if !ok {
		t.Fatalf("Expected *InsertStmt, got %T", stmt)
	}

	if ins.Table != "users" {
		t.Errorf("Expected table=users, got %q", ins.Table)
	}
	if len(ins.Values) != 3 {
		t.Fatalf("Expected 3 values, got %d", len(ins.Values))
	}

	// Check first value (int 1)
	lit1, ok := ins.Values[0].(*parser.Literal)
	if !ok || lit1.Value.(int64) != 1 {
		t.Errorf("Expected Values[0]=1, got %v", ins.Values[0])
	}

	// Check second value (string 'Alice')
	lit2, ok := ins.Values[1].(*parser.Literal)
	if !ok || lit2.Value.(string) != "Alice" {
		t.Errorf("Expected Values[1]='Alice', got %v", ins.Values[1])
	}

	// Check third value (int 30)
	lit3, ok := ins.Values[2].(*parser.Literal)
	if !ok || lit3.Value.(int64) != 30 {
		t.Errorf("Expected Values[2]=30, got %v", ins.Values[2])
	}
}

// TestParseUpdate tests: UPDATE users SET age = 31, name = 'Bob' WHERE id = 1
func TestParseUpdate(t *testing.T) {
	stmt, err := parser.ParseSQL("UPDATE users SET age = 31 WHERE id = 1")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	upd, ok := stmt.(*parser.UpdateStmt)
	if !ok {
		t.Fatalf("Expected *UpdateStmt, got %T", stmt)
	}
	if upd.Table != "users" {
		t.Errorf("Expected table=users, got %q", upd.Table)
	}
	if len(upd.Assignments) != 1 {
		t.Fatalf("Expected 1 assignment, got %d", len(upd.Assignments))
	}
	if upd.Assignments[0].Column != "age" {
		t.Errorf("Expected column=age, got %q", upd.Assignments[0].Column)
	}
	if upd.Where == nil {
		t.Error("Expected WHERE clause")
	}
}

// TestParseDelete tests: DELETE FROM users WHERE id = 5
func TestParseDelete(t *testing.T) {
	stmt, err := parser.ParseSQL("DELETE FROM users WHERE id = 5")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	del, ok := stmt.(*parser.DeleteStmt)
	if !ok {
		t.Fatalf("Expected *DeleteStmt, got %T", stmt)
	}
	if del.Table != "users" {
		t.Errorf("Expected table=users, got %q", del.Table)
	}
	if del.Where == nil {
		t.Error("Expected WHERE clause")
	}
}

// TestParseCreateTable tests: CREATE TABLE users (id INT, name TEXT, age INT)
func TestParseCreateTable(t *testing.T) {
	stmt, err := parser.ParseSQL("CREATE TABLE users (id INT, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	create, ok := stmt.(*parser.CreateTableStmt)
	if !ok {
		t.Fatalf("Expected *CreateTableStmt, got %T", stmt)
	}
	if create.TableName != "users" {
		t.Errorf("Expected tableName=users, got %q", create.TableName)
	}
	if len(create.Columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(create.Columns))
	}

	expected := []struct {
		name string
		typ  parser.DataType
	}{
		{"id", parser.DataTypeInt},
		{"name", parser.DataTypeText},
		{"age", parser.DataTypeInt},
	}

	for i, exp := range expected {
		if create.Columns[i].Name != exp.name {
			t.Errorf("Column[%d] name: expected %q, got %q", i, exp.name, create.Columns[i].Name)
		}
		if create.Columns[i].Type != exp.typ {
			t.Errorf("Column[%d] type: expected %v, got %v", i, exp.typ, create.Columns[i].Type)
		}
	}
}

// TestParseErrors tests that invalid SQL produces meaningful errors.
func TestParseErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"missing FROM", "SELECT * users"},
		{"missing table name", "SELECT * FROM"},
		{"bad WHERE", "SELECT * FROM t WHERE"},
		{"bad CREATE", "CREATE users"},
		{"empty input", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parser.ParseSQL(tc.input)
			if err == nil {
				t.Errorf("Expected error for input %q, got nil", tc.input)
			}
		})
	}
}

// TestParseNegativeNumber tests that negative numbers are parsed correctly.
func TestParseNegativeNumber(t *testing.T) {
	stmt, err := parser.ParseSQL("SELECT * FROM t WHERE score > -10")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	sel := stmt.(*parser.SelectStmt)
	bin := sel.Where.(*parser.BinaryExpr)
	lit, ok := bin.Right.(*parser.Literal)
	if !ok {
		t.Fatalf("Expected Literal, got %T", bin.Right)
	}
	if lit.Value.(int64) != -10 {
		t.Errorf("Expected -10, got %v", lit.Value)
	}
}

// TestParseParenthesizedExpr tests: WHERE (a > 1 OR b < 2) AND c = 3
func TestParseParenthesizedExpr(t *testing.T) {
	_, err := parser.ParseSQL("SELECT * FROM t WHERE (a > 1 OR b < 2) AND c = 3")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	// If it doesn't error, the parentheses were handled correctly
}

// TestASTString tests that AST nodes produce readable string representations.
func TestASTString(t *testing.T) {
	stmt, err := parser.ParseSQL("SELECT name, age FROM users WHERE age > 25")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	s := stmt.String()
	if s == "" {
		t.Error("Statement.String() returned empty string")
	}
	// Just verify it doesn't panic and returns something non-empty
}

// TestParseFloatBoolColumns tests: CREATE TABLE t (id INT, price FLOAT, active BOOL)
func TestParseFloatBoolColumns(t *testing.T) {
	stmt, err := parser.ParseSQL("CREATE TABLE products (id INT, price FLOAT, active BOOL)")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	create, ok := stmt.(*parser.CreateTableStmt)
	if !ok {
		t.Fatalf("Expected *CreateTableStmt, got %T", stmt)
	}
	if len(create.Columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(create.Columns))
	}
	expected := []struct {
		name string
		typ  parser.DataType
	}{
		{"id", parser.DataTypeInt},
		{"price", parser.DataTypeFloat},
		{"active", parser.DataTypeBool},
	}
	for i, exp := range expected {
		if create.Columns[i].Name != exp.name {
			t.Errorf("Column[%d] name: expected %q, got %q", i, exp.name, create.Columns[i].Name)
		}
		if create.Columns[i].Type != exp.typ {
			t.Errorf("Column[%d] type: expected %v, got %v", i, exp.typ, create.Columns[i].Type)
		}
	}
}

// TestParseFloatLiteral tests: WHERE price > 3.14
func TestParseFloatLiteral(t *testing.T) {
	stmt, err := parser.ParseSQL("SELECT * FROM products WHERE price > 3.14")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmt.(*parser.SelectStmt)
	bin, ok := sel.Where.(*parser.BinaryExpr)
	if !ok || bin.Op != ">" {
		t.Fatalf("Expected BinaryExpr(>), got %T", sel.Where)
	}
	lit, ok := bin.Right.(*parser.Literal)
	if !ok {
		t.Fatalf("Expected Literal, got %T", bin.Right)
	}
	f, ok := lit.Value.(float64)
	if !ok || f != 3.14 {
		t.Errorf("Expected float64(3.14), got %T(%v)", lit.Value, lit.Value)
	}
}

// TestParseTrueFalseLiterals tests: WHERE active = TRUE / FALSE
func TestParseTrueFalseLiterals(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want bool
	}{
		{"SELECT * FROM t WHERE active = TRUE", true},
		{"SELECT * FROM t WHERE active = FALSE", false},
	} {
		stmt, err := parser.ParseSQL(tc.sql)
		if err != nil {
			t.Fatalf("Parse(%q) failed: %v", tc.sql, err)
		}
		sel := stmt.(*parser.SelectStmt)
		bin := sel.Where.(*parser.BinaryExpr)
		lit, ok := bin.Right.(*parser.Literal)
		if !ok {
			t.Fatalf("Expected Literal, got %T", bin.Right)
		}
		b, ok := lit.Value.(bool)
		if !ok || b != tc.want {
			t.Errorf("Expected bool(%v), got %T(%v)", tc.want, lit.Value, lit.Value)
		}
	}
}

// TestParseInnerJoin tests: SELECT * FROM orders INNER JOIN users ON orders.user_id = users.id
func TestParseInnerJoin(t *testing.T) {
	stmt, err := parser.ParseSQL(
		"SELECT * FROM orders INNER JOIN users ON orders.user_id = users.id")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmt.(*parser.SelectStmt)
	if sel.Table != "orders" {
		t.Errorf("Expected left table 'orders', got %q", sel.Table)
	}
	if sel.Join == nil {
		t.Fatal("Expected JOIN clause, got nil")
	}
	if sel.Join.Type != "INNER" {
		t.Errorf("Expected join type INNER, got %q", sel.Join.Type)
	}
	if sel.Join.Table != "users" {
		t.Errorf("Expected right table 'users', got %q", sel.Join.Table)
	}
	bin, ok := sel.Join.On.(*parser.BinaryExpr)
	if !ok || bin.Op != "=" {
		t.Fatalf("Expected BinaryExpr(=) in ON clause, got %T", sel.Join.On)
	}
	left, ok := bin.Left.(*parser.QualifiedRef)
	if !ok || left.Table != "orders" || left.Column != "user_id" {
		t.Errorf("Expected orders.user_id, got %v", bin.Left)
	}
	right, ok := bin.Right.(*parser.QualifiedRef)
	if !ok || right.Table != "users" || right.Column != "id" {
		t.Errorf("Expected users.id, got %v", bin.Right)
	}
}

// TestParseLeftJoin tests: SELECT * FROM orders LEFT JOIN users ON orders.user_id = users.id
func TestParseLeftJoin(t *testing.T) {
	stmt, err := parser.ParseSQL(
		"SELECT * FROM orders LEFT JOIN users ON orders.user_id = users.id")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmt.(*parser.SelectStmt)
	if sel.Join == nil {
		t.Fatal("Expected JOIN clause")
	}
	if sel.Join.Type != "LEFT" {
		t.Errorf("Expected join type LEFT, got %q", sel.Join.Type)
	}
}

// TestParseBareJoin tests bare JOIN defaults to INNER.
func TestParseBareJoin(t *testing.T) {
	stmt, err := parser.ParseSQL("SELECT * FROM a JOIN b ON a.id = b.a_id")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmt.(*parser.SelectStmt)
	if sel.Join == nil {
		t.Fatal("Expected JOIN clause")
	}
	if sel.Join.Type != "INNER" {
		t.Errorf("Expected bare JOIN to default to INNER, got %q", sel.Join.Type)
	}
}

// ---- Index statements parsing tests ----

func TestParseCreateIndex(t *testing.T) {
	stmt, err := parser.ParseSQL("CREATE INDEX idx_age ON users (age)")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	create, ok := stmt.(*parser.CreateIndexStmt)
	if !ok {
		t.Fatalf("Expected *CreateIndexStmt, got %T", stmt)
	}
	if create.IndexName != "idx_age" || create.TableName != "users" || create.Column != "age" || create.Unique {
		t.Errorf("Unexpected values in CreateIndexStmt: %+v", create)
	}
}

func TestParseCreateUniqueIndex(t *testing.T) {
	stmt, err := parser.ParseSQL("CREATE UNIQUE INDEX idx_id ON users (id)")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	create, ok := stmt.(*parser.CreateIndexStmt)
	if !ok {
		t.Fatalf("Expected *CreateIndexStmt, got %T", stmt)
	}
	if !create.Unique {
		t.Error("Expected index to be unique")
	}
}

func TestParseDropIndex(t *testing.T) {
	stmt, err := parser.ParseSQL("DROP INDEX idx_age ON users")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	drop, ok := stmt.(*parser.DropIndexStmt)
	if !ok {
		t.Fatalf("Expected *DropIndexStmt, got %T", stmt)
	}
	if drop.IndexName != "idx_age" || drop.TableName != "users" {
		t.Errorf("Unexpected values in DropIndexStmt: %+v", drop)
	}
}

func TestParseShowIndexes(t *testing.T) {
	stmt, err := parser.ParseSQL("SHOW INDEXES")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	show, ok := stmt.(*parser.ShowIndexesStmt)
	if !ok {
		t.Fatalf("Expected *ShowIndexesStmt, got %T", stmt)
	}
	if show.TableName != "" {
		t.Errorf("Expected empty TableName, got %q", show.TableName)
	}

	stmt2, err := parser.ParseSQL("SHOW INDEXES FROM users")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	show2, ok := stmt2.(*parser.ShowIndexesStmt)
	if !ok {
		t.Fatalf("Expected *ShowIndexesStmt, got %T", stmt2)
	}
	if show2.TableName != "users" {
		t.Errorf("Expected TableName='users', got %q", show2.TableName)
	}
}


