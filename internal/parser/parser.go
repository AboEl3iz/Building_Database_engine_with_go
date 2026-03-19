package parser

import (
	"fmt"
	"strconv"
)


// Parser is a recursive descent parser for MiniDB's SQL subset.
//
// CONCEPT: Recursive Descent Parsing
// Each grammar rule becomes a method. Methods call each other recursively.
//
// SQL grammar (simplified):
//   Statement    → SelectStmt | InsertStmt | UpdateStmt | DeleteStmt | CreateStmt
//   SelectStmt   → SELECT columns FROM ident [WHERE expr] [ORDER BY ident [ASC|DESC]] [LIMIT int]
//   InsertStmt   → INSERT INTO ident VALUES (expr, ...)
//   UpdateStmt   → UPDATE ident SET assignment, ... [WHERE expr]
//   DeleteStmt   → DELETE FROM ident [WHERE expr]
//   CreateStmt   → CREATE TABLE ident (colDef, ...)
//
//   expr         → andExpr (OR andExpr)*
//   andExpr      → notExpr (AND notExpr)*
//   notExpr      → NOT? comparison
//   comparison   → primary (op primary)?
//   primary      → ident | literal | (expr)
//
//   op           → = | != | < | > | <= | >=
//   literal      → INT_LIT | STR_LIT
//   ident        → IDENT
//
// The grammar is left-to-right, so OR has lower precedence than AND,
// which has lower precedence than NOT, which is lower than comparisons.
// This means "a > 1 OR b < 2 AND c = 3" parses as "a > 1 OR (b < 2 AND c = 3)".
type Parser struct {
	tokens []Token
	pos    int // current position in tokens slice
}

// NewParser creates a Parser for the given token stream.
// Get tokens from: lexer.Tokenize()
func NewParser(tokens []Token) *Parser {
	return &Parser{tokens: tokens, pos: 0}
}

// Parse parses a complete SQL statement.
// This is the top-level entry point.
//
// Usage:
//   lexer := NewLexer("SELECT * FROM users;")
//   tokens, _ := lexer.Tokenize()
//   parser := NewParser(tokens)
//   stmt, _ := parser.Parse()
func (p *Parser) Parse() (Statement, error) {
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}

	// Consume optional trailing semicolon
	if p.peek().Type == TokenSEMICOLON {
		p.consume()
	}

	// After the statement (and optional semicolon), we should be at EOF
	if p.peek().Type != TokenEOF {
		tok := p.peek()
		return nil, fmt.Errorf("parser: unexpected token %q at L%d:C%d after statement", tok.Literal, tok.Line, tok.Col)
	}

	return stmt, nil
}

// ParseSQL is a convenience function that lexes and parses in one call.
func ParseSQL(sql string) (Statement, error) {
	lexer := NewLexer(sql)
	tokens, err := lexer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("lexer error: %w", err)
	}
	parser := NewParser(tokens)
	return parser.Parse()
}

// parseStatement dispatches to the appropriate statement parser
// based on the first keyword.
func (p *Parser) parseStatement() (Statement, error) {
	tok := p.peek()
	switch tok.Type {
	case TokenSELECT:
		return p.parseSelect()
	case TokenINSERT:
		return p.parseInsert()
	case TokenUPDATE:
		return p.parseUpdate()
	case TokenDELETE:
		return p.parseDelete()
	case TokenCREATE:
		return p.parseCreate()
	default:
		return nil, fmt.Errorf("parser: expected SELECT/INSERT/UPDATE/DELETE/CREATE, got %q at L%d:C%d",
			tok.Literal, tok.Line, tok.Col)
	}
}

// ---- SELECT ----
// Grammar: SELECT (* | col, ...) FROM table [WHERE expr] [ORDER BY col [ASC|DESC]] [LIMIT n]

// parseSelect parses: SELECT (* | col, ...) FROM table [[INNER|LEFT] JOIN table ON expr] [WHERE expr] [ORDER BY col [ASC|DESC]] [LIMIT n]
func (p *Parser) parseSelect() (*SelectStmt, error) {
	if err := p.expect(TokenSELECT); err != nil {
		return nil, err
	}

	stmt := &SelectStmt{}

	// Parse column list: * or col1, col2, col1.col, ...
	columns, err := p.parseColumnList()
	if err != nil {
		return nil, err
	}
	stmt.Columns = columns

	// FROM table
	if err := p.expect(TokenFROM); err != nil {
		return nil, err
	}
	tableName, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("parser: expected table name after FROM: %w", err)
	}
	stmt.Table = tableName

	// Optional: [INNER|LEFT] JOIN table ON expr
	joinType := ""
	if p.peek().Type == TokenINNER {
		p.consume() // eat INNER
		joinType = "INNER"
	} else if p.peek().Type == TokenLEFT {
		p.consume() // eat LEFT
		joinType = "LEFT"
	}
	if p.peek().Type == TokenJOIN {
		p.consume() // eat JOIN
		if joinType == "" {
			joinType = "INNER" // bare JOIN defaults to INNER
		}
		rightTable, err := p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("parser: expected table name after JOIN: %w", err)
		}
		if err := p.expect(TokenON); err != nil {
			return nil, err
		}
		onExpr, err := p.parseExpr()
		if err != nil {
			return nil, fmt.Errorf("parser: invalid JOIN ON clause: %w", err)
		}
		stmt.Join = &JoinClause{Type: joinType, Table: rightTable, On: onExpr}
	}

	// Optional: WHERE expr
	if p.peek().Type == TokenWHERE {
		p.consume() // eat WHERE
		where, err := p.parseExpr()
		if err != nil {
			return nil, fmt.Errorf("parser: invalid WHERE clause: %w", err)
		}
		stmt.Where = where
	}

	// Optional: ORDER BY col [ASC|DESC]
	if p.peek().Type == TokenORDER {
		p.consume() // eat ORDER
		if err := p.expect(TokenBY); err != nil {
			return nil, err
		}
		col, err := p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("parser: expected column name after ORDER BY: %w", err)
		}
		asc := true
		if p.peek().Type == TokenDESC {
			p.consume()
			asc = false
		} else if p.peek().Type == TokenASC {
			p.consume()
		}
		stmt.OrderBy = &OrderBy{Column: col, Asc: asc}
	}

	// Optional: LIMIT n
	if p.peek().Type == TokenLIMIT {
		p.consume() // eat LIMIT
		n, err := p.expectIntLit()
		if err != nil {
			return nil, fmt.Errorf("parser: expected integer after LIMIT: %w", err)
		}
		stmt.Limit = int(n)
	}

	return stmt, nil
}


// parseColumnList parses the column list in SELECT.
// Returns ["*"] for SELECT *, or individual column names (possibly "table.col" qualified).
func (p *Parser) parseColumnList() ([]string, error) {
	if p.peek().Type == TokenSTAR {
		p.consume()
		return []string{"*"}, nil
	}

	var cols []string
	for {
		col, err := p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("parser: expected column name: %w", err)
		}
		// Support qualified table.column references in the SELECT list
		if p.peek().Type == TokenDOT {
			p.consume() // eat dot
			colName, err := p.expectIdent()
			if err != nil {
				return nil, fmt.Errorf("parser: expected column after '.' in SELECT list: %w", err)
			}
			col = col + "." + colName
		}
		cols = append(cols, col)

		if p.peek().Type != TokenCOMMA {
			break
		}
		p.consume() // eat comma
	}

	if len(cols) == 0 {
		return nil, fmt.Errorf("parser: expected column list or *")
	}
	return cols, nil
}


// ---- INSERT ----
// Grammar: INSERT INTO table VALUES (expr, expr, ...)

func (p *Parser) parseInsert() (*InsertStmt, error) {
	if err := p.expect(TokenINSERT); err != nil {
		return nil, err
	}
	if err := p.expect(TokenINTO); err != nil {
		return nil, err
	}

	tableName, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("parser: expected table name: %w", err)
	}

	if err := p.expect(TokenVALUES); err != nil {
		return nil, err
	}
	if err := p.expect(TokenLPAREN); err != nil {
		return nil, err
	}

	// Parse comma-separated value list
	var values []Expr
	for p.peek().Type != TokenRPAREN && p.peek().Type != TokenEOF {
		val, err := p.parsePrimary()
		if err != nil {
			return nil, fmt.Errorf("parser: expected value in INSERT: %w", err)
		}
		values = append(values, val)

		if p.peek().Type == TokenCOMMA {
			p.consume()
		} else {
			break
		}
	}

	if err := p.expect(TokenRPAREN); err != nil {
		return nil, err
	}

	return &InsertStmt{Table: tableName, Values: values}, nil
}

// ---- UPDATE ----
// Grammar: UPDATE table SET col=val, ... [WHERE expr]

func (p *Parser) parseUpdate() (*UpdateStmt, error) {
	if err := p.expect(TokenUPDATE); err != nil {
		return nil, err
	}

	tableName, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("parser: expected table name: %w", err)
	}

	if err := p.expect(TokenSET); err != nil {
		return nil, err
	}

	// Parse SET assignments: col1=val1, col2=val2, ...
	var assignments []Assignment
	for {
		col, err := p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("parser: expected column name in SET: %w", err)
		}
		if err := p.expect(TokenEQ); err != nil {
			return nil, err
		}
		val, err := p.parsePrimary()
		if err != nil {
			return nil, fmt.Errorf("parser: expected value in SET: %w", err)
		}
		assignments = append(assignments, Assignment{Column: col, Value: val})

		if p.peek().Type != TokenCOMMA {
			break
		}
		p.consume()
	}

	stmt := &UpdateStmt{Table: tableName, Assignments: assignments}

	// Optional WHERE
	if p.peek().Type == TokenWHERE {
		p.consume()
		where, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

// ---- DELETE ----
// Grammar: DELETE FROM table [WHERE expr]

func (p *Parser) parseDelete() (*DeleteStmt, error) {
	if err := p.expect(TokenDELETE); err != nil {
		return nil, err
	}
	if err := p.expect(TokenFROM); err != nil {
		return nil, err
	}

	tableName, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("parser: expected table name: %w", err)
	}

	stmt := &DeleteStmt{Table: tableName}

	if p.peek().Type == TokenWHERE {
		p.consume()
		where, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

// ---- CREATE TABLE ----
// Grammar: CREATE TABLE name (col1 TYPE, col2 TYPE, ...)

func (p *Parser) parseCreate() (*CreateTableStmt, error) {
	if err := p.expect(TokenCREATE); err != nil {
		return nil, err
	}
	if err := p.expect(TokenTABLE); err != nil {
		return nil, err
	}

	tableName, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("parser: expected table name: %w", err)
	}

	if err := p.expect(TokenLPAREN); err != nil {
		return nil, err
	}

	var cols []ColumnDef
	for p.peek().Type != TokenRPAREN && p.peek().Type != TokenEOF {
		colName, err := p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("parser: expected column name: %w", err)
		}

		colType, err := p.parseDataType()
		if err != nil {
			return nil, err
		}

		cols = append(cols, ColumnDef{Name: colName, Type: colType})

		if p.peek().Type == TokenCOMMA {
			p.consume()
		} else {
			break
		}
	}

	if err := p.expect(TokenRPAREN); err != nil {
		return nil, err
	}

	return &CreateTableStmt{TableName: tableName, Columns: cols}, nil
}

func (p *Parser) parseDataType() (DataType, error) {
	tok := p.peek()
	switch tok.Type {
	case TokenINT:
		p.consume()
		return DataTypeInt, nil
	case TokenTEXT:
		p.consume()
		return DataTypeText, nil
	case TokenFLOAT:
		p.consume()
		return DataTypeFloat, nil
	case TokenBOOL:
		p.consume()
		return DataTypeBool, nil
	default:
		return 0, fmt.Errorf("parser: expected INT, TEXT, FLOAT, or BOOL, got %q at L%d:C%d", tok.Literal, tok.Line, tok.Col)
	}
}


// ---- Expression Parsing ----
//
// Expression precedence (lowest to highest):
//   OR  →  AND  →  NOT  →  comparison  →  primary
//
// This means we parse OR first (at the top level of an expression),
// and comparisons last (at the leaves).

// parseExpr parses an expression with the lowest precedence operator (OR).
// Grammar: andExpr (OR andExpr)*
func (p *Parser) parseExpr() (Expr, error) {
	left, err := p.parseAndExpr()
	if err != nil {
		return nil, err
	}

	for p.peek().Type == TokenOR {
		p.consume() // eat OR
		right, err := p.parseAndExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: "OR", Right: right}
	}

	return left, nil
}

// parseAndExpr parses AND expressions.
// Grammar: notExpr (AND notExpr)*
func (p *Parser) parseAndExpr() (Expr, error) {
	left, err := p.parseNotExpr()
	if err != nil {
		return nil, err
	}

	for p.peek().Type == TokenAND {
		p.consume() // eat AND
		right, err := p.parseNotExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: "AND", Right: right}
	}

	return left, nil
}

// parseNotExpr parses NOT expressions.
// Grammar: NOT? comparison
func (p *Parser) parseNotExpr() (Expr, error) {
	if p.peek().Type == TokenNOT {
		p.consume() // eat NOT
		operand, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: "NOT", Operand: operand}, nil
	}
	return p.parseComparison()
}

// parseComparison parses comparison expressions.
// Grammar: primary (compOp primary)?
// compOp: = | != | < | > | <= | >=
func (p *Parser) parseComparison() (Expr, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	tok := p.peek()
	var op string
	switch tok.Type {
	case TokenEQ:
		op = "="
	case TokenNEQ:
		op = "!="
	case TokenLT:
		op = "<"
	case TokenGT:
		op = ">"
	case TokenLTE:
		op = "<="
	case TokenGTE:
		op = ">="
	default:
		return left, nil // no comparison operator — just return the primary
	}

	p.consume() // eat operator
	right, err := p.parsePrimary()
	if err != nil {
		return nil, fmt.Errorf("parser: expected right-hand side of %s: %w", op, err)
	}

	return &BinaryExpr{Left: left, Op: op, Right: right}, nil
}

// parsePrimary parses the most basic expression units:
// - integer literal: 42
// - float literal: 3.14
// - boolean literal: TRUE, FALSE
// - string literal: 'Alice'
// - qualified column reference: table.column
// - plain column reference: age, name, id
// - parenthesized expression: (expr)
func (p *Parser) parsePrimary() (Expr, error) {
	tok := p.peek()

	switch tok.Type {
	case TokenINTLIT:
		p.consume()
		n, err := strconv.ParseInt(tok.Literal, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parser: invalid integer %q: %w", tok.Literal, err)
		}
		return &Literal{Value: n}, nil

	case TokenFLOATLIT:
		p.consume()
		f, err := strconv.ParseFloat(tok.Literal, 64)
		if err != nil {
			return nil, fmt.Errorf("parser: invalid float %q: %w", tok.Literal, err)
		}
		return &Literal{Value: f}, nil

	case TokenTRUE:
		p.consume()
		return &Literal{Value: true}, nil

	case TokenFALSE:
		p.consume()
		return &Literal{Value: false}, nil

	case TokenSTRLIT:
		p.consume()
		return &Literal{Value: tok.Literal}, nil

	case TokenIDENT:
		p.consume()
		// Check for qualified table.column reference
		if p.peek().Type == TokenDOT {
			p.consume() // eat dot
			colName, err := p.expectIdent()
			if err != nil {
				return nil, fmt.Errorf("parser: expected column name after '.': %w", err)
			}
			return &QualifiedRef{Table: tok.Literal, Column: colName}, nil
		}
		return &ColumnRef{Name: tok.Literal}, nil

	case TokenLPAREN:
		p.consume() // eat (
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(TokenRPAREN); err != nil {
			return nil, err
		}
		return expr, nil

	default:
		return nil, fmt.Errorf("parser: unexpected token %q (type %s) at L%d:C%d",
			tok.Literal, tok.Type, tok.Line, tok.Col)
	}
}


// ---- Token navigation helpers ----

// peek returns the current token without consuming it.
func (p *Parser) peek() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos]
}

// consume advances past the current token and returns it.
func (p *Parser) consume() Token {
	tok := p.peek()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return tok
}

// expect consumes a token of the given type, or returns an error.
// Use this to assert that the next token must be a specific keyword or symbol.
func (p *Parser) expect(t TokenType) error {
	tok := p.peek()
	if tok.Type != t {
		return fmt.Errorf("parser: expected %s, got %q (type %s) at L%d:C%d",
			t, tok.Literal, tok.Type, tok.Line, tok.Col)
	}
	p.consume()
	return nil
}

// expectIdent expects and consumes an identifier token, returning its literal.
func (p *Parser) expectIdent() (string, error) {
	tok := p.peek()
	if tok.Type != TokenIDENT {
		return "", fmt.Errorf("parser: expected identifier, got %q (type %s) at L%d:C%d",
			tok.Literal, tok.Type, tok.Line, tok.Col)
	}
	p.consume()
	return tok.Literal, nil
}

// expectIntLit expects and consumes an integer literal, returning its value.
func (p *Parser) expectIntLit() (int64, error) {
	tok := p.peek()
	if tok.Type != TokenINTLIT {
		return 0, fmt.Errorf("parser: expected integer, got %q at L%d:C%d",
			tok.Literal, tok.Line, tok.Col)
	}
	p.consume()
	n, err := strconv.ParseInt(tok.Literal, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parser: invalid integer %q: %w", tok.Literal, err)
	}
	return n, nil
}
