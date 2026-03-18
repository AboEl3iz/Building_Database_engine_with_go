// Package parser implements the SQL lexer and recursive-descent parser for MiniDB.
//
// CONCEPT: How a SQL parser works
// Raw SQL string → Lexer → Token stream → Parser → AST (Abstract Syntax Tree)
//
// The AST is a tree of Go structs that represents the meaning of the query:
//
//   "SELECT name, age FROM users WHERE age > 25"
//
//   SelectStmt{
//     Columns: ["name", "age"],
//     Table:   "users",
//     Where: BinaryExpr{
//       Left:  ColumnRef{Name: "age"},
//       Op:    ">",
//       Right: Literal{Value: 25},
//     },
//   }
//
// The Query Engine then walks this AST to produce a result.
package parser

import (
	"fmt"
	"strings"
	"unicode"
)

// TokenType identifies the type of a token.
// Tokens are the atoms of the SQL language — the smallest meaningful units.
type TokenType int

const (
	// ---- Keywords ----
	// SQL reserved words. The lexer converts these from IDENT to their specific type.
	TokenSELECT TokenType = iota
	TokenINSERT
	TokenUPDATE
	TokenDELETE
	TokenCREATE
	TokenDROP
	TokenFROM
	TokenWHERE
	TokenINTO
	TokenVALUES
	TokenSET
	TokenTABLE
	TokenORDER
	TokenBY
	TokenASC
	TokenDESC
	TokenLIMIT
	TokenAND
	TokenOR
	TokenNOT
	TokenINT    // INT data type keyword
	TokenTEXT   // TEXT data type keyword

	// ---- Literals ----
	TokenIDENT   // identifier: table name, column name, etc. (e.g., "users", "age")
	TokenINTLIT  // integer literal: 42, -7, 0
	TokenSTRLIT  // string literal: 'Alice', 'hello world'

	// ---- Symbols ----
	TokenSTAR      // *
	TokenEQ        // =
	TokenNEQ       // != or <>
	TokenLT        // <
	TokenGT        // >
	TokenLTE       // <=
	TokenGTE       // >=
	TokenLPAREN    // (
	TokenRPAREN    // )
	TokenCOMMA     // ,
	TokenSEMICOLON // ;
	TokenDOT       // .

	// ---- Special ----
	TokenEOF     // end of input — always the last token
	TokenILLEGAL // unrecognized character
)

// tokenTypeNames maps TokenType to a human-readable string for debugging.
var tokenTypeNames = map[TokenType]string{
	TokenSELECT: "SELECT", TokenINSERT: "INSERT", TokenUPDATE: "UPDATE",
	TokenDELETE: "DELETE", TokenCREATE: "CREATE", TokenDROP: "DROP",
	TokenFROM: "FROM", TokenWHERE: "WHERE", TokenINTO: "INTO",
	TokenVALUES: "VALUES", TokenSET: "SET", TokenTABLE: "TABLE",
	TokenORDER: "ORDER", TokenBY: "BY", TokenASC: "ASC", TokenDESC: "DESC",
	TokenLIMIT: "LIMIT", TokenAND: "AND", TokenOR: "OR", TokenNOT: "NOT",
	TokenINT: "INT", TokenTEXT: "TEXT",
	TokenIDENT: "IDENT", TokenINTLIT: "INT_LIT", TokenSTRLIT: "STR_LIT",
	TokenSTAR: "*", TokenEQ: "=", TokenNEQ: "!=", TokenLT: "<", TokenGT: ">",
	TokenLTE: "<=", TokenGTE: ">=",
	TokenLPAREN: "(", TokenRPAREN: ")", TokenCOMMA: ",", TokenSEMICOLON: ";", TokenDOT: ".",
	TokenEOF: "EOF", TokenILLEGAL: "ILLEGAL",
}

func (t TokenType) String() string {
	if s, ok := tokenTypeNames[t]; ok {
		return s
	}
	return fmt.Sprintf("TOKEN(%d)", int(t))
}

// keywords maps uppercase SQL keyword strings to their TokenType.
// The lexer uses this to distinguish keywords from user-defined identifiers.
var keywords = map[string]TokenType{
	"SELECT": TokenSELECT, "INSERT": TokenINSERT, "UPDATE": TokenUPDATE,
	"DELETE": TokenDELETE, "CREATE": TokenCREATE, "DROP": TokenDROP,
	"FROM": TokenFROM, "WHERE": TokenWHERE, "INTO": TokenINTO,
	"VALUES": TokenVALUES, "SET": TokenSET, "TABLE": TokenTABLE,
	"ORDER": TokenORDER, "BY": TokenBY, "ASC": TokenASC, "DESC": TokenDESC,
	"LIMIT": TokenLIMIT, "AND": TokenAND, "OR": TokenOR, "NOT": TokenNOT,
	"INT": TokenINT, "TEXT": TokenTEXT,
}

// Token is one atomic unit of the SQL input.
type Token struct {
	Type    TokenType
	Literal string // the raw text that produced this token
	Line    int    // 1-based line number (for error messages)
	Col     int    // 1-based column (for error messages)
}

func (t Token) String() string {
	return fmt.Sprintf("Token{%s %q L%d:C%d}", t.Type, t.Literal, t.Line, t.Col)
}

// ---- Lexer ----

// Lexer (also called a tokenizer or scanner) converts a raw SQL string into
// a sequence of tokens.
//
// It processes the input character by character, using a small state machine
// to recognize multi-character tokens like identifiers, numbers, and strings.
//
// Example:
//   Input:  "SELECT * FROM users WHERE id = 1;"
//   Output: [SELECT] [*] [FROM] [IDENT:users] [WHERE] [IDENT:id] [=] [INT_LIT:1] [;] [EOF]
type Lexer struct {
	input []rune // the full SQL input as Unicode code points
	pos   int    // current position in input
	line  int    // current line number (for errors)
	col   int    // current column (for errors)
}

// NewLexer creates a Lexer for the given SQL input string.
func NewLexer(input string) *Lexer {
	return &Lexer{
		input: []rune(input),
		pos:   0,
		line:  1,
		col:   1,
	}
}

// Tokenize scans the entire input and returns all tokens including the final EOF.
// This is a "batch" mode — we collect all tokens upfront.
// The parser then works from this slice (allows lookahead without re-scanning).
func (l *Lexer) Tokenize() ([]Token, error) {
	var tokens []Token
	for {
		tok, err := l.nextToken()
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, tok)
		if tok.Type == TokenEOF {
			break
		}
	}
	return tokens, nil
}

// nextToken scans and returns the next token from the input.
func (l *Lexer) nextToken() (Token, error) {
	// Skip whitespace and comments
	l.skipWhitespace()

	if l.pos >= len(l.input) {
		return l.makeToken(TokenEOF, ""), nil
	}

	ch := l.input[l.pos]
	startLine, startCol := l.line, l.col

	// ---- Single-character tokens ----
	switch ch {
	case '*':
		l.advance()
		return Token{TokenSTAR, "*", startLine, startCol}, nil
	case '(':
		l.advance()
		return Token{TokenLPAREN, "(", startLine, startCol}, nil
	case ')':
		l.advance()
		return Token{TokenRPAREN, ")", startLine, startCol}, nil
	case ',':
		l.advance()
		return Token{TokenCOMMA, ",", startLine, startCol}, nil
	case ';':
		l.advance()
		return Token{TokenSEMICOLON, ";", startLine, startCol}, nil
	case '.':
		l.advance()
		return Token{TokenDOT, ".", startLine, startCol}, nil
	case '=':
		l.advance()
		return Token{TokenEQ, "=", startLine, startCol}, nil
	}

	// ---- Multi-character symbols ----
	if ch == '<' {
		l.advance()
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.advance()
			return Token{TokenLTE, "<=", startLine, startCol}, nil
		} else if l.pos < len(l.input) && l.input[l.pos] == '>' {
			l.advance()
			return Token{TokenNEQ, "<>", startLine, startCol}, nil
		}
		return Token{TokenLT, "<", startLine, startCol}, nil
	}
	if ch == '>' {
		l.advance()
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.advance()
			return Token{TokenGTE, ">=", startLine, startCol}, nil
		}
		return Token{TokenGT, ">", startLine, startCol}, nil
	}
	if ch == '!' {
		l.advance()
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.advance()
			return Token{TokenNEQ, "!=", startLine, startCol}, nil
		}
		return Token{TokenILLEGAL, "!", startLine, startCol}, nil
	}

	// ---- String literals ('...') ----
	if ch == '\'' {
		return l.readStringLiteral(startLine, startCol)
	}

	// ---- Numeric literals (and negative numbers) ----
	if unicode.IsDigit(ch) || (ch == '-' && l.pos+1 < len(l.input) && unicode.IsDigit(l.input[l.pos+1])) {
		return l.readIntLiteral(startLine, startCol)
	}

	// ---- Identifiers and keywords ----
	if unicode.IsLetter(ch) || ch == '_' {
		return l.readIdentOrKeyword(startLine, startCol)
	}

	// Unrecognized character
	l.advance()
	return Token{TokenILLEGAL, string(ch), startLine, startCol}, nil
}

// readStringLiteral reads a single-quoted string: 'hello world'
// Supports escaped single quotes: 'it''s a test' → "it's a test"
func (l *Lexer) readStringLiteral(line, col int) (Token, error) {
	l.advance() // consume opening '
	var sb strings.Builder

	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\'' {
			l.advance()
			// Check for escaped quote: ''
			if l.pos < len(l.input) && l.input[l.pos] == '\'' {
				sb.WriteRune('\'')
				l.advance()
				continue
			}
			return Token{TokenSTRLIT, sb.String(), line, col}, nil
		}
		if ch == '\n' {
			return Token{}, fmt.Errorf("lexer L%d:C%d: unterminated string literal", line, col)
		}
		sb.WriteRune(ch)
		l.advance()
	}
	return Token{}, fmt.Errorf("lexer L%d:C%d: unterminated string literal", line, col)
}

// readIntLiteral reads an integer literal (possibly negative).
func (l *Lexer) readIntLiteral(line, col int) (Token, error) {
	var sb strings.Builder
	if l.input[l.pos] == '-' {
		sb.WriteRune('-')
		l.advance()
	}
	for l.pos < len(l.input) && unicode.IsDigit(l.input[l.pos]) {
		sb.WriteRune(l.input[l.pos])
		l.advance()
	}
	return Token{TokenINTLIT, sb.String(), line, col}, nil
}

// readIdentOrKeyword reads an identifier or keyword.
// Identifiers: table/column names like `users`, `first_name`, `_internal`
// Keywords: SELECT, FROM, WHERE, etc. (case-insensitive)
func (l *Lexer) readIdentOrKeyword(line, col int) (Token, error) {
	var sb strings.Builder
	for l.pos < len(l.input) && (unicode.IsLetter(l.input[l.pos]) || unicode.IsDigit(l.input[l.pos]) || l.input[l.pos] == '_') {
		sb.WriteRune(l.input[l.pos])
		l.advance()
	}
	word := sb.String()

	// Check if it's a SQL keyword (case-insensitive)
	upper := strings.ToUpper(word)
	if tt, ok := keywords[upper]; ok {
		return Token{tt, upper, line, col}, nil
	}

	// Otherwise it's a user-defined identifier
	return Token{TokenIDENT, word, line, col}, nil
}

// skipWhitespace advances past spaces, tabs, newlines.
func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(l.input[l.pos]) {
		if l.input[l.pos] == '\n' {
			l.line++
			l.col = 1
		} else {
			l.col++
		}
		l.pos++
	}
}

// advance moves to the next character.
func (l *Lexer) advance() {
	if l.pos < len(l.input) {
		if l.input[l.pos] == '\n' {
			l.line++
			l.col = 1
		} else {
			l.col++
		}
		l.pos++
	}
}

// makeToken creates a Token at the current position.
func (l *Lexer) makeToken(t TokenType, literal string) Token {
	return Token{Type: t, Literal: literal, Line: l.line, Col: l.col}
}
