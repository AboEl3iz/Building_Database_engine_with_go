// MiniDB — A mini SQLite/Postgres-style storage engine built from scratch in Go.
//
// This is the entry point: an interactive REPL (Read-Eval-Print Loop) that
// accepts SQL commands and executes them against the storage engine.
//
// Architecture recap:
//
//	User SQL input
//	  → Parser (lexer + recursive descent)
//	  → Executor (planner + evaluator)
//	  → B+ Tree (indexed storage)
//	  → Buffer Pool (LRU page cache)
//	  → Disk Manager (raw file I/O)
//	  → WAL (crash recovery log)
//
// Usage:
//
//	go run cmd/minidb/main.go [database_file]
//
// If no file is given, defaults to "minidb.db".
// Meta-commands (start with \):
//
//	\tables         — list all tables
//	\desc <table>   — show table schema
//	\quit or \exit  — exit the REPL
package main

import (
	"bufio"
	"fmt"
	"minidb/internal/buffer"
	"minidb/internal/catalog"
	"minidb/internal/disk"
	"minidb/internal/engine"
	"minidb/internal/parser"
	"minidb/internal/wal"
	"os"
	"strings"
)

const (
	// BufferPoolSize is how many 4KB pages we keep in memory.
	// 256 pages × 4KB = 1MB of buffer pool. Fine for a mini database.
	BufferPoolSize = 256

	// Default file names for the database, WAL log, and catalog.
	defaultDBFile      = "minidb.db"
	defaultWALFile     = "minidb.wal"
	defaultCatalogFile = "minidb.catalog.json"
)

func main() {
	// Determine database file from command-line argument
	dbFile := defaultDBFile
	if len(os.Args) > 1 {
		dbFile = os.Args[1]
	}

	walFile := dbFile + ".wal"
	catalogFile := dbFile + ".catalog.json"

	fmt.Printf("MiniDB — starting up\n")
	fmt.Printf("  Database : %s\n", dbFile)
	fmt.Printf("  WAL      : %s\n", walFile)
	fmt.Printf("  Catalog  : %s\n", catalogFile)
	fmt.Println()

	// ---- Initialize storage layers (bottom to top) ----

	// Phase 1: Disk Manager — raw page I/O
	dm, err := disk.NewDiskManager(dbFile)
	if err != nil {
		fatalf("Cannot open database file: %v", err)
	}
	defer dm.Close()

	// Phase 2: Buffer Pool — in-memory page cache with LRU eviction
	bp := buffer.NewBufferPool(BufferPoolSize, dm)

	// Phase 4: WAL — write-ahead log for crash recovery
	w, err := wal.NewWAL(walFile)
	if err != nil {
		fatalf("Cannot open WAL: %v", err)
	}
	defer w.Close()

	// Phase 3 (catalog): Table metadata store
	cat, err := catalog.NewCatalog(catalogFile)
	if err != nil {
		fatalf("Cannot open catalog: %v", err)
	}

	// Phase 6: Query Executor
	exec := engine.NewExecutor(bp, cat, w)

	// ---- Start REPL ----
	fmt.Println("MiniDB ready. Type SQL or \\help for commands.")
	fmt.Println()
	runREPL(exec, cat, bp)

	// Flush all dirty pages on exit
	if err := bp.FlushAll(); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: flush on exit failed: %v\n", err)
	}
}

// runREPL is the main interactive loop.
// It reads SQL lines, executes them, and prints results.
func runREPL(exec *engine.Executor, cat *catalog.Catalog, bp *buffer.BufferPool) {
	scanner := bufio.NewScanner(os.Stdin)
	var inputBuffer strings.Builder // accumulates multi-line SQL

	for {
		// Prompt changes when we're in the middle of a multi-line statement
		if inputBuffer.Len() == 0 {
			fmt.Print("minidb> ")
		} else {
			fmt.Print("     -> ")
		}

		// Read one line
		if !scanner.Scan() {
			// EOF (Ctrl+D) — exit gracefully
			fmt.Println("\nBye!")
			return
		}

		line := scanner.Text()

		// ---- Meta-commands (start with \) ----
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "\\") {
			handleMetaCommand(trimmed, exec, cat, bp)
			inputBuffer.Reset()
			continue
		}

		// Accumulate input (support multi-line SQL)
		inputBuffer.WriteString(line)
		inputBuffer.WriteString(" ")

		// Only execute when we see a semicolon (or the line is a complete statement)
		sql := strings.TrimSpace(inputBuffer.String())
		if !strings.Contains(sql, ";") && len(sql) > 0 {
			// No semicolon yet — wait for more input
			// Exception: if it looks like a complete one-word command, try it
			if !looksIncomplete(sql) {
				// Try parsing anyway
			} else {
				continue
			}
		}

		if sql == "" {
			inputBuffer.Reset()
			continue
		}

		// Strip trailing semicolon for the parser
		sql = strings.TrimSuffix(strings.TrimSpace(sql), ";")
		sql = strings.TrimSpace(sql)

		if sql == "" {
			inputBuffer.Reset()
			continue
		}

		// ---- Parse and Execute ----
		stmt, err := parser.ParseSQL(sql)
		if err != nil {
			fmt.Printf("Parse error: %v\n\n", err)
			inputBuffer.Reset()
			continue
		}

		result, err := exec.Execute(stmt)
		if err != nil {
			fmt.Printf("Error: %v\n\n", err)
			inputBuffer.Reset()
			continue
		}

		// Print result
		fmt.Print(result.Print())
		fmt.Println()
		inputBuffer.Reset()
	}
}

// handleMetaCommand processes REPL-specific commands (not SQL).
func handleMetaCommand(cmd string, exec *engine.Executor, cat *catalog.Catalog, bp *buffer.BufferPool) {
	parts := strings.Fields(cmd)
	switch parts[0] {
	case "\\quit", "\\exit", "\\q":
		fmt.Println("Bye!")
		os.Exit(0)

	case "\\help", "\\h":
		printHelp()

	case "\\tables", "\\t":
		tables := cat.ListTables()
		if len(tables) == 0 {
			fmt.Println("(no tables)")
		} else {
			fmt.Println("Tables:")
			for _, t := range tables {
				fmt.Printf("  %s\n", t)
			}
		}
		fmt.Println()

	case "\\desc", "\\d":
		if len(parts) < 2 {
			fmt.Println("Usage: \\desc <table_name>")
			return
		}
		desc, err := exec.DescribeTable(parts[1])
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Print(desc)
		}
		fmt.Println()

	case "\\pool":
		fmt.Printf("Buffer pool: %d total frames, %d free\n\n",
			bp.PoolSize(), bp.FreeFrames())

	default:
		fmt.Printf("Unknown command: %s (try \\help)\n\n", parts[0])
	}
}

// printHelp shows available commands.
func printHelp() {
	fmt.Print(`MiniDB Help
-----------
SQL Commands:
  CREATE TABLE name (col TYPE, ...)   Create a new table (types: INT, TEXT)
  INSERT INTO name VALUES (v1, v2)    Insert a row
  SELECT * FROM name [WHERE ...]      Query rows
  SELECT col1,col2 FROM name          Query with column selection
  UPDATE name SET col=val [WHERE ...] Update rows
  DELETE FROM name [WHERE ...]        Delete rows

WHERE operators: =  !=  <  >  <=  >=  AND  OR  NOT

Meta-commands:
  \tables      List all tables
  \desc name   Show table schema
  \pool        Show buffer pool stats
  \help        Show this help
  \quit        Exit MiniDB

Examples:
  CREATE TABLE users (id INT, name TEXT, age INT);
  INSERT INTO users VALUES (1, 'Alice', 30);
  INSERT INTO users VALUES (2, 'Bob', 25);
  SELECT * FROM users;
  SELECT * FROM users WHERE age > 25;
  SELECT name FROM users WHERE id = 1;
  UPDATE users SET age = 31 WHERE id = 1;
  DELETE FROM users WHERE id = 2;
`)
}

// looksIncomplete returns true if the SQL looks like it needs more input.
// Heuristic: it doesn't end with a semicolon and doesn't look like a complete statement.
func looksIncomplete(sql string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	// Very simple heuristic — if we started a keyword but have no semicolon, wait
	startsWithKeyword := strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "INSERT") ||
		strings.HasPrefix(upper, "UPDATE") ||
		strings.HasPrefix(upper, "DELETE") ||
		strings.HasPrefix(upper, "CREATE")
	return startsWithKeyword
}

// fatalf prints an error and exits.
func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(1)
}
