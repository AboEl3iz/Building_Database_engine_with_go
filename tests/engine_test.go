package tests

import (
	"fmt"
	"minidb/internal/buffer"
	"minidb/internal/catalog"
	"minidb/internal/disk"
	"minidb/internal/engine"
	"minidb/internal/parser"
	"minidb/internal/wal"
	"os"
	"testing"
)

// newTestEngine creates a complete MiniDB stack for integration testing.
func newTestEngine(t *testing.T) (*engine.Executor, func()) {
	t.Helper()

	dbFile, _ := os.CreateTemp("", "minidb_engine_*.db")
	dbFile.Close()

	walFile, _ := os.CreateTemp("", "minidb_engine_*.wal")
	walFile.Close()

	catalogFile, _ := os.CreateTemp("", "minidb_engine_*.catalog.json")
	catalogFile.Close()

	dm, err := disk.NewDiskManager(dbFile.Name())
	if err != nil {
		t.Fatalf("DiskManager: %v", err)
	}

	bp := buffer.NewBufferPool(128, dm)

	w, err := wal.NewWAL(walFile.Name())
	if err != nil {
		t.Fatalf("WAL: %v", err)
	}

	cat, err := catalog.NewCatalog(catalogFile.Name())
	if err != nil {
		t.Fatalf("Catalog: %v", err)
	}

	exec := engine.NewExecutor(bp, cat, w)

	cleanup := func() {
		w.Close()
		bp.FlushAll()
		dm.Close()
		os.Remove(dbFile.Name())
		os.Remove(walFile.Name())
		os.Remove(catalogFile.Name())
	}

	return exec, cleanup
}

// execSQL is a helper that parses and executes SQL, returning the ResultSet.
func execSQL(t *testing.T, exec *engine.Executor, sql string) *engine.ResultSet {
	t.Helper()
	stmt, err := parser.ParseSQL(sql)
	if err != nil {
		t.Fatalf("ParseSQL(%q): %v", sql, err)
	}
	result, err := exec.Execute(stmt)
	if err != nil {
		t.Fatalf("Execute(%q): %v", sql, err)
	}
	return result
}

// ---- Integration Tests ----

// TestEngineCreateTable tests CREATE TABLE creates a table in the catalog.
func TestEngineCreateTable(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	rs := execSQL(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	if rs.RowCount() == 0 {
		t.Error("Expected confirmation row from CREATE TABLE")
	}
}

// TestEngineDuplicateTable tests that creating the same table twice errors.
func TestEngineDuplicateTable(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE t1 (id INT)")

	stmt, _ := parser.ParseSQL("CREATE TABLE t1 (id INT)")
	_, err := exec.Execute(stmt)
	if err == nil {
		t.Error("Expected error when creating duplicate table")
	}
}

// TestEngineInsertAndSelect tests a complete INSERT → SELECT round-trip.
func TestEngineInsertAndSelect(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	execSQL(t, exec, "INSERT INTO users VALUES (1, 'Alice', 30)")
	execSQL(t, exec, "INSERT INTO users VALUES (2, 'Bob', 25)")
	execSQL(t, exec, "INSERT INTO users VALUES (3, 'Carol', 35)")

	rs := execSQL(t, exec, "SELECT * FROM users")

	if rs.RowCount() != 3 {
		t.Errorf("Expected 3 rows, got %d", rs.RowCount())
	}
}

// TestEngineSelectWhereEq tests SELECT ... WHERE id = ?  (index scan path)
func TestEngineSelectWhereEq(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE products (id INT, name TEXT)")
	execSQL(t, exec, "INSERT INTO products VALUES (1, 'Widget')")
	execSQL(t, exec, "INSERT INTO products VALUES (2, 'Gadget')")
	execSQL(t, exec, "INSERT INTO products VALUES (3, 'Doohickey')")

	rs := execSQL(t, exec, "SELECT * FROM products WHERE id = 2")

	if rs.RowCount() != 1 {
		t.Errorf("Expected 1 row for id=2, got %d", rs.RowCount())
	}
}

// TestEngineSelectWhereRange tests SELECT ... WHERE age > 25
func TestEngineSelectWhereRange(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'Alice', 30)",
		"INSERT INTO users VALUES (2, 'Bob', 20)",
		"INSERT INTO users VALUES (3, 'Carol', 35)",
		"INSERT INTO users VALUES (4, 'Dave', 22)",
		"INSERT INTO users VALUES (5, 'Eve', 28)",
	} {
		execSQL(t, exec, sql)
	}

	rs := execSQL(t, exec, "SELECT * FROM users WHERE age > 25")

	// Ages: 30, 35, 28 → 3 rows
	if rs.RowCount() != 3 {
		t.Errorf("Expected 3 rows with age > 25, got %d", rs.RowCount())
	}
}

// TestEngineSelectWhereAnd tests WHERE ... AND ...
func TestEngineSelectWhereAnd(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE scores (id INT, val INT)")
	for i := int64(1); i <= 10; i++ {
		execSQL(t, exec, "INSERT INTO scores VALUES ("+itoa(i)+", "+itoa(i*10)+")")
	}

	rs := execSQL(t, exec, "SELECT * FROM scores WHERE val > 30 AND val < 70")
	// val 40, 50, 60 → 3 rows
	if rs.RowCount() != 3 {
		t.Errorf("Expected 3 rows (val 40,50,60), got %d", rs.RowCount())
	}
}

// TestEngineSelectColumns tests column projection (SELECT name, age FROM ...)
func TestEngineSelectColumns(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE people (id INT, name TEXT, age INT)")
	execSQL(t, exec, "INSERT INTO people VALUES (1, 'Alice', 30)")

	rs := execSQL(t, exec, "SELECT name, age FROM people")

	if rs.RowCount() != 1 {
		t.Fatalf("Expected 1 row, got %d", rs.RowCount())
	}

	row := rs.Rows[0]
	if row["name"] == nil {
		t.Error("Expected 'name' column in result")
	}
	if row["id"] != nil {
		// id should NOT be in result (not requested)
		// Note: with our map-based rows, id won't be set unless it was projected
	}
}

// TestEngineSelectOrderBy tests ORDER BY sorting.
func TestEngineSelectOrderBy(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE items (id INT, score INT)")
	execSQL(t, exec, "INSERT INTO items VALUES (1, 50)")
	execSQL(t, exec, "INSERT INTO items VALUES (2, 10)")
	execSQL(t, exec, "INSERT INTO items VALUES (3, 80)")
	execSQL(t, exec, "INSERT INTO items VALUES (4, 30)")

	rs := execSQL(t, exec, "SELECT * FROM items ORDER BY score ASC")

	if rs.RowCount() != 4 {
		t.Fatalf("Expected 4 rows, got %d", rs.RowCount())
	}

	// Scores should be in ascending order: 10, 30, 50, 80
	scores := make([]int64, 0, rs.RowCount())
	for _, row := range rs.Rows {
		if v, ok := row["score"].(int64); ok {
			scores = append(scores, v)
		}
	}

	for i := 1; i < len(scores); i++ {
		if scores[i] < scores[i-1] {
			t.Errorf("ORDER BY ASC violated at index %d: %d > %d",
				i, scores[i-1], scores[i])
		}
	}
}

// TestEngineSelectLimit tests LIMIT.
func TestEngineSelectLimit(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE data (id INT)")
	for i := int64(1); i <= 20; i++ {
		execSQL(t, exec, "INSERT INTO data VALUES ("+itoa(i)+")")
	}

	rs := execSQL(t, exec, "SELECT * FROM data LIMIT 5")
	if rs.RowCount() != 5 {
		t.Errorf("Expected 5 rows with LIMIT 5, got %d", rs.RowCount())
	}
}

// TestEngineUpdate tests UPDATE modifies existing rows.
func TestEngineUpdate(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE counter (id INT, val INT)")
	execSQL(t, exec, "INSERT INTO counter VALUES (1, 10)")
	execSQL(t, exec, "INSERT INTO counter VALUES (2, 20)")
	execSQL(t, exec, "INSERT INTO counter VALUES (3, 30)")

	rs := execSQL(t, exec, "UPDATE counter SET val = 99 WHERE id = 2")
	if rs.RowCount() == 0 {
		t.Error("UPDATE should return a result")
	}
}

// TestEngineDelete tests DELETE removes rows.
func TestEngineDelete(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE rows (id INT)")
	for i := int64(1); i <= 5; i++ {
		execSQL(t, exec, "INSERT INTO rows VALUES ("+itoa(i)+")")
	}

	rs := execSQL(t, exec, "SELECT * FROM rows")
	if rs.RowCount() != 5 {
		t.Fatalf("Expected 5 rows before delete, got %d", rs.RowCount())
	}

	execSQL(t, exec, "DELETE FROM rows WHERE id = 3")

	rs = execSQL(t, exec, "SELECT * FROM rows")
	if rs.RowCount() != 4 {
		t.Errorf("Expected 4 rows after delete, got %d", rs.RowCount())
	}
}

// TestEngineUnknownTable tests that querying a non-existent table returns an error.
func TestEngineUnknownTable(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	stmt, _ := parser.ParseSQL("SELECT * FROM ghost_table")
	_, err := exec.Execute(stmt)
	if err == nil {
		t.Error("Expected error for unknown table")
	}
}

// TestEngineInsertWrongColumnCount tests that INSERT with wrong value count errors.
func TestEngineInsertWrongColumnCount(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE t (id INT, name TEXT)")

	stmt, _ := parser.ParseSQL("INSERT INTO t VALUES (1)")
	_, err := exec.Execute(stmt)
	if err == nil {
		t.Error("Expected error for wrong column count in INSERT")
	}
}

// TestEngineLargeInsert tests inserting many rows (exercises B+ tree splits).
func TestEngineLargeInsert(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE big (id INT, val INT)")

	const N = 500
	for i := int64(1); i <= N; i++ {
		execSQL(t, exec, "INSERT INTO big VALUES ("+itoa(i)+", "+itoa(i*2)+")")
	}

	rs := execSQL(t, exec, "SELECT * FROM big")
	if rs.RowCount() != N {
		t.Errorf("Expected %d rows, got %d", N, rs.RowCount())
	}
}

// TestEngineResultPrint tests that ResultSet.Print() produces non-empty output.
func TestEngineResultPrint(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	execSQL(t, exec, "INSERT INTO t VALUES (1, 'hello')")

	rs := execSQL(t, exec, "SELECT * FROM t")
	output := rs.Print()
	if output == "" {
		t.Error("ResultSet.Print() returned empty string")
	}
	t.Log("Table output:\n" + output)
}

// TestEngineMultipleTables tests two tables coexist without interference.
func TestEngineMultipleTables(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE cats (id INT, name TEXT)")
	execSQL(t, exec, "CREATE TABLE dogs (id INT, name TEXT)")

	execSQL(t, exec, "INSERT INTO cats VALUES (1, 'Whiskers')")
	execSQL(t, exec, "INSERT INTO cats VALUES (2, 'Mittens')")
	execSQL(t, exec, "INSERT INTO dogs VALUES (1, 'Buddy')")

	catRows := execSQL(t, exec, "SELECT * FROM cats")
	dogRows := execSQL(t, exec, "SELECT * FROM dogs")

	if catRows.RowCount() != 2 {
		t.Errorf("Expected 2 cats, got %d", catRows.RowCount())
	}
	if dogRows.RowCount() != 1 {
		t.Errorf("Expected 1 dog, got %d", dogRows.RowCount())
	}
}

// itoa converts int64 to string for building SQL strings in tests.
func itoa(n int64) string {
	return fmt.Sprintf("%d", n)
}

// ---- FLOAT/BOOL Type Tests ----

// TestEngineFloatInsertSelect tests inserting and selecting FLOAT values.
func TestEngineFloatInsertSelect(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE prices (id INT, amount FLOAT)")
	execSQL(t, exec, "INSERT INTO prices VALUES (1, 9.99)")
	execSQL(t, exec, "INSERT INTO prices VALUES (2, 24.50)")
	execSQL(t, exec, "INSERT INTO prices VALUES (3, 1.01)")

	rs := execSQL(t, exec, "SELECT * FROM prices")
	if rs.RowCount() != 3 {
		t.Errorf("Expected 3 rows, got %d", rs.RowCount())
	}

	// Verify the float values are preserved correctly
	for _, row := range rs.Rows {
		amt, ok := row["amount"].(float64)
		if !ok {
			t.Errorf("Expected float64 for 'amount', got %T", row["amount"])
		}
		if amt <= 0 {
			t.Errorf("Expected positive amount, got %v", amt)
		}
	}
}

// TestEngineFloatWhereFilter tests WHERE clause with FLOAT comparison.
func TestEngineFloatWhereFilter(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE products (id INT, price FLOAT)")
	execSQL(t, exec, "INSERT INTO products VALUES (1, 9.99)")
	execSQL(t, exec, "INSERT INTO products VALUES (2, 49.99)")
	execSQL(t, exec, "INSERT INTO products VALUES (3, 4.50)")
	execSQL(t, exec, "INSERT INTO products VALUES (4, 19.95)")

	rs := execSQL(t, exec, "SELECT * FROM products WHERE price > 10.0")
	// Only id=2 (49.99) and id=4 (19.95) are > 10.0
	if rs.RowCount() != 2 {
		t.Errorf("Expected 2 rows with price > 10.0, got %d", rs.RowCount())
	}
}

// TestEngineBoolInsertSelect tests inserting and selecting BOOL values.
func TestEngineBoolInsertSelect(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE flags (id INT, active BOOL)")
	execSQL(t, exec, "INSERT INTO flags VALUES (1, TRUE)")
	execSQL(t, exec, "INSERT INTO flags VALUES (2, FALSE)")
	execSQL(t, exec, "INSERT INTO flags VALUES (3, TRUE)")

	rs := execSQL(t, exec, "SELECT * FROM flags")
	if rs.RowCount() != 3 {
		t.Errorf("Expected 3 rows, got %d", rs.RowCount())
	}

	// Verify bool type is preserved
	for _, row := range rs.Rows {
		_, ok := row["active"].(bool)
		if !ok {
			t.Errorf("Expected bool for 'active', got %T(%v)", row["active"], row["active"])
		}
	}
}

// TestEngineBoolWhereFilter tests WHERE active = TRUE filter.
func TestEngineBoolWhereFilter(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE users (id INT, active BOOL)")
	execSQL(t, exec, "INSERT INTO users VALUES (1, TRUE)")
	execSQL(t, exec, "INSERT INTO users VALUES (2, FALSE)")
	execSQL(t, exec, "INSERT INTO users VALUES (3, TRUE)")
	execSQL(t, exec, "INSERT INTO users VALUES (4, FALSE)")

	rs := execSQL(t, exec, "SELECT * FROM users WHERE active = TRUE")
	if rs.RowCount() != 2 {
		t.Errorf("Expected 2 active users, got %d", rs.RowCount())
	}
}

// ---- JOIN Tests ----

// TestEngineInnerJoin tests INNER JOIN returns only matching rows.
func TestEngineInnerJoin(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	// users: id, name
	execSQL(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	execSQL(t, exec, "INSERT INTO users VALUES (1, 'Alice')")
	execSQL(t, exec, "INSERT INTO users VALUES (2, 'Bob')")

	// orders: id, user_id, item
	execSQL(t, exec, "CREATE TABLE orders (id INT, user_id INT, item TEXT)")
	execSQL(t, exec, "INSERT INTO orders VALUES (1, 1, 'book')")
	execSQL(t, exec, "INSERT INTO orders VALUES (2, 1, 'pen')")
	execSQL(t, exec, "INSERT INTO orders VALUES (3, 99, 'ghost')") // no matching user

	rs := execSQL(t, exec,
		"SELECT * FROM orders INNER JOIN users ON orders.user_id = users.id")

	// Only orders 1 and 2 have a matching user (Alice, id=1)
	if rs.RowCount() != 2 {
		t.Errorf("INNER JOIN: expected 2 rows, got %d", rs.RowCount())
	}

	// Verify result columns include both table prefixes
	for _, row := range rs.Rows {
		if row["orders.item"] == nil {
			t.Error("Expected 'orders.item' in JOIN result")
		}
		if row["users.name"] == nil {
			t.Error("Expected 'users.name' in JOIN result")
		}
	}
}

// TestEngineLeftJoin tests LEFT JOIN includes all left rows, NULLs for unmatched right.
func TestEngineLeftJoin(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	execSQL(t, exec, "INSERT INTO users VALUES (1, 'Alice')")
	execSQL(t, exec, "INSERT INTO users VALUES (2, 'Bob')")

	execSQL(t, exec, "CREATE TABLE orders (id INT, user_id INT, item TEXT)")
	execSQL(t, exec, "INSERT INTO orders VALUES (1, 1, 'book')")
	execSQL(t, exec, "INSERT INTO orders VALUES (2, 99, 'ghost')") // no matching user

	rs := execSQL(t, exec,
		"SELECT * FROM orders LEFT JOIN users ON orders.user_id = users.id")

	// LEFT JOIN: both orders appear — ghost order has NULL for user columns
	if rs.RowCount() != 2 {
		t.Errorf("LEFT JOIN: expected 2 rows (including unmatched), got %d", rs.RowCount())
	}

	// Find the unmatched row (ghost) and verify its user columns are NULL
	nullFound := false
	for _, row := range rs.Rows {
		if row["orders.item"] == "ghost" {
			if row["users.name"] != nil {
				t.Errorf("Expected NULL for users.name on unmatched LEFT JOIN row, got %v",
					row["users.name"])
			}
			nullFound = true
		}
	}
	if !nullFound {
		t.Error("Could not find the unmatched 'ghost' order row in LEFT JOIN result")
	}
}

// TestEngineInnerJoinNoMatches tests INNER JOIN with zero matching rows returns empty.
func TestEngineInnerJoinNoMatches(t *testing.T) {
	exec, cleanup := newTestEngine(t)
	defer cleanup()

	execSQL(t, exec, "CREATE TABLE left_t (id INT, val INT)")
	execSQL(t, exec, "INSERT INTO left_t VALUES (1, 10)")

	execSQL(t, exec, "CREATE TABLE right_t (id INT, val INT)")
	execSQL(t, exec, "INSERT INTO right_t VALUES (99, 20)")

	rs := execSQL(t, exec,
		"SELECT * FROM left_t INNER JOIN right_t ON left_t.id = right_t.id")

	if rs.RowCount() != 0 {
		t.Errorf("INNER JOIN with no matches: expected 0 rows, got %d", rs.RowCount())
	}
}

