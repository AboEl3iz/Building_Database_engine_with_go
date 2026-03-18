// Package catalog manages table and schema metadata (the "data dictionary").
//
// CONCEPT: What is a catalog?
// The catalog stores metadata ABOUT the database: what tables exist,
// what columns each table has, where their B+ tree root pages are, etc.
//
// Without a catalog, the database engine doesn't know:
//   - What tables exist
//   - What columns a table has
//   - What data types columns have
//   - Where the B+ tree for a table is stored (which root page)
//
// The catalog itself is persisted — it must survive restarts.
// For MiniDB, we use a simple in-memory catalog backed by a JSON file.
// Production databases (Postgres, MySQL) store the catalog in special
// "system tables" in the same database file.
package catalog

import (
	"encoding/json"
	"fmt"
	"minidb/internal/disk"
	"minidb/internal/parser"
	"os"
	"sync"
)

// TableSchema describes the structure of one table.
type TableSchema struct {
	Name       string              // table name (e.g., "users")
	Columns    []ColumnSchema      // ordered list of columns
	RootPageID disk.PageID         // PageID of the B+ tree root for this table
	ColIndex   map[string]int      // column name → index (for fast lookup)
}

// ColumnSchema describes one column.
type ColumnSchema struct {
	Name     string          // column name (e.g., "age")
	Type     parser.DataType // INT or TEXT
	Offset   int             // column's position (0-based) in a row's value array
}

// Catalog manages all table schemas.
// Thread-safe: RWMutex allows concurrent reads, exclusive writes.
type Catalog struct {
	mu       sync.RWMutex
	tables   map[string]*TableSchema // table name → schema
	filePath string                  // path to persist catalog JSON
}

// NewCatalog creates an in-memory catalog.
// If filePath exists, it loads the catalog from disk.
// If it doesn't exist, starts with an empty catalog.
func NewCatalog(filePath string) (*Catalog, error) {
	c := &Catalog{
		tables:   make(map[string]*TableSchema),
		filePath: filePath,
	}

	// Try to load existing catalog
	data, err := os.ReadFile(filePath)
	if err == nil && len(data) > 0 {
		if err := c.loadFromJSON(data); err != nil {
			return nil, fmt.Errorf("catalog: failed to load from %q: %w", filePath, err)
		}
	}
	// If file doesn't exist, that's fine — we start empty

	return c, nil
}

// CreateTable registers a new table schema in the catalog.
// Returns an error if a table with the same name already exists.
func (c *Catalog) CreateTable(name string, columns []parser.ColumnDef, rootPageID disk.PageID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[name]; exists {
		return fmt.Errorf("catalog: table %q already exists", name)
	}

	// Build column schemas with offsets
	colSchemas := make([]ColumnSchema, len(columns))
	colIndex := make(map[string]int, len(columns))
	for i, col := range columns {
		colSchemas[i] = ColumnSchema{
			Name:   col.Name,
			Type:   col.Type,
			Offset: i,
		}
		colIndex[col.Name] = i
	}

	c.tables[name] = &TableSchema{
		Name:       name,
		Columns:    colSchemas,
		RootPageID: rootPageID,
		ColIndex:   colIndex,
	}

	return c.persist()
}

// GetTable returns the schema for a table by name.
// Returns (nil, error) if the table doesn't exist.
func (c *Catalog) GetTable(name string) (*TableSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schema, ok := c.tables[name]
	if !ok {
		return nil, fmt.Errorf("catalog: table %q does not exist", name)
	}
	return schema, nil
}

// TableExists returns true if a table with the given name exists.
func (c *Catalog) TableExists(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.tables[name]
	return ok
}

// ListTables returns all table names.
func (c *Catalog) ListTables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	names := make([]string, 0, len(c.tables))
	for name := range c.tables {
		names = append(names, name)
	}
	return names
}

// DropTable removes a table from the catalog.
func (c *Catalog) DropTable(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.tables[name]; !ok {
		return fmt.Errorf("catalog: table %q does not exist", name)
	}
	delete(c.tables, name)
	return c.persist()
}

// UpdateRootPageID updates the root page for a table.
// Called when a B+ tree's root changes (e.g., after root split).
func (c *Catalog) UpdateRootPageID(tableName string, newRootID disk.PageID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	schema, ok := c.tables[tableName]
	if !ok {
		return fmt.Errorf("catalog: table %q not found", tableName)
	}
	schema.RootPageID = newRootID
	return c.persist()
}

// ---- Serialization ----
// We persist the catalog as JSON. This is simple but not the most efficient.
// Production DBs store the catalog in the database file itself (as system pages).

// catalogJSON is the on-disk format for the catalog.
type catalogJSON struct {
	Tables []tableJSON `json:"tables"`
}

type tableJSON struct {
	Name       string       `json:"name"`
	Columns    []columnJSON `json:"columns"`
	RootPageID uint32       `json:"root_page_id"`
}

type columnJSON struct {
	Name   string `json:"name"`
	Type   int    `json:"type"` // 0 = INT, 1 = TEXT
	Offset int    `json:"offset"`
}

// persist writes the catalog to disk as JSON.
// Called after every write (CreateTable, DropTable).
func (c *Catalog) persist() error {
	var cj catalogJSON
	for _, schema := range c.tables {
		tj := tableJSON{
			Name:       schema.Name,
			RootPageID: uint32(schema.RootPageID),
		}
		for _, col := range schema.Columns {
			tj.Columns = append(tj.Columns, columnJSON{
				Name:   col.Name,
				Type:   int(col.Type),
				Offset: col.Offset,
			})
		}
		cj.Tables = append(cj.Tables, tj)
	}

	data, err := json.MarshalIndent(cj, "", "  ")
	if err != nil {
		return fmt.Errorf("catalog: JSON marshal failed: %w", err)
	}

	if err := os.WriteFile(c.filePath, data, 0644); err != nil {
		return fmt.Errorf("catalog: write failed: %w", err)
	}
	return nil
}

// loadFromJSON restores the catalog from JSON bytes.
func (c *Catalog) loadFromJSON(data []byte) error {
	var cj catalogJSON
	if err := json.Unmarshal(data, &cj); err != nil {
		return err
	}

	for _, tj := range cj.Tables {
		colSchemas := make([]ColumnSchema, len(tj.Columns))
		colIndex := make(map[string]int, len(tj.Columns))
		for i, col := range tj.Columns {
			colSchemas[i] = ColumnSchema{
				Name:   col.Name,
				Type:   parser.DataType(col.Type),
				Offset: col.Offset,
			}
			colIndex[col.Name] = i
		}
		c.tables[tj.Name] = &TableSchema{
			Name:       tj.Name,
			Columns:    colSchemas,
			RootPageID: disk.PageID(tj.RootPageID),
			ColIndex:   colIndex,
		}
	}
	return nil
}
