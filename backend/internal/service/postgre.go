package service

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"vind/backend/helper"
	"vind/backend/internal/model"

	_ "github.com/lib/pq"
)

type PostgresClient struct {
	db *sql.DB
}

func NewPostgresClient() *PostgresClient {
	return &PostgresClient{}
}

func (p *PostgresClient) Connect(dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	p.db = db
	return db.Ping()
}

func (p *PostgresClient) Disconnect() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

func (p *PostgresClient) ListSchemas() ([]string, error) {
	rows, err := p.db.Query(`SELECT schema_name FROM information_schema.schemata`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		schemas = append(schemas, name)
	}
	return schemas, nil
}

func (p *PostgresClient) ListTables(schema string) ([]string, error) {
	if schema == "" {
		schema = "public"
	}

	query := `SELECT table_name FROM information_schema.tables WHERE table_schema = $1`
	rows, err := p.db.Query(query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, nil
}

func (p *PostgresClient) ListColumns(schema, table string) ([]model.Column, error) {
	query := `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position;
	`

	rows, err := p.db.Query(query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []model.Column
	for rows.Next() {
		var col model.Column
		var nullable string
		if err := rows.Scan(&col.Name, &col.Type, &nullable); err != nil {
			return nil, err
		}
		col.Nullable = nullable == "YES"
		columns = append(columns, col)
	}

	return columns, nil
}

func (p *PostgresClient) ExecuteQuery(sql string) ([]string, [][]any, error) {
	trimmed := strings.TrimSpace(strings.ToUpper(sql))
	if !strings.HasPrefix(trimmed, "SELECT") {
		// For non-SELECT queries, use Exec
		_, err := p.db.Exec(sql)
		if err != nil {
			return nil, nil, err
		}
		// Return no columns or rows
		return nil, nil, nil
	}

	// SELECT query
	rows, err := p.db.Query(sql)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	var results [][]any
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, nil, err
		}
		results = append(results, values)
	}

	return columns, results, nil
}

func (p *PostgresClient) GetTableData(req model.TableDataRequest) ([]string, [][]any, error) {
	if !helper.IsValidIdentifier(req.Schema) || !helper.IsValidIdentifier(req.Table) {
		return nil, nil, errors.New("invalid schema or table name")
	}

	limitInt, err := strconv.Atoi(req.Limit)
	if err != nil || limitInt < 0 {
		return nil, nil, fmt.Errorf("invalid limit")
	}

	offsetInt, err := strconv.Atoi(req.Offset)
	if err != nil || offsetInt < 0 {
		return nil, nil, fmt.Errorf("invalid offset")
	}

	query := fmt.Sprintf(`SELECT * FROM "%s"."%s" LIMIT $1 OFFSET $2`, req.Schema, req.Table)
	rows, err := p.db.Query(query, limitInt, offsetInt)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	var results [][]any
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, nil, err
		}
		results = append(results, values)
	}

	return columns, results, nil
}

func (p *PostgresClient) InsertRecord(schema, table string, data map[string]any) error {
	if len(data) == 0 {
		return errors.New("no data to insert")
	}

	columns := []string{}
	placeholders := []string{}
	values := []any{}

	i := 1
	for col, val := range data {
		columns = append(columns, col)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		values = append(values, val)
		i++
	}

	query := fmt.Sprintf(
		`INSERT INTO "%s"."%s" (%s) VALUES (%s)`,
		schema,
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	_, err := p.db.Exec(query, values...)
	return err
}

func (p *PostgresClient) UpdateRecord(schema, table string, data, where map[string]any) (int64, error) {
	if len(data) == 0 {
		return 0, errors.New("no fields to update")
	}
	if len(where) == 0 {
		return 0, errors.New("missing WHERE clause — dangerous update prevented")
	}

	setClauses := []string{}
	whereClauses := []string{}
	values := []any{}
	i := 1

	// Build SET clause
	for col, val := range data {
		if !helper.IsValidIdentifier(col) {
			return 0, fmt.Errorf("invalid column name: %s", col)
		}
		setClauses = append(setClauses, fmt.Sprintf(`"%s" = $%d`, col, i))
		values = append(values, val)
		i++
	}

	// Build WHERE clause
	for col, val := range where {
		if !helper.IsValidIdentifier(col) {
			return 0, fmt.Errorf("invalid column name: %s", col)
		}
		whereClauses = append(whereClauses, fmt.Sprintf(`"%s" = $%d`, col, i))
		values = append(values, val)
		i++
	}

	query := fmt.Sprintf(
		`UPDATE "%s"."%s" SET %s WHERE %s`,
		schema,
		table,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)

	result, err := p.db.Exec(query, values...)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}

func (p *PostgresClient) DeleteRecord(schema, table string, conditions map[string]any) (int64, error) {
	if schema == "" {
		schema = "public"
	}

	if table == "" || len(conditions) == 0 {
		return 0, fmt.Errorf("table name and conditions are required")
	}

	query := fmt.Sprintf(`DELETE FROM "%s"."%s" WHERE `, schema, table)

	var args []any
	var conds []string
	i := 1
	for col, val := range conditions {
		if !helper.IsValidIdentifier(col) {
			return 0, fmt.Errorf("invalid column name: %s", col)
		}
		conds = append(conds, fmt.Sprintf(`"%s" = $%d`, col, i))
		args = append(args, val)
		i++
	}
	query += strings.Join(conds, " AND ")

	result, err := p.db.Exec(query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute delete: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}
