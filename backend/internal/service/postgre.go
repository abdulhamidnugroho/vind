package service

import (
	"database/sql"
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

func (p *PostgresClient) RunQuery(query string) ([]map[string]any, error) {
	rows, err := p.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	results := []map[string]any{}
	for rows.Next() {
		columns := make([]any, len(cols))
		columnPointers := make([]any, len(cols))

		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		rowMap := map[string]any{}
		for i, colName := range cols {
			val := columnPointers[i].(*any)
			rowMap[colName] = *val
		}

		results = append(results, rowMap)
	}

	return results, nil
}
