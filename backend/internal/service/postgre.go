package service

import (
	"database/sql"

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

func (p *PostgresClient) RunQuery(query string) ([]map[string]interface{}, error) {
	rows, err := p.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	results := []map[string]interface{}{}
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))

		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		rowMap := map[string]interface{}{}
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			rowMap[colName] = *val
		}

		results = append(results, rowMap)
	}

	return results, nil
}
