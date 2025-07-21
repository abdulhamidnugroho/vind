package service

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"vind/backend/helper"
	"vind/backend/internal/model"

	"github.com/lib/pq"
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
		SELECT 
			c.column_name,
			c.data_type,
			c.is_nullable,
			c.column_default,
			-- Check if column is part of a unique constraint
			EXISTS (
				SELECT 1 FROM information_schema.table_constraints tc
				JOIN information_schema.constraint_column_usage ccu 
					ON tc.constraint_name = ccu.constraint_name
				WHERE tc.constraint_type = 'UNIQUE'
					AND tc.table_schema = c.table_schema
					AND tc.table_name = c.table_name
					AND ccu.column_name = c.column_name
			) AS is_unique,
			-- Get foreign key reference if any
			(
				SELECT
					pg_get_constraintdef(con.oid)
				FROM
					pg_constraint con
					INNER JOIN pg_class rel ON rel.oid = con.conrelid
					INNER JOIN pg_attribute att ON att.attrelid = rel.oid AND att.attnum = ANY(con.conkey)
				WHERE
					con.contype = 'f'
					AND rel.relname = $2
					AND att.attname = c.column_name
					AND rel.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)
				LIMIT 1
			) AS foreign_key
		FROM information_schema.columns c
		WHERE c.table_schema = $1 AND c.table_name = $2
		ORDER BY c.ordinal_position;
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
		var defaultVal sql.NullString
		var isUnique sql.NullBool
		var foreignKey sql.NullString

		err := rows.Scan(&col.Name, &col.Type, &nullable, &defaultVal, &isUnique, &foreignKey)
		if err != nil {
			return nil, err
		}

		col.Nullable = nullable == "YES"
		if defaultVal.Valid {
			col.Default = defaultVal.String
		}
		col.IsUnique = isUnique.Valid && isUnique.Bool
		if foreignKey.Valid {
			col.ForeignKey = foreignKey.String
		}

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

	query := fmt.Sprintf(`SELECT * FROM "%s"."%s"`, req.Schema, req.Table)
	args := []any{}
	conditions := []string{}

	for _, f := range req.Filters {
		parts := strings.SplitN(f, ":", 3)
		if len(parts) != 3 {
			continue
		}
		col := pq.QuoteIdentifier(parts[0])
		op := strings.ToUpper(parts[1])
		val := parts[2]

		switch op {
		case "LIKE", "=", ">", "<", ">=", "<=":
			conditions = append(conditions, fmt.Sprintf("%s %s $%d", col, op, len(args)+1))
			args = append(args, val)
		default:
			continue // skip unsupported operator
		}
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	if req.OrderBy != "" && helper.IsValidIdentifier(req.OrderBy) {
		query += " ORDER BY " + pq.QuoteIdentifier(req.OrderBy)
	}

	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(args)+1, len(args)+2)
	args = append(args, limitInt, offsetInt)

	rows, err := p.db.Query(query, args...)
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

func (c *PostgresClient) CreateTable(tableName string, columns []model.ColumnDef) error {
	if tableName == "" || len(columns) == 0 {
		return fmt.Errorf("invalid table definition")
	}

	var colDefs []string
	var pkCols []string

	for _, col := range columns {
		colParts := []string{pq.QuoteIdentifier(col.Name), col.Type}
		if col.NotNull {
			colParts = append(colParts, "NOT NULL")
		}
		if col.Default != "" {
			colParts = append(colParts, "DEFAULT "+col.Default)
		}
		colDefs = append(colDefs, strings.Join(colParts, " "))

		if col.PrimaryKey {
			pkCols = append(pkCols, pq.QuoteIdentifier(col.Name))
		}
	}

	if len(pkCols) > 0 {
		colDefs = append(colDefs, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pkCols, ", ")))
	}

	query := fmt.Sprintf(
		"CREATE TABLE %s (%s);",
		pq.QuoteIdentifier(tableName),
		strings.Join(colDefs, ", "),
	)

	_, err := c.db.Exec(query)
	return err
}

func (c *PostgresClient) AlterTable(tableName string, ops []model.AlterTableOperation) error {
	if tableName == "" || len(ops) == 0 {
		return fmt.Errorf("invalid alter table request")
	}

	var statements []string
	for _, op := range ops {
		switch op.Action {
		case "add_column":
			if op.ColumnName == "" || op.Type == "" {
				return fmt.Errorf("add_column requires column_name and type")
			}
			stmt := fmt.Sprintf("ADD COLUMN %s %s", pq.QuoteIdentifier(op.ColumnName), op.Type)
			statements = append(statements, stmt)

		case "drop_column":
			if op.ColumnName == "" {
				return fmt.Errorf("drop_column requires column_name")
			}
			stmt := fmt.Sprintf("DROP COLUMN %s", pq.QuoteIdentifier(op.ColumnName))
			statements = append(statements, stmt)

		case "rename_column":
			if op.ColumnName == "" || op.NewName == "" {
				return fmt.Errorf("rename_column requires column_name and new_name")
			}
			stmt := fmt.Sprintf("RENAME COLUMN %s TO %s", pq.QuoteIdentifier(op.ColumnName), pq.QuoteIdentifier(op.NewName))
			statements = append(statements, stmt)

		case "alter_column":
			if op.ColumnName == "" {
				return fmt.Errorf("alter_column requires column_name")
			}
			if op.Type != "" {
				stmt := fmt.Sprintf("ALTER COLUMN %s TYPE %s", pq.QuoteIdentifier(op.ColumnName), op.Type)
				statements = append(statements, stmt)
			}
			if op.NotNull != nil {
				if *op.NotNull {
					statements = append(statements, fmt.Sprintf("ALTER COLUMN %s SET NOT NULL", pq.QuoteIdentifier(op.ColumnName)))
				} else {
					statements = append(statements, fmt.Sprintf("ALTER COLUMN %s DROP NOT NULL", pq.QuoteIdentifier(op.ColumnName)))
				}
			}
			if op.Default != "" {
				statements = append(statements, fmt.Sprintf("ALTER COLUMN %s SET DEFAULT %s", pq.QuoteIdentifier(op.ColumnName), op.Default))
			}

		default:
			return fmt.Errorf("unsupported action: %s", op.Action)
		}
	}

	query := fmt.Sprintf("ALTER TABLE %s %s;", pq.QuoteIdentifier(tableName), strings.Join(statements, ", "))
	_, err := c.db.Exec(query)
	return err
}

func (c *PostgresClient) DropTable(tableName string, cascade bool) error {
	if tableName == "" {
		return fmt.Errorf("table name is required")
	}

	query := fmt.Sprintf("DROP TABLE %s", pq.QuoteIdentifier(tableName))
	if cascade {
		query += " CASCADE"
	}
	query += ";"

	_, err := c.db.Exec(query)
	return err
}
