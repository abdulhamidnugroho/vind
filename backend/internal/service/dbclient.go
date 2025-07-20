package service

import "vind/backend/internal/model"

type DBClient interface {
	Connect(dsn string) error
	Disconnect() error
	ListSchemas() ([]string, error)
	ListTables(schema string) ([]string, error)
	ListColumns(schema, table string) ([]model.Column, error)
	ExecuteQuery(query string) ([]string, [][]any, error)
	GetTableData(req model.TableDataRequest) ([]string, [][]any, error)
	InsertRecord(schema, table string, data map[string]any) error
	UpdateRecord(schema, table string, data, where map[string]any) (int64, error)
	DeleteRecord(schema, table string, conditions map[string]any) (int64, error)
	CreateTable(tableName string, columns []model.ColumnDef) error
	AlterTable(tableName string, ops []model.AlterTableOperation) error
}
