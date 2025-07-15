package service

import "vind/backend/internal/model"

type DBClient interface {
	Connect(dsn string) error
	Disconnect() error
	ListSchemas() ([]string, error)
	ListTables(schema string) ([]string, error)
	ListColumns(schema, table string) ([]model.Column, error)
	ExecuteQuery(query string) ([]string, [][]any, error)
}
