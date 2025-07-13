package service

type DBClient interface {
	Connect(dsn string) error
	Disconnect() error
	ListSchemas() ([]string, error)
	ListTables(schema string) ([]string, error)
	RunQuery(query string) ([]map[string]any, error)
}
