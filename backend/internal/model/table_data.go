package model

type TableDataRequest struct {
	Schema  string   `json:"schema"`
	Table   string   `json:"table"`
	Limit   string   `json:"limit"`
	Offset  string   `json:"offset"`
	OrderBy string   `json:"order_by"` // optional
	Filters []string `json:"filter"`   // e.g. ["name:like:john", "age:>=:30"]
}

type TableDataResponse struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

type CreateTableRequest struct {
	TableName string      `json:"table_name" binding:"required"`
	Columns   []ColumnDef `json:"columns" binding:"required,dive"`
}

type ColumnDef struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	PrimaryKey bool   `json:"primary_key"`
	NotNull    bool   `json:"not_null"`
	Default    string `json:"default,omitempty"`
}
