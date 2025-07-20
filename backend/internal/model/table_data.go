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

type AlterTableOperation struct {
	Action     string `json:"action" binding:"required"` // "add_column", "drop_column", "rename_column", "alter_column"
	ColumnName string `json:"column_name,omitempty"`
	NewName    string `json:"new_name,omitempty"` // For rename_column
	Type       string `json:"type,omitempty"`     // For add_column or alter_column
	NotNull    *bool  `json:"not_null,omitempty"` // For alter_column
	Default    string `json:"default,omitempty"`  // For alter_column
}

type AlterTableRequest struct {
	Operations []AlterTableOperation `json:"operations" binding:"required,min=1,dive"`
}
