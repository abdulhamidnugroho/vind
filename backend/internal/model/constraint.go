package model

type AddConstraintParams struct {
	TableName      string   `json:"table_name"`
	ConstraintName string   `json:"constraint_name"`
	Type           string   `json:"type"`                  // "PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CHECK"
	Columns        []string `json:"columns"`               // columns for PK/UNIQUE
	RefTable       string   `json:"ref_table,omitempty"`   // required for FK
	RefColumns     []string `json:"ref_columns,omitempty"` // required for FK
	OnDelete       string   `json:"on_delete,omitempty"`
	OnUpdate       string   `json:"on_update,omitempty"`
	CheckExpr      string   `json:"check_expr,omitempty"` // required for CHECK
}

type ConstraintInfo struct {
	ConstraintName string `json:"constraint_name"`
	ConstraintType string `json:"constraint_type"`
	TableName      string `json:"table_name"`
	Definition     string `json:"definition"`
}
