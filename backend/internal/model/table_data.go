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
