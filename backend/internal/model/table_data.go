package model

type TableDataRequest struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	Limit  string `json:"limit"`
	Offset string `json:"offset"`
}

type TableDataResponse struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}
