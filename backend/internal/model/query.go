package model

type QueryRequest struct {
	SQL string `json:"sql"`
}

type QueryResponse struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}
