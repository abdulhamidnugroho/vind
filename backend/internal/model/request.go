package model

type ConnectRequest struct {
	Driver string `json:"driver"` // e.g. "postgres"
	DSN    string `json:"dsn"`    // connection string
}
