package model

type Column struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Nullable   bool   `json:"nullable"`
	Default    string `json:"default"`
	IsUnique   bool   `json:"is_unique"`
	ForeignKey string `json:"foreign_key"`
}
