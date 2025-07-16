package handler

import (
	"log"
	"net/http"

	"vind/backend/internal/model"

	"github.com/gin-gonic/gin"
)

func QueryHandler(c *gin.Context) {
	var req model.QueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if activeDB == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No active database connection"})
		return
	}
	log.Println("Executing query:", req.SQL)
	columns, results, err := activeDB.ExecuteQuery(req.SQL)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if columns == nil {
		c.JSON(http.StatusOK, gin.H{"message": "Query executed successfully"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"columns": columns,
		"rows":    results,
	})
}

func TableDataHandler(c *gin.Context) {
	schema := c.DefaultQuery("schema", "public")
	table := c.Query("table")
	limit := c.DefaultQuery("limit", "100")
	offset := c.DefaultQuery("offset", "0")

	if activeDB == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Not connected to any database"})
		return
	}

	if table == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing table name"})
		return
	}

	req := model.TableDataRequest{
		Schema: schema,
		Table:  table,
		Limit:  limit,
		Offset: offset,
	}

	columns, rows, err := activeDB.GetTableData(req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	resp := model.TableDataResponse{
		Columns: columns,
		Rows:    rows,
	}

	c.JSON(http.StatusOK, resp)
}

func InsertRecordHandler(c *gin.Context) {
	var req struct {
		Schema string         `json:"schema"`
		Table  string         `json:"table"`
		Data   map[string]any `json:"data"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON"})
		return
	}

	if activeDB == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No active DB connection"})
		return
	}

	if err := activeDB.InsertRecord(req.Schema, req.Table, req.Data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Record inserted successfully"})
}

func UpdateRecordHandler(c *gin.Context) {
	var req struct {
		Schema string         `json:"schema"`
		Table  string         `json:"table"`
		Data   map[string]any `json:"data"`  // fields to update
		Where  map[string]any `json:"where"` // where clause
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON"})
		return
	}

	if activeDB == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No active DB connection"})
		return
	}

	rowsAffected, err := activeDB.UpdateRecord(req.Schema, req.Table, req.Data, req.Where)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Record(s) updated successfully", "rows_affected": rowsAffected})
}

func DeleteRecordHandler(c *gin.Context) {
	var req struct {
		Schema     string         `json:"schema"`     // optional, default to "public"
		Table      string         `json:"table"`      // required
		Conditions map[string]any `json:"conditions"` // required, e.g., {"id": 1}
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request payload"})
		return
	}

	if req.Table == "" || len(req.Conditions) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing table or conditions"})
		return
	}

	if activeDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "No active database connection"})
		return
	}

	rowsAffected, err := activeDB.DeleteRecord(req.Schema, req.Table, req.Conditions)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Record deleted successfully", "rows_affected": rowsAffected})
}
