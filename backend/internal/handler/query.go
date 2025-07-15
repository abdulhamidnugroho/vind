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
