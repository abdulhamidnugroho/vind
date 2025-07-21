package handler

import (
	"log"
	"net/http"
	"strconv"
	"vind/backend/internal/model"
	"vind/backend/internal/service"

	"github.com/gin-gonic/gin"
)

var activeDB service.DBClient

// newPostgresClient is a function that returns a service.DBClient.
// By default, it returns service.NewPostgresClient(), but can be overridden in tests.
var newPostgresClient func() service.DBClient = func() service.DBClient { return service.NewPostgresClient() }

func Ping(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "pong",
	})
}

func ConnectHandler(c *gin.Context) {
	var req model.ConnectRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	switch req.Driver {
	case "postgres":
		activeDB = newPostgresClient()
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "Unsupported driver"})
		return
	}

	if err := activeDB.Connect(req.DSN); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to connect: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Connected successfully"})
}

func ListTablesHandler(c *gin.Context) {
	if activeDB == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No active DB connection"})
		return
	}

	schema := c.DefaultQuery("schema", "public")

	tables, err := activeDB.ListTables(schema)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if tables == nil {
		tables = []string{}
	}

	c.JSON(http.StatusOK, gin.H{"tables": tables})
}

func ListColumnsHandler(c *gin.Context) {
	if activeDB == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Not connected to any database"})
		return
	}

	schema := c.DefaultQuery("schema", "public")
	table := c.Query("table")
	if table == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing 'table' query parameter"})
		return
	}

	log.Printf("Listing columns for %s.%s\n", schema, table)
	columns, err := activeDB.ListColumns(schema, table)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch columns: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"columns": columns})
}

func CreateTableHandler(c *gin.Context) {
	if activeDB == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No active DB connection"})
		return
	}
	var req model.CreateTableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := activeDB.CreateTable(req.TableName, req.Columns); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message": "table created successfully",
		"table":   req.TableName,
	})
}

func AlterTableHandler(c *gin.Context) {
	tableName := c.Param("table_name")
	var req model.AlterTableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if activeDB == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No active DB connection"})
		return
	}

	if err := activeDB.AlterTable(tableName, req.Operations); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "table altered successfully",
		"table":   tableName,
	})
}

func DropTableHandler(c *gin.Context) {
	if activeDB == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No active DB connection"})
		return
	}

	tableName := c.Param("table_name")
	cascade := false

	// Optional ?cascade=true
	if val := c.Query("cascade"); val != "" {
		if parsed, err := strconv.ParseBool(val); err == nil {
			cascade = parsed
		}
	}

	if err := activeDB.DropTable(tableName, cascade); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "table dropped successfully",
		"table":   tableName,
	})
}
