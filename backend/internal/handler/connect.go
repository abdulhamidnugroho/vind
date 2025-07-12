package handler

import (
	"net/http"

	"vind/backend/internal/model"
	"vind/backend/internal/service"

	"github.com/gin-gonic/gin"
)

var activeDB service.DBClient

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
		activeDB = service.NewPostgresClient()
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
