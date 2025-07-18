package main

import (
	"os"
	"vind/backend/internal/handler"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load("../.env")
	if err != nil {
		panic("Error loading .env file")
	}

	r := gin.Default()

	r.GET("/ping", handler.Ping)

	r.POST("/connect", handler.ConnectHandler)
	r.GET("/tables", handler.ListTablesHandler)
	r.GET("/columns", handler.ListColumnsHandler)
	r.POST("/query", handler.QueryHandler)
	r.GET("/records", handler.TableDataHandler)
	r.POST("/records", handler.InsertRecordHandler)
	r.PUT("/records", handler.UpdateRecordHandler)
	r.DELETE("/records", handler.DeleteRecordHandler)
	r.POST("/api/schema/tables", handler.CreateTableHandler)

	r.Run(":" + os.Getenv("PORT")) // Default port is set in .env file
}
