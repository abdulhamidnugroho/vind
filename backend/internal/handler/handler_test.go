package handler

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"vind/backend/internal/model"
	"vind/backend/internal/service"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

type mockDBClient struct {
	connectFunc      func(dsn string) error
	listColumnsFunc  func(schema, table string) ([]model.Column, error)
	executeQueryFunc func(query string) ([]string, [][]any, error)
	getTableDataFunc func(model.TableDataRequest) ([]string, [][]any, error)
	insertRecordFunc func(schema, table string, data map[string]any) error
	updateRecordFunc func(schema, table string, data, where map[string]any) (int64, error)
	deleteRecordFunc func(schema, table string, conditions map[string]any) (int64, error)
	createTableFunc  func(tableName string, columns []model.ColumnDef) error
	alterTableFunc   func(tableName string, ops []model.AlterTableOperation) error
	dropTableFunc    func(tableName string, cascade bool) error
}

func (m *mockDBClient) Connect(dsn string) error {
	return m.connectFunc(dsn)
}
func (m *mockDBClient) Disconnect() error                          { return nil }
func (m *mockDBClient) ListSchemas() ([]string, error)             { return nil, nil }
func (m *mockDBClient) ListTables(schema string) ([]string, error) { return nil, nil }
func (m *mockDBClient) ListColumns(schema, table string) ([]model.Column, error) {
	if m.listColumnsFunc != nil {
		return m.listColumnsFunc(schema, table)
	}
	return nil, nil
}
func (m *mockDBClient) ExecuteQuery(query string) ([]string, [][]any, error) {
	if m.executeQueryFunc != nil {
		return m.executeQueryFunc(query)
	}
	return nil, nil, nil
}
func (m *mockDBClient) GetTableData(req model.TableDataRequest) ([]string, [][]any, error) {
	if m.getTableDataFunc != nil {
		return m.getTableDataFunc(req)
	}
	return nil, nil, nil
}
func (m *mockDBClient) InsertRecord(schema, table string, data map[string]any) error {
	if m.insertRecordFunc != nil {
		return m.insertRecordFunc(schema, table, data)
	}
	return nil
}
func (m *mockDBClient) UpdateRecord(schema, table string, data, where map[string]any) (int64, error) {
	if m.updateRecordFunc != nil {
		return m.updateRecordFunc(schema, table, data, where)
	}
	return 0, nil
}
func (m *mockDBClient) DeleteRecord(schema, table string, conditions map[string]any) (int64, error) {
	if m.deleteRecordFunc != nil {
		return m.deleteRecordFunc(schema, table, conditions)
	}
	return 0, nil
}
func (m *mockDBClient) CreateTable(tableName string, columns []model.ColumnDef) error {
	if m.createTableFunc != nil {
		return m.createTableFunc(tableName, columns)
	}
	return nil
}
func (m *mockDBClient) AlterTable(tableName string, ops []model.AlterTableOperation) error {
	if m.alterTableFunc != nil {
		return m.alterTableFunc(tableName, ops)
	}
	return nil
}
func (m *mockDBClient) DropTable(tableName string, cascade bool) error {
	if m.dropTableFunc != nil {
		return m.dropTableFunc(tableName, cascade)
	}
	return nil
}

type listTablesMock struct {
	mockDBClient
	listTablesFunc func(schema string) ([]string, error)
}

// Override ListTables to use the injected func
func (m *listTablesMock) ListTables(schema string) ([]string, error) {
	return m.listTablesFunc(schema)
}

func TestConnectHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name           string
		body           string
		driver         string
		mockConnectErr error
		expectedCode   int
		expectedBody   string
	}{
		{
			name:         "invalid json",
			body:         `{"driver": "postgres"`, // malformed
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":"Invalid request"}`,
		},
		{
			name:         "unsupported driver",
			body:         `{"driver": "mysql", "dsn": "abc"}`,
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":"Unsupported driver"}`,
		},
		{
			name:           "connect error",
			body:           `{"driver": "postgres", "dsn": "bad"}`,
			driver:         "postgres",
			mockConnectErr: errors.New("fail"),
			expectedCode:   http.StatusInternalServerError,
			expectedBody:   `{"error":"Failed to connect: fail"}`,
		},
		{
			name:           "success",
			body:           `{"driver": "postgres", "dsn": "good"}`,
			driver:         "postgres",
			mockConnectErr: nil,
			expectedCode:   http.StatusOK,
			expectedBody:   `{"message":"Connected successfully"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Patch newPostgresClient for postgres case
			if tc.driver == "postgres" {
				newPostgresClient = func() service.DBClient {
					return &mockDBClient{connectFunc: func(dsn string) error {
						return tc.mockConnectErr
					}}
				}
			} else {
				newPostgresClient = func() service.DBClient { return nil }
			}

			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request, _ = http.NewRequest("POST", "/connect", bytes.NewBufferString(tc.body))
			c.Request.Header.Set("Content-Type", "application/json")

			ConnectHandler(c)

			assert.Equal(t, tc.expectedCode, w.Code)
			assert.Contains(t, w.Body.String(), tc.expectedBody)
		})
	}
}

func TestListTablesHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name           string
		activeDB       service.DBClient
		schema         string
		listTablesFunc func(schema string) ([]string, error)
		expectedCode   int
		expectedBody   string
	}{
		{
			name:         "no active db",
			activeDB:     nil,
			schema:       "",
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":"No active DB connection"}`,
		},
		{
			name: "list tables error",
			activeDB: &listTablesMock{listTablesFunc: func(schema string) ([]string, error) {
				return nil, errors.New("fail")
			}},
			schema:       "myschema",
			expectedCode: http.StatusInternalServerError,
			expectedBody: `{"error":"fail"}`,
		},
		{
			name: "tables is nil",
			activeDB: &listTablesMock{listTablesFunc: func(schema string) ([]string, error) {
				return nil, nil
			}},
			schema:       "",
			expectedCode: http.StatusOK,
			expectedBody: `{"tables":[]}`,
		},
		{
			name: "tables list",
			activeDB: &listTablesMock{listTablesFunc: func(schema string) ([]string, error) {
				return []string{"foo", "bar"}, nil
			}},
			schema:       "public",
			expectedCode: http.StatusOK,
			expectedBody: `{"tables":["foo","bar"]}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Patch global activeDB
			activeDB = tc.activeDB
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			url := "/tables"
			if tc.schema != "" {
				url += "?schema=" + tc.schema
			}
			c.Request, _ = http.NewRequest("GET", url, nil)

			ListTablesHandler(c)

			assert.Equal(t, tc.expectedCode, w.Code)
			assert.Contains(t, w.Body.String(), tc.expectedBody)
		})
	}
}

func TestQueryHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name             string
		activeDB         service.DBClient
		body             string
		executeQueryFunc func(query string) ([]string, [][]any, error)
		expectedCode     int
		expectedBody     string
	}{
		{
			name:         "invalid json",
			activeDB:     &mockDBClient{},
			body:         `{"sql": `, // malformed
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":"Invalid request"}`,
		},
		{
			name:         "no active db",
			activeDB:     nil,
			body:         `{"sql": "SELECT 1"}`,
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":"No active database connection"}`,
		},
		{
			name: "query error",
			activeDB: &mockDBClient{
				executeQueryFunc: func(query string) ([]string, [][]any, error) {
					return nil, nil, errors.New("fail query")
				},
			},
			body:         `{"sql": "SELECT * FROM foo"}`,
			expectedCode: http.StatusInternalServerError,
			expectedBody: `{"error":"fail query"}`,
		},
		{
			name: "success",
			activeDB: &mockDBClient{
				executeQueryFunc: func(query string) ([]string, [][]any, error) {
					return []string{"id", "name"}, [][]any{{1, "Alice"}, {2, "Bob"}}, nil
				},
			},
			body:         `{"sql": "SELECT id, name FROM users"}`,
			expectedCode: http.StatusOK,
			expectedBody: `{"columns":["id","name"],"rows":[[1,"Alice"],[2,"Bob"]]}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			activeDB = tc.activeDB
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request, _ = http.NewRequest("POST", "/query", bytes.NewBufferString(tc.body))
			c.Request.Header.Set("Content-Type", "application/json")

			// Patch ExecuteQuery if needed
			if m, ok := tc.activeDB.(*mockDBClient); ok && tc.executeQueryFunc != nil {
				m.executeQueryFunc = tc.executeQueryFunc
			}

			QueryHandler(c)

			assert.Equal(t, tc.expectedCode, w.Code)
			assert.Contains(t, w.Body.String(), tc.expectedBody)
		})
	}
}

func TestListColumnsHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name            string
		activeDB        service.DBClient
		schema          string
		table           string
		listColumnsFunc func(schema, table string) ([]model.Column, error)
		expectedCode    int
		expectedBody    string
	}{
		{
			name:         "no active db",
			activeDB:     nil,
			table:        "users",
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":"Not connected to any database"}`,
		},
		{
			name:         "missing table param",
			activeDB:     &mockDBClient{},
			table:        "",
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":"Missing 'table' query parameter"}`,
		},
		{
			name: "db error",
			activeDB: &mockDBClient{
				listColumnsFunc: func(schema, table string) ([]model.Column, error) {
					return nil, errors.New("fail")
				},
			},
			table:        "users",
			expectedCode: http.StatusInternalServerError,
			expectedBody: `{"error":"Failed to fetch columns: fail"}`,
		},
		{
			name: "success",
			activeDB: &mockDBClient{
				listColumnsFunc: func(schema, table string) ([]model.Column, error) {
					return []model.Column{
						{Name: "id", Type: "int", Nullable: false, Default: "", IsUnique: false, ForeignKey: ""},
						{Name: "name", Type: "text", Nullable: false, Default: "", IsUnique: false, ForeignKey: ""},
					}, nil
				},
			},
			schema:       "public",
			table:        "users",
			expectedCode: http.StatusOK,
			expectedBody: `{"columns":[{"name":"id","type":"int","nullable":false,"default":"","is_unique":false,"foreign_key":""},{"name":"name","type":"text","nullable":false,"default":"","is_unique":false,"foreign_key":""}]}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			activeDB = tc.activeDB
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			url := "/columns?table=" + tc.table
			if tc.schema != "" {
				url += "&schema=" + tc.schema
			}
			c.Request, _ = http.NewRequest("GET", url, nil)

			// Patch ListColumns if needed
			if m, ok := tc.activeDB.(*mockDBClient); ok && tc.listColumnsFunc != nil {
				m.listColumnsFunc = tc.listColumnsFunc
			}

			ListColumnsHandler(c)

			assert.Equal(t, tc.expectedCode, w.Code)
			assert.Contains(t, w.Body.String(), tc.expectedBody)
		})
	}
}

func TestTableDataHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name           string
		activeDB       service.DBClient
		queryParams    string
		getTableDataFn func(model.TableDataRequest) ([]string, [][]any, error)
		expectedCode   int
		expectedBody   string
	}{
		{
			name:         "no active db",
			activeDB:     nil,
			queryParams:  "schema=public&table=users",
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":"Not connected to any database"}`,
		},
		{
			name:         "missing table param",
			activeDB:     &mockDBClient{},
			queryParams:  "schema=public",
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":"Missing table name"}`,
		},
		{
			name: "db error",
			activeDB: &mockDBClient{
				getTableDataFunc: func(req model.TableDataRequest) ([]string, [][]any, error) {
					return nil, nil, errors.New("fail db")
				},
			},
			queryParams:  "schema=public&table=users",
			expectedCode: http.StatusInternalServerError,
			expectedBody: `{"error":"fail db"}`,
		},
		{
			name: "success",
			activeDB: &mockDBClient{
				getTableDataFunc: func(req model.TableDataRequest) ([]string, [][]any, error) {
					return []string{"id", "name"}, [][]any{{1, "Alice"}, {2, "Bob"}}, nil
				},
			},
			queryParams:  "schema=public&table=users&limit=2&offset=0",
			expectedCode: http.StatusOK,
			expectedBody: `{"columns":["id","name"],"rows":[[1,"Alice"],[2,"Bob"]]}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			activeDB = tc.activeDB
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			url := "/tabledata?" + tc.queryParams
			c.Request, _ = http.NewRequest("GET", url, nil)

			// Patch GetTableData if needed
			if m, ok := tc.activeDB.(*mockDBClient); ok && tc.getTableDataFn != nil {
				m.getTableDataFunc = tc.getTableDataFn
			}

			TableDataHandler(c)

			assert.Equal(t, tc.expectedCode, w.Code)
			assert.Contains(t, w.Body.String(), tc.expectedBody)
		})
	}
}

func TestInsertRecordHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name             string
		activeDB         service.DBClient
		body             string
		insertRecordFunc func(schema, table string, data map[string]any) error
		expectedCode     int
		expectedBody     string
	}{
		{
			name:         "invalid json",
			activeDB:     &mockDBClient{},
			body:         `{"schema": "public", "table": "users", "data": `, // malformed
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":"Invalid JSON"}`,
		},
		{
			name:         "no active db",
			activeDB:     nil,
			body:         `{"schema": "public", "table": "users", "data": {"name": "Abdul"}}`,
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":"No active DB connection"}`,
		},
		{
			name: "db error",
			activeDB: &mockDBClient{
				insertRecordFunc: func(schema, table string, data map[string]any) error {
					return errors.New("fail insert")
				},
			},
			body:         `{"schema": "public", "table": "users", "data": {"name": "Abdul"}}`,
			expectedCode: http.StatusInternalServerError,
			expectedBody: `{"error":"fail insert"}`,
		},
		{
			name: "success",
			activeDB: &mockDBClient{
				insertRecordFunc: func(schema, table string, data map[string]any) error {
					return nil
				},
			},
			body:         `{"schema": "public", "table": "users", "data": {"name": "Abdul"}}`,
			expectedCode: http.StatusOK,
			expectedBody: `{"message":"Record inserted successfully"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			activeDB = tc.activeDB
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request, _ = http.NewRequest("POST", "/records", bytes.NewBufferString(tc.body))
			c.Request.Header.Set("Content-Type", "application/json")

			// Patch InsertRecord if needed
			if m, ok := tc.activeDB.(*mockDBClient); ok && tc.insertRecordFunc != nil {
				m.insertRecordFunc = tc.insertRecordFunc
			}

			InsertRecordHandler(c)

			assert.Equal(t, tc.expectedCode, w.Code)
			assert.Contains(t, w.Body.String(), tc.expectedBody)
		})
	}
}

func TestUpdateRecordHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name             string
		activeDB         service.DBClient
		body             string
		updateRecordFunc func(schema, table string, data, where map[string]any) (int64, error)
		expectedCode     int
		expectedBody     string
	}{
		{
			name:         "invalid json",
			activeDB:     &mockDBClient{},
			body:         `{"schema": "public", "table": "users", "data": {"email": "updated@example.com"}, "where": `, // malformed
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":"Invalid JSON"}`,
		},
		{
			name:         "no active db",
			activeDB:     nil,
			body:         `{"schema": "public", "table": "users", "data": {"email": "updated@example.com"}, "where": {"id": 1}}`,
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":"No active DB connection"}`,
		},
		{
			name: "db error",
			activeDB: &mockDBClient{
				updateRecordFunc: func(schema, table string, data, where map[string]any) (int64, error) {
					return 0, errors.New("fail update")
				},
			},
			body:         `{"schema": "public", "table": "users", "data": {"email": "updated@example.com"}, "where": {"id": 1}}`,
			expectedCode: http.StatusInternalServerError,
			expectedBody: `{"error":"fail update"}`,
		},
		{
			name: "success",
			activeDB: &mockDBClient{
				updateRecordFunc: func(schema, table string, data, where map[string]any) (int64, error) {
					return 1, nil
				},
			},
			body:         `{"schema": "public", "table": "users", "data": {"email": "updated@example.com"}, "where": {"id": 1}}`,
			expectedCode: http.StatusOK,
			expectedBody: `{"message":"Record(s) updated successfully","rows_affected":1}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			activeDB = tc.activeDB
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request, _ = http.NewRequest("PUT", "/records", bytes.NewBufferString(tc.body))
			c.Request.Header.Set("Content-Type", "application/json")

			// Patch UpdateRecord if needed
			if m, ok := tc.activeDB.(*mockDBClient); ok && tc.updateRecordFunc != nil {
				m.updateRecordFunc = tc.updateRecordFunc
			}

			UpdateRecordHandler(c)

			assert.Equal(t, tc.expectedCode, w.Code)
			if tc.expectedCode == http.StatusOK {
				assert.JSONEq(t, tc.expectedBody, w.Body.String())
			} else {
				assert.Contains(t, w.Body.String(), tc.expectedBody)
			}
		})
	}
}

func TestCreateTableHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name            string
		activeDB        service.DBClient
		body            string
		createTableFunc func(tableName string, columns []model.ColumnDef) error
		expectedCode    int
		expectedBody    string
	}{
		{
			name:         "invalid json",
			activeDB:     &mockDBClient{},
			body:         `{"table_name": "users", "columns": [`, // malformed
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":`, // partial match
		},
		{
			name: "db error",
			activeDB: &mockDBClient{
				createTableFunc: func(tableName string, columns []model.ColumnDef) error {
					return errors.New("fail create table")
				},
			},
			body:         `{"table_name": "users", "columns": [{"name": "id", "type": "int"}]}`,
			expectedCode: http.StatusInternalServerError,
			expectedBody: `{"error":"fail create table"}`,
		},
		{
			name: "success",
			activeDB: &mockDBClient{
				createTableFunc: func(tableName string, columns []model.ColumnDef) error {
					return nil
				},
			},
			body:         `{"table_name": "users", "columns": [{"name": "id", "type": "int"}]}`,
			expectedCode: http.StatusCreated,
			expectedBody: `{"message":"table created successfully","table":"users"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			activeDB = tc.activeDB
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request, _ = http.NewRequest("POST", "/tables", bytes.NewBufferString(tc.body))
			c.Request.Header.Set("Content-Type", "application/json")

			// Patch CreateTable if needed
			if m, ok := tc.activeDB.(*mockDBClient); ok && tc.createTableFunc != nil {
				m.createTableFunc = tc.createTableFunc
			}

			CreateTableHandler(c)

			assert.Equal(t, tc.expectedCode, w.Code)
			if tc.expectedCode == http.StatusCreated {
				assert.JSONEq(t, tc.expectedBody, w.Body.String())
			} else {
				assert.Contains(t, w.Body.String(), tc.expectedBody)
			}
		})
	}
}

func TestDeleteRecordHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name             string
		activeDB         service.DBClient
		body             string
		deleteRecordFunc func(schema, table string, conditions map[string]any) (int64, error)
		expectedCode     int
		expectedBody     string
	}{
		{
			name:         "invalid json",
			activeDB:     &mockDBClient{},
			body:         `{"schema": "public", "table": "users", "conditions": `, // malformed
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":"Invalid request payload"}`,
		},
		{
			name:         "missing table",
			activeDB:     &mockDBClient{},
			body:         `{"schema": "public", "conditions": {"id": 1}}`,
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":"Missing table or conditions"}`,
		},
		{
			name:         "missing conditions",
			activeDB:     &mockDBClient{},
			body:         `{"schema": "public", "table": "users"}`,
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":"Missing table or conditions"}`,
		},
		{
			name:         "no active db",
			activeDB:     nil,
			body:         `{"schema": "public", "table": "users", "conditions": {"id": 1}}`,
			expectedCode: http.StatusServiceUnavailable,
			expectedBody: `{"error":"No active database connection"}`,
		},
		{
			name: "db error",
			activeDB: &mockDBClient{
				deleteRecordFunc: func(schema, table string, conditions map[string]any) (int64, error) {
					return 0, errors.New("fail delete")
				},
			},
			body:         `{"schema": "public", "table": "users", "conditions": {"id": 1}}`,
			expectedCode: http.StatusInternalServerError,
			expectedBody: `{"error":"fail delete"}`,
		},
		{
			name: "success",
			activeDB: &mockDBClient{
				deleteRecordFunc: func(schema, table string, conditions map[string]any) (int64, error) {
					return 1, nil
				},
			},
			body:         `{"schema": "public", "table": "users", "conditions": {"id": 1}}`,
			expectedCode: http.StatusOK,
			expectedBody: `{"message":"Record deleted successfully","rows_affected":1}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			activeDB = tc.activeDB
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request, _ = http.NewRequest("DELETE", "/records", bytes.NewBufferString(tc.body))
			c.Request.Header.Set("Content-Type", "application/json")

			// Patch DeleteRecord if needed
			if m, ok := tc.activeDB.(*mockDBClient); ok && tc.deleteRecordFunc != nil {
				m.deleteRecordFunc = tc.deleteRecordFunc
			}

			DeleteRecordHandler(c)

			assert.Equal(t, tc.expectedCode, w.Code)
			if tc.expectedCode == http.StatusOK {
				assert.JSONEq(t, tc.expectedBody, w.Body.String())
			} else {
				assert.Contains(t, w.Body.String(), tc.expectedBody)
			}
		})
	}
}

func TestAlterTableHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name           string
		activeDB       service.DBClient
		tableName      string
		body           string
		alterTableFunc func(tableName string, ops []model.AlterTableOperation) error
		expectedCode   int
		expectedBody   string
	}{
		{
			name:         "invalid json",
			activeDB:     &mockDBClient{},
			tableName:    "users",
			body:         `{"operations": [{"action": "add_column"`, // malformed
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":`,
		},
		{
			name:         "missing operations",
			activeDB:     &mockDBClient{},
			tableName:    "users",
			body:         `{}`,
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":`,
		},
		{
			name:         "empty operations array",
			activeDB:     &mockDBClient{},
			tableName:    "users",
			body:         `{"operations": []}`,
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":`,
		},
		{
			name:         "invalid operation - missing action",
			activeDB:     &mockDBClient{},
			tableName:    "users",
			body:         `{"operations": [{"column_name": "email", "type": "varchar(255)"}]}`,
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":`,
		},
		{
			name:         "no active db",
			activeDB:     nil,
			tableName:    "users",
			body:         `{"operations": [{"action": "add_column", "column_name": "email", "type": "varchar(255)"}]}`,
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":`,
		},
		{
			name: "db error",
			activeDB: &mockDBClient{
				alterTableFunc: func(tableName string, ops []model.AlterTableOperation) error {
					return errors.New("fail alter table")
				},
			},
			tableName:    "users",
			body:         `{"operations": [{"action": "add_column", "column_name": "email", "type": "varchar(255)"}]}`,
			expectedCode: http.StatusInternalServerError,
			expectedBody: `{"error":"fail alter table"}`,
		},
		{
			name: "success - add column",
			activeDB: &mockDBClient{
				alterTableFunc: func(tableName string, ops []model.AlterTableOperation) error {
					return nil
				},
			},
			tableName:    "users",
			body:         `{"operations": [{"action": "add_column", "column_name": "email", "type": "varchar(255)"}]}`,
			expectedCode: http.StatusOK,
			expectedBody: `{"message":"table altered successfully","table":"users"}`,
		},
		{
			name: "success - drop column",
			activeDB: &mockDBClient{
				alterTableFunc: func(tableName string, ops []model.AlterTableOperation) error {
					return nil
				},
			},
			tableName:    "users",
			body:         `{"operations": [{"action": "drop_column", "column_name": "old_field"}]}`,
			expectedCode: http.StatusOK,
			expectedBody: `{"message":"table altered successfully","table":"users"}`,
		},
		{
			name: "success - rename column",
			activeDB: &mockDBClient{
				alterTableFunc: func(tableName string, ops []model.AlterTableOperation) error {
					return nil
				},
			},
			tableName:    "users",
			body:         `{"operations": [{"action": "rename_column", "column_name": "old_name", "new_name": "new_name"}]}`,
			expectedCode: http.StatusOK,
			expectedBody: `{"message":"table altered successfully","table":"users"}`,
		},
		{
			name: "success - alter column",
			activeDB: &mockDBClient{
				alterTableFunc: func(tableName string, ops []model.AlterTableOperation) error {
					return nil
				},
			},
			tableName:    "users",
			body:         `{"operations": [{"action": "alter_column", "column_name": "age", "type": "bigint", "not_null": true, "default": "0"}]}`,
			expectedCode: http.StatusOK,
			expectedBody: `{"message":"table altered successfully","table":"users"}`,
		},
		{
			name: "success - multiple operations",
			activeDB: &mockDBClient{
				alterTableFunc: func(tableName string, ops []model.AlterTableOperation) error {
					return nil
				},
			},
			tableName:    "users",
			body:         `{"operations": [{"action": "add_column", "column_name": "email", "type": "varchar(255)"}, {"action": "drop_column", "column_name": "old_field"}]}`,
			expectedCode: http.StatusOK,
			expectedBody: `{"message":"table altered successfully","table":"users"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			activeDB = tc.activeDB
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request, _ = http.NewRequest("PUT", "/tables/"+tc.tableName, bytes.NewBufferString(tc.body))
			c.Request.Header.Set("Content-Type", "application/json")
			c.Params = gin.Params{
				{Key: "table_name", Value: tc.tableName},
			}

			// Patch AlterTable if needed
			if m, ok := tc.activeDB.(*mockDBClient); ok && tc.alterTableFunc != nil {
				m.alterTableFunc = tc.alterTableFunc
			}

			AlterTableHandler(c)

			assert.Equal(t, tc.expectedCode, w.Code)
			if tc.expectedCode == http.StatusOK {
				assert.JSONEq(t, tc.expectedBody, w.Body.String())
			} else {
				assert.Contains(t, w.Body.String(), tc.expectedBody)
			}
		})
	}
}

func TestDropTableHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name          string
		activeDB      service.DBClient
		tableName     string
		cascadeQuery  string
		dropTableFunc func(tableName string, cascade bool) error
		expectedCode  int
		expectedBody  string
	}{
		{
			name:         "no active db",
			activeDB:     nil,
			tableName:    "users",
			expectedCode: http.StatusBadRequest,
			expectedBody: `{"error":"No active DB connection"}`,
		},
		{
			name: "success without cascade",
			activeDB: &mockDBClient{
				dropTableFunc: func(tableName string, cascade bool) error {
					assert.Equal(t, "users", tableName)
					assert.False(t, cascade)
					return nil
				},
			},
			tableName:    "users",
			expectedCode: http.StatusOK,
			expectedBody: `{"message":"table dropped successfully","table":"users"}`,
		},
		{
			name: "success with cascade=true",
			activeDB: &mockDBClient{
				dropTableFunc: func(tableName string, cascade bool) error {
					assert.Equal(t, "users", tableName)
					assert.True(t, cascade)
					return nil
				},
			},
			tableName:    "users",
			cascadeQuery: "true",
			expectedCode: http.StatusOK,
			expectedBody: `{"message":"table dropped successfully","table":"users"}`,
		},
		{
			name: "success with cascade=false",
			activeDB: &mockDBClient{
				dropTableFunc: func(tableName string, cascade bool) error {
					assert.Equal(t, "users", tableName)
					assert.False(t, cascade)
					return nil
				},
			},
			tableName:    "users",
			cascadeQuery: "false",
			expectedCode: http.StatusOK,
			expectedBody: `{"message":"table dropped successfully","table":"users"}`,
		},
		{
			name: "invalid cascade parameter ignored",
			activeDB: &mockDBClient{
				dropTableFunc: func(tableName string, cascade bool) error {
					assert.Equal(t, "users", tableName)
					assert.False(t, cascade) // Should default to false for invalid values
					return nil
				},
			},
			tableName:    "users",
			cascadeQuery: "invalid",
			expectedCode: http.StatusOK,
			expectedBody: `{"message":"table dropped successfully","table":"users"}`,
		},
		{
			name: "db error",
			activeDB: &mockDBClient{
				dropTableFunc: func(tableName string, cascade bool) error {
					return errors.New("table does not exist")
				},
			},
			tableName:    "nonexistent",
			expectedCode: http.StatusInternalServerError,
			expectedBody: `{"error":"table does not exist"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			activeDB = tc.activeDB
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)

			// Build URL with query parameter if provided
			url := "/tables/" + tc.tableName
			if tc.cascadeQuery != "" {
				url += "?cascade=" + tc.cascadeQuery
			}

			c.Request, _ = http.NewRequest("DELETE", url, nil)
			c.Params = gin.Params{
				{Key: "table_name", Value: tc.tableName},
			}

			// Set up query parameters
			if tc.cascadeQuery != "" {
				c.Request.URL.RawQuery = "cascade=" + tc.cascadeQuery
			}

			// Patch DropTable if needed
			if m, ok := tc.activeDB.(*mockDBClient); ok && tc.dropTableFunc != nil {
				m.dropTableFunc = tc.dropTableFunc
			}

			DropTableHandler(c)

			assert.Equal(t, tc.expectedCode, w.Code)
			if tc.expectedCode == http.StatusOK {
				assert.JSONEq(t, tc.expectedBody, w.Body.String())
			} else {
				assert.JSONEq(t, tc.expectedBody, w.Body.String())
			}
		})
	}
}
