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
	connectFunc         func(dsn string) error
	listColumnsFunc     func(schema, table string) ([]model.Column, error)
	executeQueryFunc    func(query string) ([]string, [][]any, error)
	getTableDataFunc    func(model.TableDataRequest) ([]string, [][]any, error)
	insertRecordFunc    func(schema, table string, data map[string]any) error
	updateRecordFunc    func(schema, table string, data, where map[string]any) (int64, error)
	deleteRecordFunc    func(schema, table string, conditions map[string]any) (int64, error)
	createTableFunc     func(tableName string, columns []model.ColumnDef) error
	alterTableFunc      func(tableName string, ops []model.AlterTableOperation) error
	dropTableFunc       func(tableName string, cascade bool) error
	addConstraintFunc   func(params model.AddConstraintParams) error
	dropConstraintFunc  func(tableName, constraintName string, cascade bool) error
	listConstraintsFunc func(tableName string) ([]model.ConstraintInfo, error)
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
func (m *mockDBClient) AddConstraint(params model.AddConstraintParams) error {
	if m.addConstraintFunc != nil {
		return m.addConstraintFunc(params)
	}
	return nil
}
func (m *mockDBClient) DropConstraint(tableName, constraintName string, cascade bool) error {
	if m.dropConstraintFunc != nil {
		return m.dropConstraintFunc(tableName, constraintName, cascade)
	}
	return nil
}
func (m *mockDBClient) ListConstraints(tableName string) ([]model.ConstraintInfo, error) {
	if m.listConstraintsFunc != nil {
		return m.listConstraintsFunc(tableName)
	}
	return nil, nil
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

func TestAddConstraintHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name              string
		activeDB          service.DBClient
		body              string
		addConstraintFunc func(params model.AddConstraintParams) error
		expectedCode      int
		expectedBody      string
	}{
		{
			name:         "invalid json",
			activeDB:     &mockDBClient{},
			body:         `{"invalid json"}`,
			expectedCode: http.StatusBadRequest,
			expectedBody: "invalid character",
		},
		{
			name: "success primary key constraint",
			activeDB: &mockDBClient{
				addConstraintFunc: func(params model.AddConstraintParams) error {
					assert.Equal(t, "users", params.TableName)
					assert.Equal(t, "pk_users", params.ConstraintName)
					assert.Equal(t, "PRIMARY KEY", params.Type)
					assert.Equal(t, []string{"id"}, params.Columns)
					return nil
				},
			},
			body:         `{"table_name":"users","constraint_name":"pk_users","type":"PRIMARY KEY","columns":["id"]}`,
			expectedCode: http.StatusCreated,
			expectedBody: `{"message":"constraint added successfully","constraint":"pk_users"}`,
		},
		{
			name: "success foreign key constraint",
			activeDB: &mockDBClient{
				addConstraintFunc: func(params model.AddConstraintParams) error {
					assert.Equal(t, "orders", params.TableName)
					assert.Equal(t, "fk_user_id", params.ConstraintName)
					assert.Equal(t, "FOREIGN KEY", params.Type)
					assert.Equal(t, []string{"user_id"}, params.Columns)
					assert.Equal(t, "users", params.RefTable)
					assert.Equal(t, []string{"id"}, params.RefColumns)
					assert.Equal(t, "CASCADE", params.OnDelete)
					return nil
				},
			},
			body:         `{"table_name":"orders","constraint_name":"fk_user_id","type":"FOREIGN KEY","columns":["user_id"],"ref_table":"users","ref_columns":["id"],"on_delete":"CASCADE"}`,
			expectedCode: http.StatusCreated,
			expectedBody: `{"message":"constraint added successfully","constraint":"fk_user_id"}`,
		},
		{
			name: "success check constraint",
			activeDB: &mockDBClient{
				addConstraintFunc: func(params model.AddConstraintParams) error {
					assert.Equal(t, "products", params.TableName)
					assert.Equal(t, "chk_price", params.ConstraintName)
					assert.Equal(t, "CHECK", params.Type)
					assert.Equal(t, "price > 0", params.CheckExpr)
					return nil
				},
			},
			body:         `{"table_name":"products","constraint_name":"chk_price","type":"CHECK","check_expr":"price > 0"}`,
			expectedCode: http.StatusCreated,
			expectedBody: `{"message":"constraint added successfully","constraint":"chk_price"}`,
		},
		{
			name: "db error",
			activeDB: &mockDBClient{
				addConstraintFunc: func(params model.AddConstraintParams) error {
					return errors.New("constraint already exists")
				},
			},
			body:         `{"table_name":"users","constraint_name":"pk_users","type":"PRIMARY KEY","columns":["id"]}`,
			expectedCode: http.StatusInternalServerError,
			expectedBody: "constraint already exists",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			activeDB = tc.activeDB
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request, _ = http.NewRequest("POST", "/api/schema/constraints", bytes.NewBufferString(tc.body))
			c.Request.Header.Set("Content-Type", "application/json")

			// Patch AddConstraint if needed
			if m, ok := tc.activeDB.(*mockDBClient); ok && tc.addConstraintFunc != nil {
				m.addConstraintFunc = tc.addConstraintFunc
			}

			AddConstraintHandler(c)

			assert.Equal(t, tc.expectedCode, w.Code)
			if tc.expectedCode == http.StatusCreated {
				assert.JSONEq(t, tc.expectedBody, w.Body.String())
			} else {
				assert.Contains(t, w.Body.String(), tc.expectedBody)
			}
		})
	}
}

func TestDropConstraintHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name               string
		activeDB           service.DBClient
		tableName          string
		constraintName     string
		cascadeQuery       string
		dropConstraintFunc func(tableName, constraintName string, cascade bool) error
		expectedCode       int
		expectedBody       string
	}{
		{
			name: "success without cascade",
			activeDB: &mockDBClient{
				dropConstraintFunc: func(tableName, constraintName string, cascade bool) error {
					assert.Equal(t, "users", tableName)
					assert.Equal(t, "pk_users", constraintName)
					assert.False(t, cascade)
					return nil
				},
			},
			tableName:      "users",
			constraintName: "pk_users",
			expectedCode:   http.StatusOK,
			expectedBody:   `{"message":"constraint dropped successfully","constraint":"pk_users"}`,
		},
		{
			name: "success with cascade=true",
			activeDB: &mockDBClient{
				dropConstraintFunc: func(tableName, constraintName string, cascade bool) error {
					assert.Equal(t, "orders", tableName)
					assert.Equal(t, "fk_user_id", constraintName)
					assert.True(t, cascade)
					return nil
				},
			},
			tableName:      "orders",
			constraintName: "fk_user_id",
			cascadeQuery:   "true",
			expectedCode:   http.StatusOK,
			expectedBody:   `{"message":"constraint dropped successfully","constraint":"fk_user_id"}`,
		},
		{
			name: "success with cascade=false",
			activeDB: &mockDBClient{
				dropConstraintFunc: func(tableName, constraintName string, cascade bool) error {
					assert.Equal(t, "products", tableName)
					assert.Equal(t, "chk_price", constraintName)
					assert.False(t, cascade)
					return nil
				},
			},
			tableName:      "products",
			constraintName: "chk_price",
			cascadeQuery:   "false",
			expectedCode:   http.StatusOK,
			expectedBody:   `{"message":"constraint dropped successfully","constraint":"chk_price"}`,
		},
		{
			name: "invalid cascade parameter ignored",
			activeDB: &mockDBClient{
				dropConstraintFunc: func(tableName, constraintName string, cascade bool) error {
					assert.Equal(t, "users", tableName)
					assert.Equal(t, "unique_email", constraintName)
					assert.False(t, cascade) // Should default to false for invalid values
					return nil
				},
			},
			tableName:      "users",
			constraintName: "unique_email",
			cascadeQuery:   "invalid",
			expectedCode:   http.StatusOK,
			expectedBody:   `{"message":"constraint dropped successfully","constraint":"unique_email"}`,
		},
		{
			name: "db error",
			activeDB: &mockDBClient{
				dropConstraintFunc: func(tableName, constraintName string, cascade bool) error {
					return errors.New("constraint does not exist")
				},
			},
			tableName:      "nonexistent",
			constraintName: "fake_constraint",
			expectedCode:   http.StatusInternalServerError,
			expectedBody:   "constraint does not exist",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			activeDB = tc.activeDB
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)

			// Build URL with query parameter if provided
			url := "/api/schema/constraints/" + tc.tableName + "/" + tc.constraintName
			if tc.cascadeQuery != "" {
				url += "?cascade=" + tc.cascadeQuery
			}

			c.Request, _ = http.NewRequest("DELETE", url, nil)
			c.Params = gin.Params{
				{Key: "table_name", Value: tc.tableName},
				{Key: "constraint_name", Value: tc.constraintName},
			}

			// Set up query parameters
			if tc.cascadeQuery != "" {
				c.Request.URL.RawQuery = "cascade=" + tc.cascadeQuery
			}

			// Patch DropConstraint if needed
			if m, ok := tc.activeDB.(*mockDBClient); ok && tc.dropConstraintFunc != nil {
				m.dropConstraintFunc = tc.dropConstraintFunc
			}

			DropConstraintHandler(c)

			assert.Equal(t, tc.expectedCode, w.Code)
			if tc.expectedCode == http.StatusOK {
				assert.JSONEq(t, tc.expectedBody, w.Body.String())
			} else {
				assert.Contains(t, w.Body.String(), tc.expectedBody)
			}
		})
	}
}

func TestListConstraintsHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name                string
		activeDB            service.DBClient
		tableName           string
		listConstraintsFunc func(tableName string) ([]model.ConstraintInfo, error)
		expectedCode        int
		expectedBody        string
	}{
		{
			name: "success with constraints",
			activeDB: &mockDBClient{
				listConstraintsFunc: func(tableName string) ([]model.ConstraintInfo, error) {
					assert.Equal(t, "users", tableName)
					return []model.ConstraintInfo{
						{
							ConstraintName: "pk_users",
							ConstraintType: "p",
							TableName:      "users",
							Definition:     "PRIMARY KEY (id)",
						},
						{
							ConstraintName: "unique_email",
							ConstraintType: "u",
							TableName:      "users",
							Definition:     "UNIQUE (email)",
						},
					}, nil
				},
			},
			tableName:    "users",
			expectedCode: http.StatusOK,
			expectedBody: `{"constraints":[{"constraint_name":"pk_users","constraint_type":"p","table_name":"users","definition":"PRIMARY KEY (id)"},{"constraint_name":"unique_email","constraint_type":"u","table_name":"users","definition":"UNIQUE (email)"}]}`,
		},
		{
			name: "success with no constraints",
			activeDB: &mockDBClient{
				listConstraintsFunc: func(tableName string) ([]model.ConstraintInfo, error) {
					assert.Equal(t, "empty_table", tableName)
					return []model.ConstraintInfo{}, nil
				},
			},
			tableName:    "empty_table",
			expectedCode: http.StatusOK,
			expectedBody: `{"constraints":[]}`,
		},
		{
			name: "success with nil constraints",
			activeDB: &mockDBClient{
				listConstraintsFunc: func(tableName string) ([]model.ConstraintInfo, error) {
					assert.Equal(t, "another_table", tableName)
					return nil, nil
				},
			},
			tableName:    "another_table",
			expectedCode: http.StatusOK,
			expectedBody: `{"constraints":null}`,
		},
		{
			name: "db error",
			activeDB: &mockDBClient{
				listConstraintsFunc: func(tableName string) ([]model.ConstraintInfo, error) {
					return nil, errors.New("table does not exist")
				},
			},
			tableName:    "nonexistent",
			expectedCode: http.StatusInternalServerError,
			expectedBody: "table does not exist",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			activeDB = tc.activeDB
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request, _ = http.NewRequest("GET", "/api/schema/"+tc.tableName+"/constraints", nil)
			c.Params = gin.Params{
				{Key: "table_name", Value: tc.tableName},
			}

			// Patch ListConstraints if needed
			if m, ok := tc.activeDB.(*mockDBClient); ok && tc.listConstraintsFunc != nil {
				m.listConstraintsFunc = tc.listConstraintsFunc
			}

			ListConstraintsHandler(c)

			assert.Equal(t, tc.expectedCode, w.Code)
			if tc.expectedCode == http.StatusOK {
				assert.JSONEq(t, tc.expectedBody, w.Body.String())
			} else {
				assert.Contains(t, w.Body.String(), tc.expectedBody)
			}
		})
	}
}
