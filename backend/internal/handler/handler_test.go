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

// mockDBClient implements service.DBClient for testing
// Only Connect is implemented for this handler's test

type mockDBClient struct {
	connectFunc      func(dsn string) error
	listColumnsFunc  func(schema, table string) ([]model.Column, error)
	executeQueryFunc func(query string) ([]string, [][]any, error)
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
						{Name: "id", Type: "int"},
						{Name: "name", Type: "text"},
					}, nil
				},
			},
			schema:       "public",
			table:        "users",
			expectedCode: http.StatusOK,
			expectedBody: `{"columns":[{"name":"id","type":"int","nullable":false},{"name":"name","type":"text","nullable":false}]}`,
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
