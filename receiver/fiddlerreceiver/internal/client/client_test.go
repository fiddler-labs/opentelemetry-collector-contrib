// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	testCases := []struct {
		name          string
		opts          []Option
		expectedError string
	}{
		{
			name: "valid configuration",
			opts: []Option{
				WithEndpoint("https://example.com"),
				WithToken("test-token"),
				WithTimeout(10 * time.Second),
			},
			expectedError: "",
		},
		{
			name: "missing endpoint",
			opts: []Option{
				WithToken("test-token"),
			},
			expectedError: "endpoint must be specified",
		},
		{
			name: "missing token",
			opts: []Option{
				WithEndpoint("https://example.com"),
			},
			expectedError: "token must be specified",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client, err := NewClient(tc.opts...)
			if tc.expectedError != "" {
				assert.Error(t, err)
				if err != nil {
					assert.Contains(t, err.Error(), tc.expectedError)
				}
				assert.Nil(t, client)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, client)
			}
		})
	}
}

func TestListModels(t *testing.T) {
	// Sample response data
	modelsResponse := struct {
		Data struct {
			Items []Model `json:"items"`
		} `json:"data"`
	}{}

	modelsResponse.Data.Items = []Model{
		{
			ID:   "model1",
			Name: "Model 1",
			Project: Project{
				ID:   "project1",
				Name: "Project 1",
			},
		},
		{
			ID:   "model2",
			Name: "Model 2",
			Project: Project{
				ID:   "project2",
				Name: "Project 2",
			},
		},
	}

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check request
		assert.Equal(t, "/v3/models", r.URL.Path)
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))

		// Send response
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(modelsResponse)
		assert.NoError(t, err)
	}))
	defer server.Close()

	// Create client
	client, err := NewClient(
		WithEndpoint(server.URL),
		WithToken("test-token"),
	)
	require.NoError(t, err)
	require.NotNil(t, client)

	// Call the API
	models, err := client.ListModels(context.Background())
	require.NoError(t, err)
	require.Len(t, models, 2)

	// Verify the models
	assert.Equal(t, "model1", models[0].ID)
	assert.Equal(t, "Model 1", models[0].Name)
	assert.Equal(t, "project1", models[0].Project.ID)
	assert.Equal(t, "Model 2", models[1].Name)
}

func TestListModelsError(t *testing.T) {
	// Create test server that returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error": "Unauthorized"}`))
	}))
	defer server.Close()

	// Create client
	client, err := NewClient(
		WithEndpoint(server.URL),
		WithToken("test-token"),
	)
	require.NoError(t, err)
	require.NotNil(t, client)

	// Call the API
	models, err := client.ListModels(context.Background())
	assert.Error(t, err)
	assert.Nil(t, models)
	assert.Contains(t, err.Error(), "401")
}

func TestGetMetrics(t *testing.T) {
	// Sample response data
	metricsResponse := struct {
		Data struct {
			Metrics []Metric `json:"metrics"`
			Columns []Column `json:"columns"`
		} `json:"data"`
	}{}

	metricsResponse.Data.Metrics = []Metric{
		{
			ID:               "traffic",
			Type:             "traffic",
			Columns:          []string{"timestamp", "count"},
			RequiresBaseline: false,
		},
		{
			ID:               "drift",
			Type:             "drift",
			Columns:          []string{"timestamp", "drift_score"},
			RequiresBaseline: true,
		},
	}

	metricsResponse.Data.Columns = []Column{
		{
			ID:    "feature1",
			Group: "Inputs",
		},
		{
			ID:    "output1",
			Group: "Outputs",
		},
	}

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check request
		assert.Equal(t, "/v3/models/model1/metrics", r.URL.Path)
		assert.Equal(t, "GET", r.Method)

		// Send response
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(metricsResponse)
		assert.NoError(t, err)
	}))
	defer server.Close()

	// Create client
	client, err := NewClient(
		WithEndpoint(server.URL),
		WithToken("test-token"),
	)
	require.NoError(t, err)
	require.NotNil(t, client)

	// Call the API
	metrics, outputs, err := client.GetMetrics(context.Background(), "model1")
	require.NoError(t, err)
	require.Len(t, metrics, 2)
	require.Len(t, outputs, 1)

	// Verify the metrics
	assert.Equal(t, "traffic", metrics[0].ID)
	assert.False(t, metrics[0].RequiresBaseline)
	assert.Equal(t, "drift", metrics[1].ID)
	assert.True(t, metrics[1].RequiresBaseline)

	// Verify outputs
	assert.Equal(t, "output1", outputs[0])
}

func TestGetBaseline(t *testing.T) {
	// Sample response data
	baselineResponse := struct {
		Data struct {
			Items []Baseline `json:"items"`
		} `json:"data"`
	}{}

	baselineResponse.Data.Items = []Baseline{
		{
			ID:   "baseline1",
			Name: "default_static_baseline",
		},
		{
			ID:   "baseline2",
			Name: "another_baseline",
		},
	}

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check request
		assert.Equal(t, "/v3/models/model1/baselines", r.URL.Path)
		assert.Equal(t, "GET", r.Method)

		// Send response
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(baselineResponse)
		assert.NoError(t, err)
	}))
	defer server.Close()

	// Create client
	client, err := NewClient(
		WithEndpoint(server.URL),
		WithToken("test-token"),
	)
	require.NoError(t, err)
	require.NotNil(t, client)

	// Test finding a specific baseline
	baselineID, err := client.GetBaseline(context.Background(), "model1", "default_static_baseline")
	require.NoError(t, err)
	assert.Equal(t, "baseline1", baselineID)

	// Test with a non-existent baseline (should return the first one)
	baselineID, err = client.GetBaseline(context.Background(), "model1", "nonexistent")
	require.NoError(t, err)
	assert.Equal(t, "baseline1", baselineID)
}

func TestRunQuery(t *testing.T) {
	// Sample response data
	queryResponse := QueryResponse{}
	queryResponse.Data.Project = Project{ID: "project1", Name: "Project 1"}
	queryResponse.Data.Results = map[string]QueryResult{
		"query1": {
			Model: Model{
				ID:   "model1",
				Name: "Model 1",
				Project: Project{
					ID:   "project1",
					Name: "Project 1",
				},
			},
			Metric:   "traffic",
			ColNames: []string{"timestamp", "count"},
			Data:     [][]interface{}{{1622505600000.0, 42.0}},
		},
	}

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check request
		assert.Equal(t, "/v3/queries", r.URL.Path)
		assert.Equal(t, "POST", r.Method)

		// Read and validate request
		var req QueryRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		assert.NoError(t, err)
		assert.Equal(t, "project1", req.ProjectID)
		assert.Equal(t, "MONITORING", req.QueryType)

		// Send response
		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(queryResponse)
		assert.NoError(t, err)
	}))
	defer server.Close()

	// Create client
	client, err := NewClient(
		WithEndpoint(server.URL),
		WithToken("test-token"),
	)
	require.NoError(t, err)
	require.NotNil(t, client)

	// Create query request
	request := QueryRequest{}
	request.ProjectID = "project1"
	request.QueryType = "MONITORING"
	request.Filters.BinSize = "Hour"
	request.Filters.TimeRange.StartTime = "2021-06-01 00:00:00"
	request.Filters.TimeRange.EndTime = "2021-06-01 01:00:00"
	request.Filters.TimeZone = "UTC"
	request.Queries = []Query{
		{
			QueryKey:   "query1",
			Categories: []string{},
			Columns:    []string{"timestamp", "count"},
			VizType:    "line",
			Metric:     "traffic",
			MetricType: "traffic",
			ModelID:    "model1",
			BaselineID: "",
		},
	}

	// Call the API
	resp, err := client.RunQuery(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify the response
	assert.Equal(t, "Project 1", resp.Data.Project.Name)
	assert.Len(t, resp.Data.Results, 1)
	assert.Contains(t, resp.Data.Results, "query1")
	assert.Equal(t, "Model 1", resp.Data.Results["query1"].Model.Name)
	assert.Equal(t, "traffic", resp.Data.Results["query1"].Metric)
	assert.Equal(t, 42.0, resp.Data.Results["query1"].Data[0][1])
}
