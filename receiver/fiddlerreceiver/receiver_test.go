// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package fiddlerreceiver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/fiddlerreceiver/internal/client"
)

func TestNewReceiver(t *testing.T) {
	consumer := consumertest.NewNop()
	settings := receivertest.NewNopSettings()
	fr := newFiddlerReceiver(&Config{
		Endpoint: defaultEndpoint,
		Token:    defaultAuthToken,
		Interval: defaultInterval,
		Timeout:  defaultTimeout,
	}, consumer, settings)

	assert.NotNil(t, fr)
	assert.Same(t, consumer, fr.consumer)
}

func TestStartAndShutdown(t *testing.T) {
	// Setup mock server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Return models list on models endpoint
		if r.URL.Path == "/v3/models" {
			modelsResponse := struct {
				Data struct {
					Items []client.Model `json:"items"`
				} `json:"data"`
			}{}
			modelsResponse.Data.Items = []client.Model{
				{
					ID:   "model1",
					Name: "Model 1",
					Project: client.Project{
						ID:   "project1",
						Name: "Project 1",
					},
				},
			}
			json.NewEncoder(w).Encode(modelsResponse)
		}
	}))
	defer ts.Close()

	config := &Config{
		Endpoint: ts.URL,
		Token:    defaultAuthToken,
		Interval: defaultInterval,
		Timeout:  defaultTimeout,
	}
	consumer := consumertest.NewNop()
	settings := receivertest.NewNopSettings()
	fr := newFiddlerReceiver(config, consumer, settings)

	// Test start
	err := fr.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Test shutdown
	err = fr.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestCollect(t *testing.T) {
	// Mock expected input/output data
	// Models data
	models := []client.Model{
		{
			ID:   "model1",
			Name: "Model 1",
			Project: client.Project{
				ID:   "project1",
				Name: "Project 1",
			},
		},
		{
			ID:   "model2",
			Name: "Model 2",
			Project: client.Project{
				ID:   "project1",
				Name: "Project 1",
			},
		},
	}

	// Metrics data per model - using realistic metric types from the example
	modelMetrics := map[string][]client.Metric{
		"model1": {
			// Service metrics
			{
				ID:                 "traffic",
				Type:               "service_metrics",
				Columns:            []string{},
				RequiresBaseline:   false,
				RequiresCategories: false,
			},
			// Drift metrics
			{
				ID:                 "jsd",
				Type:               "drift",
				Columns:            []string{"creditscore", "geography", "gender", "age", "balance"},
				RequiresBaseline:   true,
				RequiresCategories: false,
			},
			{
				ID:                 "psi",
				Type:               "drift",
				Columns:            []string{"creditscore", "geography", "gender", "age", "balance"},
				RequiresBaseline:   true,
				RequiresCategories: false,
			},
			// Data integrity metrics
			{
				ID:                 "range_violation_count",
				Type:               "data_integrity",
				Columns:            []string{"__ANY__", "creditscore", "age", "balance"},
				RequiresBaseline:   false,
				RequiresCategories: false,
			},
			{
				ID:                 "null_violation_count",
				Type:               "data_integrity",
				Columns:            []string{"__ANY__", "creditscore", "geography", "gender", "age"},
				RequiresBaseline:   false,
				RequiresCategories: false,
			},
		},
		"model2": {
			// Performance metrics
			{
				ID:                 "precision",
				Type:               "performance",
				Columns:            []string{},
				RequiresBaseline:   false,
				RequiresCategories: false,
			},
			{
				ID:                 "recall",
				Type:               "performance",
				Columns:            []string{},
				RequiresBaseline:   false,
				RequiresCategories: false,
			},
			{
				ID:                 "accuracy",
				Type:               "performance",
				Columns:            []string{},
				RequiresBaseline:   false,
				RequiresCategories: false,
			},
			// Additional data integrity metric
			{
				ID:                 "type_violation_count",
				Type:               "data_integrity",
				Columns:            []string{"__ANY__", "creditscore", "geography", "gender"},
				RequiresBaseline:   false,
				RequiresCategories: false,
			},
		},
	}

	// Baselines per model
	modelBaselines := map[string][]client.Baseline{
		"model1": {
			{
				ID:   "baseline1",
				Name: "default_static_baseline",
			},
		},
		"model2": {
			{
				ID:   "baseline2",
				Name: "default_static_baseline",
			},
		},
	}

	// Expected queries to be sent (will be validated)
	expectedQueries := map[string][]client.Query{
		"model1": {
			{
				QueryKey:   "traffic",
				Categories: []string{},
				Columns:    []string{},
				VizType:    "line",
				Metric:     "traffic",
				MetricType: "service_metrics",
				ModelID:    "model1",
				BaselineID: "",
			},
			{
				QueryKey:   "jsd",
				Categories: []string{},
				Columns:    []string{"creditscore", "geography", "gender", "age", "balance"},
				VizType:    "line",
				Metric:     "jsd",
				MetricType: "drift",
				ModelID:    "model1",
				BaselineID: "baseline1",
			},
			{
				QueryKey:   "psi",
				Categories: []string{},
				Columns:    []string{"creditscore", "geography", "gender", "age", "balance"},
				VizType:    "line",
				Metric:     "psi",
				MetricType: "drift",
				ModelID:    "model1",
				BaselineID: "baseline1",
			},
			{
				QueryKey:   "range_violation_count",
				Categories: []string{},
				Columns:    []string{"__ANY__", "creditscore", "age", "balance"},
				VizType:    "line",
				Metric:     "range_violation_count",
				MetricType: "data_integrity",
				ModelID:    "model1",
				BaselineID: "",
			},
			{
				QueryKey:   "null_violation_count",
				Categories: []string{},
				Columns:    []string{"__ANY__", "creditscore", "geography", "gender", "age"},
				VizType:    "line",
				Metric:     "null_violation_count",
				MetricType: "data_integrity",
				ModelID:    "model1",
				BaselineID: "",
			},
		},
		"model2": {
			{
				QueryKey:   "precision",
				Categories: []string{},
				Columns:    []string{},
				VizType:    "line",
				Metric:     "precision",
				MetricType: "performance",
				ModelID:    "model2",
				BaselineID: "",
			},
			{
				QueryKey:   "recall",
				Categories: []string{},
				Columns:    []string{},
				VizType:    "line",
				Metric:     "recall",
				MetricType: "performance",
				ModelID:    "model2",
				BaselineID: "",
			},
			{
				QueryKey:   "accuracy",
				Categories: []string{},
				Columns:    []string{},
				VizType:    "line",
				Metric:     "accuracy",
				MetricType: "performance",
				ModelID:    "model2",
				BaselineID: "",
			},
			{
				QueryKey:   "type_violation_count",
				Categories: []string{},
				Columns:    []string{"__ANY__", "creditscore", "geography", "gender"},
				VizType:    "line",
				Metric:     "type_violation_count",
				MetricType: "data_integrity",
				ModelID:    "model2",
				BaselineID: "",
			},
		},
	}

	// Mock query responses with a timestamp in RFC3339 format as expected by the receiver
	timestamp := time.Unix(1622505600, 0).UTC().Format(time.RFC3339)
	queryResponses := map[string]client.QueryResponse{
		"model1": {
			Data: struct {
				Project client.Project                `json:"project"`
				Results map[string]client.QueryResult `json:"results"`
			}{
				Project: client.Project{ID: "project1", Name: "Project 1"},
				Results: map[string]client.QueryResult{
					"traffic": {
						Model:    models[0],
						Metric:   "traffic",
						ColNames: []string{"timestamp", "count"},
						Data:     [][]interface{}{{timestamp, 42.0}},
					},
					"jsd": {
						Model:    models[0],
						Metric:   "jsd",
						ColNames: []string{"timestamp", "jsd,creditscore", "jsd,geography", "jsd,gender", "jsd,age", "jsd,balance"},
						Data: [][]interface{}{
							{timestamp, 0.15, 0.22, 0.18, 0.35, 0.28},
						},
					},
					"psi": {
						Model:    models[0],
						Metric:   "psi",
						ColNames: []string{"timestamp", "psi,creditscore", "psi,geography", "psi,gender", "psi,age", "psi,balance"},
						Data: [][]interface{}{
							{timestamp, 0.17, 0.19, 0.11, 0.26, 0.36},
						},
					},
					"range_violation_count": {
						Model:    models[0],
						Metric:   "range_violation_count",
						ColNames: []string{"timestamp", "count"},
						Data:     [][]interface{}{{timestamp, 15.0}},
					},
					"null_violation_count": {
						Model:    models[0],
						Metric:   "null_violation_count",
						ColNames: []string{"timestamp", "count"},
						Data:     [][]interface{}{{timestamp, 8.0}},
					},
				},
			},
		},
		"model2": {
			Data: struct {
				Project client.Project                `json:"project"`
				Results map[string]client.QueryResult `json:"results"`
			}{
				Project: client.Project{ID: "project1", Name: "Project 1"},
				Results: map[string]client.QueryResult{
					"precision": {
						Model:    models[1],
						Metric:   "precision",
						ColNames: []string{"timestamp", "value"},
						Data:     [][]interface{}{{timestamp, 0.92}},
					},
					"recall": {
						Model:    models[1],
						Metric:   "recall",
						ColNames: []string{"timestamp", "value"},
						Data:     [][]interface{}{{timestamp, 0.88}},
					},
					"accuracy": {
						Model:    models[1],
						Metric:   "accuracy",
						ColNames: []string{"timestamp", "value"},
						Data:     [][]interface{}{{timestamp, 0.90}},
					},
					"type_violation_count": {
						Model:    models[1],
						Metric:   "type_violation_count",
						ColNames: []string{"timestamp", "count"},
						Data:     [][]interface{}{{timestamp, 5.0}},
					},
				},
			},
		},
	}

	// Setup the test HTTP server to mock Fiddler API
	receivedQueries := make(map[string][]client.Query)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Mock Models endpoint
		if r.URL.Path == "/v3/models" {
			modelsResponse := struct {
				Data struct {
					Items []client.Model `json:"items"`
				} `json:"data"`
			}{}
			modelsResponse.Data.Items = models
			json.NewEncoder(w).Encode(modelsResponse)
			return
		}

		// Mock metrics for a specific model
		metricsMatch := regexp.MustCompile(`^/v3/models/([^/]+)/metrics$`).FindStringSubmatch(r.URL.Path)
		if metricsMatch != nil {
			modelID := metricsMatch[1]
			metricsResponse := struct {
				Data struct {
					Metrics []client.Metric `json:"metrics"`
					Columns []client.Column `json:"columns"`
				} `json:"data"`
			}{}

			metrics, exists := modelMetrics[modelID]
			if !exists {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			metricsResponse.Data.Metrics = metrics
			metricsResponse.Data.Columns = []client.Column{
				{
					ID:    "age",
					Group: "Inputs",
				},
				{
					ID:    "balance",
					Group: "Inputs",
				},
				{
					ID:    "creditscore",
					Group: "Inputs",
				},
				{
					ID:    "gender",
					Group: "Inputs",
				},
				{
					ID:    "predicted_churn",
					Group: "Outputs",
				},
			}

			json.NewEncoder(w).Encode(metricsResponse)
			return
		}

		// Mock baselines for a specific model
		baselineMatch := regexp.MustCompile(`^/v3/models/([^/]+)/baselines$`).FindStringSubmatch(r.URL.Path)
		if baselineMatch != nil {
			modelID := baselineMatch[1]
			baselineResponse := struct {
				Data struct {
					Items []client.Baseline `json:"items"`
				} `json:"data"`
			}{}

			baselines, exists := modelBaselines[modelID]
			if !exists {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			baselineResponse.Data.Items = baselines
			json.NewEncoder(w).Encode(baselineResponse)
			return
		}

		// Mock query endpoint
		if r.URL.Path == "/v3/queries" && r.Method == http.MethodPost {
			var req client.QueryRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			// Find the model ID from the query
			var modelID string
			if len(req.Queries) > 0 {
				modelID = req.Queries[0].ModelID
			}

			// Store the received query for later validation
			receivedQueries[modelID] = req.Queries

			// Return the corresponding mock response
			if response, exists := queryResponses[modelID]; exists {
				json.NewEncoder(w).Encode(response)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
			return
		}

		// Default: not found
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	// Create receiver with test consumer
	config := &Config{
		Endpoint:           ts.URL,
		Token:              defaultAuthToken,
		Interval:           defaultInterval,
		Timeout:            defaultTimeout,
		EnabledMetricTypes: []string{"data_integrity", "drift", "service_metrics", "performance"},
	}
	sink := new(consumertest.MetricsSink)
	logger := zap.NewNop()
	settings := receivertest.NewNopSettings()
	settings.Logger = logger
	fr := newFiddlerReceiver(config, sink, settings)

	// Set up client
	otelClient, err := client.NewClient(
		client.WithEndpoint(ts.URL),
		client.WithToken(defaultAuthToken),
		client.WithTimeout(defaultTimeout),
	)

	require.NoError(t, err)

	fr.client = otelClient

	// Manually trigger collection
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = fr.collect(ctx)
	require.NoError(t, err)

	// Validate the queries sent to Fiddler API
	require.Len(t, receivedQueries, len(expectedQueries),
		"Expected queries for %d models, got %d", len(expectedQueries), len(receivedQueries))

	for modelID, expectedModelQueries := range expectedQueries {
		actualQueries, exists := receivedQueries[modelID]
		require.True(t, exists, "Expected queries for model %s but found none", modelID)

		// Sort queries by metric to ensure consistent comparison
		sortQueries := func(queries []client.Query) []client.Query {
			sorted := make([]client.Query, len(queries))
			copy(sorted, queries)
			sort.Slice(sorted, func(i, j int) bool {
				return sorted[i].Metric < sorted[j].Metric
			})
			return sorted
		}

		expectedSorted := sortQueries(expectedModelQueries)
		actualSorted := sortQueries(actualQueries)

		require.Len(t, actualSorted, len(expectedSorted),
			"Model %s: expected %d queries, got %d", modelID, len(expectedSorted), len(actualSorted))

		for i, expectedQuery := range expectedSorted {
			actualQuery := actualSorted[i]

			assert.Equal(t, expectedQuery.QueryKey, actualQuery.QueryKey,
				"Query key mismatch for %s", expectedQuery.Metric)
			assert.Equal(t, expectedQuery.Metric, actualQuery.Metric,
				"Metric mismatch for %s", expectedQuery.Metric)
			assert.Equal(t, expectedQuery.MetricType, actualQuery.MetricType,
				"Metric type mismatch for %s", expectedQuery.Metric)
			assert.Equal(t, expectedQuery.ModelID, actualQuery.ModelID,
				"Model ID mismatch for %s", expectedQuery.Metric)
			assert.Equal(t, expectedQuery.BaselineID, actualQuery.BaselineID,
				"Baseline ID mismatch for %s", expectedQuery.Metric)
			assert.Equal(t, expectedQuery.Categories, actualQuery.Categories,
				"Categories mismatch for %s", expectedQuery.Metric)
			assert.Equal(t, expectedQuery.Columns, actualQuery.Columns,
				"Columns mismatch for %s", expectedQuery.Metric)
		}
	}

	// Validate metrics output
	allMetrics := sink.AllMetrics()
	require.NotEmpty(t, allMetrics, "Expected metrics to be collected")
	metrics := allMetrics[0]

	// There should be 1 resource metrics (for the project)
	require.Equal(t, 1, metrics.ResourceMetrics().Len(), "Expected 1 resource metrics")
	rm := metrics.ResourceMetrics().At(0)

	// Check resource attributes
	attrs := rm.Resource().Attributes()
	serviceName, ok := attrs.Get("service.name")
	require.True(t, ok, "Expected service.name attribute")
	assert.Equal(t, "fiddler", serviceName.Str(), "Expected service.name to be 'fiddler'")

	project, ok := attrs.Get("fiddler.project")
	require.True(t, ok, "Expected fiddler.project attribute")
	assert.Equal(t, "Project 1", project.Str(), "Expected project name to be 'Project 1'")

	// Scope metrics
	require.Equal(t, 1, rm.ScopeMetrics().Len(), "Expected 1 scope metrics")
	sm := rm.ScopeMetrics().At(0)

	// Helper to find a metric by name
	findMetric := func(name string) (pmetric.Metric, bool) {
		for i := 0; i < sm.Metrics().Len(); i++ {
			metric := sm.Metrics().At(i)
			if metric.Name() == name {
				return metric, true
			}
		}
		return pmetric.Metric{}, false
	}

	// Verify total number of metrics
	// Model 1: 1 traffic + 1 jsd (with 3 data points) + 1 psi (with 3 data points) + 2 data integrity = 5
	// Model 2: 3 performance + 1 data integrity = 4
	// Total: 9
	expectedMetricsCount := 9
	assert.Equal(t, expectedMetricsCount, sm.Metrics().Len(),
		"Expected %d metrics, got %d", expectedMetricsCount, sm.Metrics().Len())

	// Check model 1 metrics
	// Check traffic metric (service_metrics)
	trafficMetric, found := findMetric("fiddler.service_metrics.traffic")
	require.True(t, found, "Expected traffic metric")
	require.Equal(t, 1, trafficMetric.Gauge().DataPoints().Len())
	dp := trafficMetric.Gauge().DataPoints().At(0)
	assert.Equal(t, 42.0, dp.DoubleValue())

	// Check model attribute
	val, ok := dp.Attributes().Get("model")
	require.True(t, ok, "Expected model attribute")
	assert.Equal(t, "Model 1", val.Str())

	// Check JSD metric (drift)
	jsdMetric, found := findMetric("fiddler.drift.jsd")
	require.True(t, found, "Expected jsd metric")
	require.Equal(t, 5, jsdMetric.Gauge().DataPoints().Len(), "Expected 5 data points for jsd (one per feature)")

	// Check attributes for first jsd data point
	dp = jsdMetric.Gauge().DataPoints().At(0)
	val, ok = dp.Attributes().Get("feature")
	require.True(t, ok, "Expected feature attribute")
	assert.Equal(t, "creditscore", val.Str())
	assert.Equal(t, 0.15, dp.DoubleValue())

	// Check PSI metric (drift)
	psiMetric, found := findMetric("fiddler.drift.psi")
	require.True(t, found, "Expected psi metric")
	require.Equal(t, 5, psiMetric.Gauge().DataPoints().Len(), "Expected 5 data points for psi (one per feature)")

	// Check data integrity metrics
	rangeViolationMetric, found := findMetric("fiddler.data_integrity.range_violation_count")
	require.True(t, found, "Expected range violation metric")
	require.Equal(t, 1, rangeViolationMetric.Gauge().DataPoints().Len())
	dp = rangeViolationMetric.Gauge().DataPoints().At(0)
	assert.Equal(t, 15.0, dp.DoubleValue())

	// Check model 2 metrics
	// Check precision metric (performance)
	precisionMetric, found := findMetric("fiddler.performance.precision")
	require.True(t, found, "Expected precision metric")
	require.Equal(t, 1, precisionMetric.Gauge().DataPoints().Len())
	dp = precisionMetric.Gauge().DataPoints().At(0)
	assert.Equal(t, 0.92, dp.DoubleValue())

	// Check model attribute
	val, ok = dp.Attributes().Get("model")
	require.True(t, ok, "Expected model attribute")
	assert.Equal(t, "Model 2", val.Str())

	// Check recall metric (performance)
	recallMetric, found := findMetric("fiddler.performance.recall")
	require.True(t, found, "Expected recall metric")
	require.Equal(t, 1, recallMetric.Gauge().DataPoints().Len())
	dp = recallMetric.Gauge().DataPoints().At(0)
	assert.Equal(t, 0.88, dp.DoubleValue())

	// Check accuracy metric (performance)
	accuracyMetric, found := findMetric("fiddler.performance.accuracy")
	require.True(t, found, "Expected accuracy metric")
	require.Equal(t, 1, accuracyMetric.Gauge().DataPoints().Len())
	dp = accuracyMetric.Gauge().DataPoints().At(0)
	assert.Equal(t, 0.90, dp.DoubleValue())
}

func TestHelperFunctions(t *testing.T) {
	// Test formatTime
	t1 := time.Date(2021, 6, 1, 12, 0, 0, 0, time.UTC)
	assert.Equal(t, "2021-06-01 12:00:00", formatTime(t1))

	// Test getBinSizeString
	assert.Equal(t, "Hour", getBinSizeString(30*time.Minute))
	assert.Equal(t, "Day", getBinSizeString(12*time.Hour))
	assert.Equal(t, "Week", getBinSizeString(7*24*time.Hour))
	assert.Equal(t, "Month", getBinSizeString(30*24*time.Hour))

	// Test isMetricEnabled
	enabledMetrics := []string{"traffic", "drift"}
	assert.True(t, isMetricEnabled("traffic", enabledMetrics))
	assert.True(t, isMetricEnabled("drift", enabledMetrics))
	assert.False(t, isMetricEnabled("performance", enabledMetrics))
	assert.True(t, isMetricEnabled("anything", []string{})) // empty list means all enabled
}
