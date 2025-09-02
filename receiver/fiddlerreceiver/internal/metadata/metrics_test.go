// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metadata

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/fiddlerreceiver/internal/client"
)

func TestMetricBuilder(t *testing.T) {
	mb := NewMetricBuilder(nil)
	assert.NotNil(t, mb)

	metrics := mb.Build()
	assert.Equal(t, 0, metrics.ResourceMetrics().Len())
}

func TestAddDataPoints(t *testing.T) {
	timestampStr := "2025-03-04T15:00:00+00:00"

	expectedTime, err := time.Parse(time.RFC3339, timestampStr)
	require.NoError(t, err)

	// Create test metric builder with predefined metric types
	mb := NewMetricBuilder(nil)

	// Add metric types
	mb.AddMetricType("jsd", "drift")
	mb.AddMetricType("precision", "performance")
	mb.AddMetricType("recall", "performance")
	mb.AddMetricType("type_violation_count", "data_integrity")
	mb.AddMetricType("traffic", "service_metrics")

	results := map[string]client.QueryResult{
		"traffic": {
			Model: client.Model{
				ID:   "model1",
				Name: "Model 1",
				Project: client.Project{
					ID:   "project1",
					Name: "Project 1",
				},
			},
			Metric:   "traffic",
			ColNames: []string{"timestamp", "traffic"},
			Columns:  []string{},
			Data:     [][]interface{}{{timestampStr, 50}},
		},
		"jsd": {
			Model: client.Model{
				ID:   "model1",
				Name: "Model 1",
				Project: client.Project{
					ID:   "project1",
					Name: "Project 1",
				},
			},
			Metric:   "jsd",
			ColNames: []string{"timestamp", "jsd,feature1"},
			Columns:  []string{"feature1"},
			Data:     [][]interface{}{{timestampStr, 0.85}},
		},
		"precision": {
			Model: client.Model{
				ID:   "model2",
				Name: "Model 2",
				Project: client.Project{
					ID:   "project1",
					Name: "Project 1",
				},
			},
			Metric:   "precision",
			ColNames: []string{"timestamp", "precision"},
			Columns:  []string{},
			Data:     [][]interface{}{{timestampStr, 0.92}},
		},
		"recall": {
			Model: client.Model{
				ID:   "model2",
				Name: "Model 2",
				Project: client.Project{
					ID:   "project1",
					Name: "Project 1",
				},
			},
			Metric:   "recall",
			ColNames: []string{"timestamp", "recall"},
			Columns:  []string{},
			Data:     [][]interface{}{{timestampStr, 0.88}},
		},
		"type_violation_count": {
			Model: client.Model{
				ID:   "model3",
				Name: "Model 3",
				Project: client.Project{
					ID:   "project1",
					Name: "Project 1",
				},
			},
			Metric: "type_violation_count",
			ColNames: []string{
				"timestamp",
				"type_violation_count,feature1",
				"type_violation_count,feature2",
				"type_violation_count,feature3",
			},
			Columns: []string{"feature1", "feature2", "feature3"},
			Data:    [][]interface{}{{timestampStr, 15, 3, 2}},
		},
		"empty": {
			Model: client.Model{
				ID:   "model3",
				Name: "Model 3",
			},
			Metric:   "empty",
			ColNames: []string{"timestamp"},
			Columns:  []string{},
			Data:     [][]interface{}{},
		},
	}

	// Build metrics
	mb.AddDataPoints("Project 1", results)
	metrics := mb.Build()

	// Verify resource metrics
	assert.Equal(t, 1, metrics.ResourceMetrics().Len())
	rm := metrics.ResourceMetrics().At(0)

	// Verify resource attributes
	attrs := rm.Resource().Attributes()
	val, ok := attrs.Get("service.name")
	assert.True(t, ok)
	assert.Equal(t, "fiddler", val.Str())

	val, ok = attrs.Get("fiddler.project")
	assert.True(t, ok)
	assert.Equal(t, "Project 1", val.Str())

	// Verify scope metrics
	assert.Equal(t, 1, rm.ScopeMetrics().Len())
	sm := rm.ScopeMetrics().At(0)

	// Expected metrics:
	// - 1 traffic metric
	// - 1 jsd metric with feature
	// - 2 performance metrics (precision, recall)
	// - 1 data integrity metric (type violation with multiple features as data points)
	expectedMetricCount := 5
	assert.Equal(t, expectedMetricCount, sm.Metrics().Len())

	expectedTimestamp := pcommon.NewTimestampFromTime(expectedTime)

	// For all metrics, verify they have the correct timestamp
	for i := 0; i < sm.Metrics().Len(); i++ {
		metric := sm.Metrics().At(i)
		if metric.Gauge().DataPoints().Len() > 0 {
			dp := metric.Gauge().DataPoints().At(0)
			assert.Equal(t, expectedTimestamp, dp.Timestamp(),
				"Timestamp not preserved for metric %s", metric.Name())
		}
	}

	// Helper to find a specific metric by name
	findMetric := func(name string) (int, bool) {
		for i := 0; i < sm.Metrics().Len(); i++ {
			if sm.Metrics().At(i).Name() == name {
				return i, true
			}
		}
		return 0, false
	}

	// Check traffic metric
	idx, found := findMetric("fiddler.service_metrics.traffic")
	assert.True(t, found)
	trafficMetric := sm.Metrics().At(idx)
	assert.Equal(t, "fiddler.service_metrics.traffic", trafficMetric.Name())

	// Check traffic data points
	require.Equal(t, 1, trafficMetric.Gauge().DataPoints().Len())
	dp := trafficMetric.Gauge().DataPoints().At(0)
	assert.Equal(t, expectedTimestamp, dp.Timestamp())
	assert.Equal(t, 50.0, dp.DoubleValue())

	// Check traffic attributes
	dpAttrs := dp.Attributes()
	val, ok = dpAttrs.Get("model")
	assert.True(t, ok)
	assert.Equal(t, "Model 1", val.Str())

	// Check jsd metric
	idx, found = findMetric("fiddler.drift.jsd")
	assert.True(t, found)
	jsdMetric := sm.Metrics().At(idx)
	assert.Equal(t, "fiddler.drift.jsd", jsdMetric.Name())

	// Check jsd data point
	require.Equal(t, 1, jsdMetric.Gauge().DataPoints().Len())
	dp = jsdMetric.Gauge().DataPoints().At(0)
	assert.Equal(t, expectedTimestamp, dp.Timestamp())
	assert.Equal(t, 0.85, dp.DoubleValue())

	// Check jsd attributes
	dpAttrs = dp.Attributes()
	val, ok = dpAttrs.Get("feature")
	assert.True(t, ok)
	assert.Equal(t, "feature1", val.Str())
	val, ok = dpAttrs.Get("model")
	assert.True(t, ok)
	assert.Equal(t, "Model 1", val.Str())

	// Check precision metric
	idx, found = findMetric("fiddler.performance.precision")
	assert.True(t, found)
	precisionMetric := sm.Metrics().At(idx)
	assert.Equal(t, "fiddler.performance.precision", precisionMetric.Name())

	require.Equal(t, 1, precisionMetric.Gauge().DataPoints().Len())
	dp = precisionMetric.Gauge().DataPoints().At(0)
	assert.Equal(t, expectedTimestamp, dp.Timestamp())
	assert.Equal(t, 0.92, dp.DoubleValue())

	val, ok = dp.Attributes().Get("model")
	assert.True(t, ok)
	assert.Equal(t, "Model 2", val.Str())

	// Check recall metric
	idx, found = findMetric("fiddler.performance.recall")
	assert.True(t, found)
	recallMetric := sm.Metrics().At(idx)
	assert.Equal(t, "fiddler.performance.recall", recallMetric.Name())

	require.Equal(t, 1, recallMetric.Gauge().DataPoints().Len())
	dp = recallMetric.Gauge().DataPoints().At(0)
	assert.Equal(t, expectedTimestamp, dp.Timestamp())
	assert.Equal(t, 0.88, dp.DoubleValue())

	val, ok = dp.Attributes().Get("model")
	assert.True(t, ok)
	assert.Equal(t, "Model 2", val.Str())

	// Check data integrity metrics
	idx, found = findMetric("fiddler.data_integrity.type_violation_count")
	assert.True(t, found)
	typeViolationMetric := sm.Metrics().At(idx)
	assert.Equal(t, "fiddler.data_integrity.type_violation_count", typeViolationMetric.Name())

	require.Equal(t, 3, typeViolationMetric.Gauge().DataPoints().Len()) // One for each feature

	// First feature violation
	dp = typeViolationMetric.Gauge().DataPoints().At(0)
	assert.Equal(t, 15.0, dp.DoubleValue())
	dpAttrs = dp.Attributes()
	val, ok = dpAttrs.Get("feature")
	assert.True(t, ok)
	assert.Equal(t, "feature1", val.Str())
	val, ok = dpAttrs.Get("model")
	assert.True(t, ok)
	assert.Equal(t, "Model 3", val.Str())

	// Second feature violation
	dp = typeViolationMetric.Gauge().DataPoints().At(1)
	assert.Equal(t, 3.0, dp.DoubleValue())
	dpAttrs = dp.Attributes()
	val, ok = dpAttrs.Get("feature")
	assert.True(t, ok)
	assert.Equal(t, "feature2", val.Str())
	val, ok = dpAttrs.Get("model")
	assert.True(t, ok)
	assert.Equal(t, "Model 3", val.Str())
}

func TestSplitColumnName(t *testing.T) {
	testCases := []struct {
		input    string
		expected []string
	}{
		{
			input:    "jsd,feature1",
			expected: []string{"jsd", "feature1"},
		},
		{
			input:    "simple",
			expected: []string{"simple"},
		},
		{
			input:    "",
			expected: []string{},
		},
		{
			input:    "a,b,c",
			expected: []string{"a", "b", "c"},
		},
	}

	for _, tc := range testCases {
		result := splitColumnName(tc.input)
		assert.Equal(t, tc.expected, result)
	}
}

func TestExtractTimestamp(t *testing.T) {
	testCases := []struct {
		name     string
		row      []interface{}
		colNames []string
		expectOk bool
		expected time.Time
	}{
		{
			name:     "string timestamp",
			row:      []interface{}{"2023-01-01T12:00:00Z", 123},
			colNames: []string{"timestamp", "value"},
			expectOk: true,
			expected: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		},
		{
			name:     "string timestamp",
			row:      []interface{}{"2023-01-01T12:00:00Z", 123},
			colNames: []string{"timestamp", "value"},
			expectOk: true,
			expected: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		},
		{
			name:     "no timestamp column",
			row:      []interface{}{123, 456},
			colNames: []string{"col1", "col2"},
			expectOk: false,
		},
		{
			name:     "invalid timestamp format",
			row:      []interface{}{"not-a-timestamp", 123},
			colNames: []string{"timestamp", "value"},
			expectOk: false,
		},
		{
			name:     "timestamp is not a string",
			row:      []interface{}{123.456, 789},
			colNames: []string{"timestamp", "value"},
			expectOk: false,
		},
		{
			name:     "timestamp column index out of bounds",
			row:      []interface{}{},
			colNames: []string{"timestamp", "value"},
			expectOk: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ts, ok := extractTimestamp(tc.row, tc.colNames)
			assert.Equal(t, tc.expectOk, ok)
			if tc.expectOk {
				assert.False(t, ts.IsZero())
			}
		})
	}
}

func TestExtractValue(t *testing.T) {
	testCases := []struct {
		name     string
		input    interface{}
		expected float64
		expectOk bool
	}{
		{
			name:     "float value",
			input:    42.5,
			expected: 42.5,
			expectOk: true,
		},
		{
			name:     "integer value",
			input:    42,
			expected: 42.0,
			expectOk: true,
		},
		{
			name:     "string number",
			input:    "42.5",
			expected: 42.5,
			expectOk: true,
		},
		{
			name:     "non-numeric string",
			input:    "not-a-number",
			expected: 0,
			expectOk: false,
		},
		{
			name:     "boolean value",
			input:    true,
			expected: 0,
			expectOk: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			val, ok := extractValue(tc.input)
			assert.Equal(t, tc.expectOk, ok)
			if tc.expectOk {
				assert.Equal(t, tc.expected, val)
			}
		})
	}
}
