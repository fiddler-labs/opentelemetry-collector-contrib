// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metadata // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/fiddlerreceiver/internal/metadata"

import (
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/fiddlerreceiver/internal/client"
)

// MetricBuilder is a helper to create metrics from Fiddler API responses
type MetricBuilder struct {
	metrics       pmetric.Metrics
	logger        *zap.Logger
	metricTypeMap map[string]string // Maps metric ID to its type
}

// NewMetricBuilder creates a new MetricBuilder
func NewMetricBuilder(logger *zap.Logger) *MetricBuilder {
	if logger == nil {
		logger = zap.NewNop()
	}

	return &MetricBuilder{
		metrics:       pmetric.NewMetrics(),
		logger:        logger,
		metricTypeMap: make(map[string]string),
	}
}

// Build finalizes the metrics and returns them
func (b *MetricBuilder) Build() pmetric.Metrics {
	return b.metrics
}

// AddMetricType adds a metric type mapping
func (b *MetricBuilder) AddMetricType(metricID, metricType string) {
	b.metricTypeMap[metricID] = metricType
}

// AddDataPoints adds metrics data points from Fiddler API query results
func (b *MetricBuilder) AddDataPoints(projectName string, results map[string]client.QueryResult) {
	// Get a resource metric for this project
	rm := b.getOrCreateResourceMetric(projectName)

	// Get or create scope
	var sm pmetric.ScopeMetrics
	if rm.ScopeMetrics().Len() > 0 {
		sm = rm.ScopeMetrics().At(0)
	} else {
		sm = rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("fiddlerreceiver")
	}

	// Process each query result
	for _, result := range results {
		if len(result.Data) == 0 || len(result.ColNames) == 0 {
			continue
		}

		for _, row := range result.Data {
			if len(row) == 0 || len(row) != len(result.ColNames) {
				continue
			}

			timestamp, found := extractTimestamp(row, result.ColNames)
			if !found {
				b.logger.Error("Missing timestamp in row data",
					zap.String("project", projectName),
					zap.String("metric", result.Metric),
					zap.String("model", result.Model.Name))
				continue
			}

			// Process each column (skipping timestamp column)
			for colIdx, colName := range result.ColNames {
				if colName == "timestamp" {
					continue
				}

				if colIdx >= len(row) {
					continue
				}

				value, ok := extractValue(row[colIdx])
				if !ok {
					continue
				}

				b.addMetricFromColumn(sm, projectName, result, timestamp, colName, value)
			}
		}
	}
}

// getOrCreateResourceMetric gets an existing resource metric for the project or creates a new one
func (b *MetricBuilder) getOrCreateResourceMetric(projectName string) pmetric.ResourceMetrics {
	// Look for existing resource metric for this project
	for i := 0; i < b.metrics.ResourceMetrics().Len(); i++ {
		rm := b.metrics.ResourceMetrics().At(i)
		projectAttr, ok := rm.Resource().Attributes().Get("fiddler.project")
		if ok && projectAttr.Str() == projectName {
			return rm
		}
	}

	// Create new resource metric if not found
	rm := b.metrics.ResourceMetrics().AppendEmpty()
	resource := rm.Resource()
	resource.Attributes().PutStr("service.name", "fiddler")
	resource.Attributes().PutStr("fiddler.project", projectName)

	return rm
}

// addMetricFromColumn adds a metric based on the column name and metric type
func (b *MetricBuilder) addMetricFromColumn(sm pmetric.ScopeMetrics, projectName string, result client.QueryResult, timestamp time.Time, colName string, value float64) {
	modelName := result.Model.Name
	metricName := result.Metric
	modelVersion := result.Model.Version

	// Get metric type from our mapping, if available
	metricType, metricTypeExists := b.metricTypeMap[metricName]

	var fullMetricName string
	if metricTypeExists {
		fullMetricName = "fiddler." + metricType + "." + metricName
	} else {
		fullMetricName = "fiddler." + metricName
	}

	var metric pmetric.Metric
	var dp pmetric.NumberDataPoint
	metricFound := false

	for i := 0; i < sm.Metrics().Len(); i++ {
		if sm.Metrics().At(i).Name() == fullMetricName {
			metric = sm.Metrics().At(i)
			metricFound = true
			break
		}
	}

	if !metricFound {
		metric = sm.Metrics().AppendEmpty()
		metric.SetName(fullMetricName)
		metric.SetDescription("Fiddler metric: " + metricName)
		metric.SetUnit("1")
		metric.SetEmptyGauge() // Set empty gauge only when creating a new metric
	}

	dp = metric.Gauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	dp.SetDoubleValue(value)

	dp.Attributes().PutStr("model", modelName)
	dp.Attributes().PutStr("project", projectName)
	dp.Attributes().PutStr("model_version", modelVersion)

	if metricTypeExists && metricType != "" {
		dp.Attributes().PutStr("metric_type", metricType)
	}

	parts := splitColumnName(colName)
	if len(parts) > 1 {
		// Handle column names like "metric,feature"
		if parts[1] != "" {
			dp.Attributes().PutStr("feature", parts[1])
		}
	}
}

// extractTimestamp extracts the timestamp from a row
func extractTimestamp(row []interface{}, colNames []string) (time.Time, bool) {
	var timestampColIdx int = -1
	for i, colName := range colNames {
		if colName == "timestamp" {
			timestampColIdx = i
			break
		}
	}

	if timestampColIdx >= 0 && timestampColIdx < len(row) {
		if tsStr, ok := row[timestampColIdx].(string); ok {
			if t, err := time.Parse(time.RFC3339, tsStr); err == nil {
				return t.UTC(), true
			}
		}
	}

	return time.Time{}, false
}

// extractValue extracts a numeric value from various types
func extractValue(rawValue interface{}) (float64, bool) {
	switch v := rawValue.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// splitColumnName splits a column name like "metric,feature" into parts
func splitColumnName(name string) []string {
	if name == "" {
		return []string{}
	}

	return strings.Split(name, ",")
}
