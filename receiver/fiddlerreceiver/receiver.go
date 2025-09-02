// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package fiddlerreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/fiddlerreceiver"

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/fiddlerreceiver/internal/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/fiddlerreceiver/internal/metadata"
)

const (
	defaultBinSize = time.Hour
)

var (
	errNoModelsAvailable = fmt.Errorf("no models available in Fiddler API")
)

type fiddlerReceiver struct {
	settings receiver.Settings
	config   *Config
	consumer consumer.Metrics
	client   client.Client
	logger   *zap.Logger
	stopCh   chan struct{}
}

func newFiddlerReceiver(config *Config, consumer consumer.Metrics, settings receiver.Settings) *fiddlerReceiver {
	return &fiddlerReceiver{
		settings: settings,
		config:   config,
		consumer: consumer,
		logger:   settings.Logger,
		stopCh:   make(chan struct{}),
	}
}

// Start begins collecting metrics from Fiddler API.
func (fr *fiddlerReceiver) Start(ctx context.Context, host component.Host) error {
	var err error
	fr.client, err = client.NewClient(
		client.WithEndpoint(fr.config.Endpoint),
		client.WithToken(fr.config.Token),
		client.WithTimeout(fr.config.Timeout),
	)
	if err != nil {
		return fmt.Errorf("failed to create fiddler client: %w", err)
	}

	fr.logger.Info("Starting Fiddler metrics receiver",
		zap.String("endpoint", fr.config.Endpoint),
		zap.Duration("interval", fr.config.Interval),
		zap.Duration("offset", fr.config.Offset),
		zap.Strings("enabled_metric_types", fr.config.EnabledMetricTypes),
	)

	go fr.startCollection(ctx)

	return nil
}

func (fr *fiddlerReceiver) startCollection(ctx context.Context) {
	if err := fr.collect(ctx); err != nil {
		fr.logger.Error("Failed to collect metrics from Fiddler", zap.Error(err))
	}

	ticker := time.NewTicker(fr.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := fr.collect(ctx); err != nil {
				fr.logger.Error("Failed to collect metrics from Fiddler", zap.Error(err))
			}
		case <-fr.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (fr *fiddlerReceiver) collect(ctx context.Context) error {
	fr.logger.Debug("Collecting metrics from Fiddler")

	// List all models
	models, err := fr.client.ListModels(ctx)
	if err != nil {
		fr.logger.Error("Failed to list models, aborting collection", zap.Error(err))
		return err
	}

	if len(models) == 0 {
		return errNoModelsAvailable
	}

	fr.logger.Debug("Found models from Fiddler API", zap.Int("count", len(models)))

	mb := metadata.NewMetricBuilder(fr.logger)

	// Process each model
	for _, model := range models {
		fr.logger.Debug("Processing model",
			zap.String("model_id", model.ID),
			zap.String("model_name", model.Name),
			zap.String("project_name", model.Project.Name))

		// Get available metrics for the model
		metrics, _, err := fr.client.GetMetrics(ctx, model.ID)
		if err != nil {
			fr.logger.Error("Error getting metrics for model",
				zap.String("model_id", model.ID),
				zap.Error(err))
			continue
		}

		// Add metric types to the builder
		for _, metric := range metrics {
			mb.AddMetricType(metric.ID, metric.Type)
		}

		// Filter metrics by enabled types
		var enabledMetrics []client.Metric
		for _, metric := range metrics {
			if isMetricEnabled(metric.Type, fr.config.EnabledMetricTypes) {
				enabledMetrics = append(enabledMetrics, metric)
			}
		}

		if len(enabledMetrics) == 0 {
			fr.logger.Debug("No enabled metrics found for model", zap.String("model_id", model.ID))
			continue
		}

		// Create queries for enabled metrics
		queries, err := fr.createQueries(ctx, model.ID, enabledMetrics)
		if err != nil {
			fr.logger.Error("Error creating queries for model",
				zap.String("model_id", model.ID),
				zap.Error(err))
			continue
		}

		// Run queries if we have any
		if len(queries) == 0 {
			continue
		}

		// Calculate time range for query
		endTime := time.Now()
		startTime := endTime.Add(-fr.config.Offset)

		// Prepare query request
		request := client.QueryRequest{}
		request.ProjectID = model.Project.ID
		request.QueryType = "MONITORING"
		request.Filters.BinSize = getBinSizeString(defaultBinSize)
		request.Filters.TimeRange.StartTime = formatTime(startTime)
		request.Filters.TimeRange.EndTime = formatTime(endTime)
		request.Filters.TimeZone = "UTC"
		request.Queries = queries

		// Execute query
		response, err := fr.client.RunQuery(ctx, request)
		if err != nil {
			fr.logger.Error("Error running query for model",
				zap.String("model_id", model.ID),
				zap.Error(err))
			continue
		}

		// Add data points to metrics builder
		mb.AddDataPoints(model.Project.Name, response.Data.Results)
	}

	// Build and send metrics
	metrics := mb.Build()
	if err := fr.consumer.ConsumeMetrics(ctx, metrics); err != nil {
		fr.logger.Error("Failed to consume and process OTLP metrics", zap.Error(err))
		return err
	}

	fr.logger.Info("Successfully collected and processed metrics from Fiddler API")
	return nil
}

func (fr *fiddlerReceiver) createQueries(ctx context.Context, modelID string, metrics []client.Metric) ([]client.Query, error) {
	var queries []client.Query
	defaultBaselineName := "default_static_baseline"

	for _, metric := range metrics {
		// TODO: Skipping data_integrity metrics for any_violation_count and any_violation_percentage
		// because there is a known issue with the Fiddler API's handling of these metrics
		if metric.Type == "data_integrity" &&
			(metric.ID == "any_violation_count" || metric.ID == "any_violation_percentage") {
			fr.logger.Debug("Skipping known problematic metric",
				zap.String("model_id", modelID),
				zap.String("metric_id", metric.ID),
				zap.String("metric_type", metric.Type))
			continue
		}

		if metric.RequiresCategories {
			continue
		}

		baselineID := ""
		if metric.RequiresBaseline {
			var err error
			baselineID, err = fr.client.GetBaseline(ctx, modelID, defaultBaselineName)
			if err != nil {
				fr.logger.Warn("Failed to get baseline for model",
					zap.String("model_id", modelID),
					zap.Error(err))
				continue
			}
			if baselineID == "" {
				fr.logger.Debug("No baseline found for model, skipping metric",
					zap.String("model_id", modelID),
					zap.String("metric_id", metric.ID))
				continue
			}
		}

		queries = append(queries, client.Query{
			QueryKey:   metric.ID,
			Categories: []string{},
			Columns:    metric.Columns,
			VizType:    "line",
			Metric:     metric.ID,
			MetricType: metric.Type,
			ModelID:    modelID,
			BaselineID: baselineID,
		})
	}

	prettyJSON, err := json.MarshalIndent(queries, "", "  ")
	if err != nil {
		fr.logger.Debug("Created queries (failed to format as JSON)", zap.Any("queries", queries))
	} else {
		fr.logger.Debug("Created queries", zap.String("queries", string(prettyJSON)))
	}

	return queries, nil
}

// formatTime formats time for Fiddler API
func formatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}

// getBinSizeString gets the appropriate bin size string for a duration
func getBinSizeString(d time.Duration) string {
	hours := d.Hours()
	if hours <= 1 {
		return "Hour"
	}
	if hours <= 24 {
		return "Day"
	}
	if hours <= 168 {
		return "Week"
	}
	return "Month"
}

// isMetricEnabled checks if a metric type is in the enabled list
func isMetricEnabled(metricType string, enabledMetrics []string) bool {
	if len(enabledMetrics) == 0 {
		return true // if no enabled metrics specified, enable all
	}

	for _, enabled := range enabledMetrics {
		if enabled == metricType {
			return true
		}
	}
	return false
}

// Shutdown stops the Fiddler metrics receiver.
func (fr *fiddlerReceiver) Shutdown(ctx context.Context) error {
	fr.logger.Info("Stopping Fiddler metrics receiver")
	close(fr.stopCh)
	return nil
}
