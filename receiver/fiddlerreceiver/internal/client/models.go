// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/fiddlerreceiver/internal/client"

// Model represents a Fiddler model
type Model struct {
	ID      string  `json:"id"`
	Name    string  `json:"name"`
	Project Project `json:"project"`
	Version string  `json:"version"`
}

// Project represents a Fiddler project
type Project struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Metric represents a metric type available for a model
type Metric struct {
	ID                 string   `json:"id"`
	Type               string   `json:"type"`
	Columns            []string `json:"columns"`
	RequiresCategories bool     `json:"requires_categories"`
	RequiresBaseline   bool     `json:"requires_baseline"`
}

// Column represents a data column in Fiddler
type Column struct {
	ID    string `json:"id"`
	Group string `json:"group"`
}

// Baseline represents a Fiddler model baseline
type Baseline struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// QueryRequest represents a request to the Fiddler queries endpoint
type QueryRequest struct {
	Filters struct {
		BinSize   string `json:"bin_size"`
		TimeRange struct {
			StartTime string `json:"start_time"`
			EndTime   string `json:"end_time"`
		} `json:"time_range"`
		TimeZone string `json:"time_zone"`
	} `json:"filters"`
	ProjectID string  `json:"project_id"`
	QueryType string  `json:"query_type"`
	Queries   []Query `json:"queries"`
}

// Query represents an individual query in a QueryRequest
type Query struct {
	QueryKey   string   `json:"query_key"`
	Categories []string `json:"categories"`
	Columns    []string `json:"columns"`
	VizType    string   `json:"viz_type"`
	Metric     string   `json:"metric"`
	MetricType string   `json:"metric_type"`
	ModelID    string   `json:"model_id"`
	BaselineID string   `json:"baseline_id"`
}

// QueryResponse represents the response from a Fiddler query
type QueryResponse struct {
	Data struct {
		Project Project                `json:"project"`
		Results map[string]QueryResult `json:"results"`
	} `json:"data"`
}

// QueryResult represents the result of a single query
type QueryResult struct {
	Model    Model           `json:"model"`
	Metric   string          `json:"metric"`
	Columns  []string        `json:"columns"`
	ColNames []string        `json:"col_names"`
	Data     [][]interface{} `json:"data"`
}
