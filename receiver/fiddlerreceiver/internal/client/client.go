// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/fiddlerreceiver/internal/client"

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client is the interface for interacting with the Fiddler API
type Client interface {
	// ListModels retrieves all models from the Fiddler API
	ListModels(ctx context.Context) ([]Model, error)

	// GetMetrics retrieves the available metrics for a model
	GetMetrics(ctx context.Context, modelID string) ([]Metric, []string, error)

	// GetBaseline retrieves a specific baseline or the default one for a model
	GetBaseline(ctx context.Context, modelID, baselineName string) (string, error)

	// RunQuery executes a monitoring query and returns the results
	RunQuery(ctx context.Context, request QueryRequest) (*QueryResponse, error)
}

// config contains the configuration for the Fiddler client
type config struct {
	endpoint string
	token    string
	timeout  time.Duration
}

// Option is used to configure the Fiddler client
type Option func(*config)

// WithEndpoint sets the Fiddler API endpoint
func WithEndpoint(endpoint string) Option {
	return func(c *config) {
		c.endpoint = endpoint
	}
}

// WithToken sets the Fiddler API token
func WithToken(token string) Option {
	return func(c *config) {
		c.token = token
	}
}

// WithTimeout sets the HTTP timeout
func WithTimeout(timeout time.Duration) Option {
	return func(c *config) {
		c.timeout = timeout
	}
}

// HTTPClient is the concrete implementation of the Client interface
type HTTPClient struct {
	client   *http.Client
	endpoint string
	token    string
}

// NewClient creates a new Fiddler API client
func NewClient(opts ...Option) (Client, error) {
	cfg := &config{
		timeout: 30 * time.Second,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.endpoint == "" {
		return nil, fmt.Errorf("endpoint must be specified")
	}

	if cfg.token == "" {
		return nil, fmt.Errorf("token must be specified")
	}

	// Remove trailing slash if present
	endpoint := strings.TrimSuffix(cfg.endpoint, "/")

	httpClient := &http.Client{
		Timeout: cfg.timeout,
	}

	return &HTTPClient{
		client:   httpClient,
		endpoint: endpoint,
		token:    cfg.token,
	}, nil
}

// call performs an HTTP request to the Fiddler API
func (c *HTTPClient) call(ctx context.Context, method, endpoint string, jsonRequest interface{}) (*http.Response, error) {
	var reqBody io.Reader
	if jsonRequest != nil {
		reqData, err := json.Marshal(jsonRequest)
		if err != nil {
			return nil, fmt.Errorf("error marshaling JSON request: %w", err)
		}
		reqBody = strings.NewReader(string(reqData))
	}

	fullURL := fmt.Sprintf("%s/%s", c.endpoint, endpoint)
	req, err := http.NewRequestWithContext(ctx, method, fullURL, reqBody)
	if err != nil {
		return nil, fmt.Errorf("error creating HTTP request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.token))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Integration", "fiddler-otel-receiver")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing HTTP request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		bodyBytes, _ := io.ReadAll(resp.Body)
		bodyStr := string(bodyBytes)

		return nil, &APIError{
			StatusCode: resp.StatusCode,
			Message:    bodyStr,
			Endpoint:   endpoint,
		}
	}

	return resp, nil
}

// ListModels implements the Client interface
func (c *HTTPClient) ListModels(ctx context.Context) ([]Model, error) {
	endpoint := "v3/models"

	resp, err := c.call(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var listResponse struct {
		Data struct {
			Items []Model `json:"items"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&listResponse); err != nil {
		return nil, fmt.Errorf("error decoding model list response: %w", err)
	}

	return listResponse.Data.Items, nil
}

// GetMetrics retrieves the available metrics for a model
func (c *HTTPClient) GetMetrics(ctx context.Context, modelID string) ([]Metric, []string, error) {
	endpoint := fmt.Sprintf("v3/models/%s/metrics", modelID)

	resp, err := c.call(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	var metricsResponse struct {
		Data struct {
			Metrics []Metric `json:"metrics"`
			Columns []Column `json:"columns"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&metricsResponse); err != nil {
		return nil, nil, fmt.Errorf("error decoding metrics response: %w", err)
	}

	// Extract outputs
	outputs := []string{}
	for _, column := range metricsResponse.Data.Columns {
		if column.Group == "Outputs" {
			outputs = append(outputs, column.ID)
		}
	}

	return metricsResponse.Data.Metrics, outputs, nil
}

// GetBaseline retrieves a specific baseline or the default one for a model
func (c *HTTPClient) GetBaseline(ctx context.Context, modelID, baselineName string) (string, error) {
	endpoint := fmt.Sprintf("v3/models/%s/baselines", modelID)

	resp, err := c.call(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var baselinesResponse struct {
		Data struct {
			Items []Baseline `json:"items"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&baselinesResponse); err != nil {
		return "", fmt.Errorf("error decoding baselines response: %w", err)
	}

	// Look for the specific baseline
	for _, baseline := range baselinesResponse.Data.Items {
		if baseline.Name == baselineName {
			return baseline.ID, nil
		}
	}

	// If not found and there are baselines, return the first one
	if len(baselinesResponse.Data.Items) > 0 {
		return baselinesResponse.Data.Items[0].ID, nil
	}

	return "", nil
}

// RunQuery executes a monitoring query and returns the results
func (c *HTTPClient) RunQuery(ctx context.Context, request QueryRequest) (*QueryResponse, error) {
	endpoint := "v3/queries"

	resp, err := c.call(ctx, http.MethodPost, endpoint, request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var queryResponse QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResponse); err != nil {
		return nil, fmt.Errorf("error decoding query response: %w", err)
	}

	return &queryResponse, nil
}
