// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package fiddlerreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/fiddlerreceiver"

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
)

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")
	assert.NoError(t, confmap.New().Unmarshal(&cfg))
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))

	fiddlerCfg, ok := cfg.(*Config)
	require.True(t, ok, "failed to convert to fiddler config")

	assert.Equal(t, defaultTimeout, fiddlerCfg.Timeout)
	assert.Equal(t, defaultInterval, fiddlerCfg.Interval)
	assert.Equal(t, defaultEndpoint, fiddlerCfg.Endpoint)
	assert.Equal(t, defaultAuthToken, fiddlerCfg.Token)
}

func TestValidateConfig(t *testing.T) {
	testCases := []struct {
		desc        string
		updateFunc  func(*Config)
		expectedErr string
	}{
		{
			desc: "valid config",
			updateFunc: func(cfg *Config) {
				cfg.Endpoint = "https://app.fiddler.ai"
				cfg.Token = "test-token"
			},
			expectedErr: "",
		},
		{
			desc:        "missing endpoint",
			updateFunc:  func(cfg *Config) { cfg.Endpoint = "" },
			expectedErr: "endpoint must be specified",
		},
		{
			desc:        "missing token",
			updateFunc:  func(cfg *Config) { cfg.Token = "" },
			expectedErr: "token must be specified",
		},
		{
			desc:        "interval too short",
			updateFunc:  func(cfg *Config) { cfg.Interval = 1 * time.Minute },
			expectedErr: fmt.Sprintf("interval must be at least %d minutes", minimumInterval/time.Minute),
		},
		{
			desc:        "timeout zero",
			updateFunc:  func(cfg *Config) { cfg.Timeout = 0 },
			expectedErr: "timeout must be greater than 0",
		},
		{
			desc:        "timeout negative",
			updateFunc:  func(cfg *Config) { cfg.Timeout = -1 * time.Second },
			expectedErr: "timeout must be greater than 0",
		},
		{
			desc:        "offset too long",
			updateFunc:  func(cfg *Config) { cfg.Offset = 49 * time.Hour },
			expectedErr: fmt.Sprintf("offset must be no more than %d hours", int(maximumOffset/time.Hour)),
		},
		{
			desc:        "offset missing",
			updateFunc:  func(cfg *Config) {},
			expectedErr: "",
		},
		{
			desc:        "offset valid",
			updateFunc:  func(cfg *Config) { cfg.Offset = 2 * time.Hour },
			expectedErr: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig().(*Config)

			// Set a valid configuration to start with
			cfg.Endpoint = "https://app.fiddler.ai"
			cfg.Token = "test-token"

			// Apply the test case specific update
			tc.updateFunc(cfg)

			// Validate
			err := cfg.Validate()
			if tc.expectedErr != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLoadConfig_MissingOffset(t *testing.T) {
	// given a user has provided a configuration for the Fiddler receiver
	// and that configuration specifies a valid `endpoint` and `token`
	// but the configuration does not specify an `offset` value
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	configMap := map[string]any{
		"endpoint": "https://app.fiddler.ai",
		"token":    "test-token",
	}
	conf := confmap.NewFromStringMap(configMap)

	// when the collector loads and validates this configuration
	err := conf.Unmarshal(&cfg)
	require.NoError(t, err)
	fiddlerCfg, ok := cfg.(*Config)
	require.True(t, ok)
	err = fiddlerCfg.Validate()

	// then the final configuration should be considered valid
	require.NoError(t, err)

	// and the `offset` value should be automatically set to the default
	assert.Equal(t, defaultOffset, fiddlerCfg.Offset)
}
