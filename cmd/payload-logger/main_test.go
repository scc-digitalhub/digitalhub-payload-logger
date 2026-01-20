// SPDX-FileCopyrightText: © 2026 DSLab - Fondazione Bruno Kessler
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfig_Defaults(t *testing.T) {
	config, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Test default values
	if config.Server.Port != ":5051" {
		t.Errorf("Server.Port = %v, want :5051", config.Server.Port)
	}
	if config.Server.MetadataNamespace != "digitalhub" {
		t.Errorf("Server.MetadataNamespace = %v, want digitalhub", config.Server.MetadataNamespace)
	}
	if config.Database.TableName != "request_logs" {
		t.Errorf("Database.TableName = %v, want request_logs", config.Database.TableName)
	}
	if config.Database.MaxRetries != 3 {
		t.Errorf("Database.MaxRetries = %v, want 3", config.Database.MaxRetries)
	}
	if config.Database.BatchSize != 50 {
		t.Errorf("Database.BatchSize = %v, want 50", config.Database.BatchSize)
	}
	if config.Database.FlushInterval != 10*time.Second {
		t.Errorf("Database.FlushInterval = %v, want 10s", config.Database.FlushInterval)
	}
	if config.Processing.NumWorkers != 10 {
		t.Errorf("Processing.NumWorkers = %v, want 10", config.Processing.NumWorkers)
	}
	if config.Processing.ChannelBufferSize != 1000 {
		t.Errorf("Processing.ChannelBufferSize = %v, want 1000", config.Processing.ChannelBufferSize)
	}
}

func TestLoadConfig_FromYAML(t *testing.T) {
	// Create a temporary YAML file
	yamlContent := `
server:
  port: ":8080"
  sink_type: "postgres"
  metadata_namespace: "custom-namespace"
database:
  url: "postgres://test:test@localhost/testdb"
  table_name: "custom_logs"
  max_retries: 5
  batch_size: 100
  flush_interval: "30s"
  max_open_conns: 50
  max_idle_conns: 10
  conn_max_lifetime: "10m"
  max_body_size: 20971520
processing:
  num_workers: 20
  channel_buffer_size: 2000
memory:
  max_body_size: 20971520
  max_log_body_size: 2048
`

	tmpFile, err := os.CreateTemp("", "config_test_*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(yamlContent); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	config, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Test loaded values
	if config.Server.Port != ":8080" {
		t.Errorf("Server.Port = %v, want :8080", config.Server.Port)
	}
	if config.Server.SinkType != "postgres" {
		t.Errorf("Server.SinkType = %v, want postgres", config.Server.SinkType)
	}
	if config.Server.MetadataNamespace != "custom-namespace" {
		t.Errorf("Server.MetadataNamespace = %v, want custom-namespace", config.Server.MetadataNamespace)
	}
	if config.Database.URL != "postgres://test:test@localhost/testdb" {
		t.Errorf("Database.URL = %v, want postgres://test:test@localhost/testdb", config.Database.URL)
	}
	if config.Database.TableName != "custom_logs" {
		t.Errorf("Database.TableName = %v, want custom_logs", config.Database.TableName)
	}
	if config.Database.MaxRetries != 5 {
		t.Errorf("Database.MaxRetries = %v, want 5", config.Database.MaxRetries)
	}
	if config.Database.BatchSize != 100 {
		t.Errorf("Database.BatchSize = %v, want 100", config.Database.BatchSize)
	}
	if config.Database.FlushInterval != 30*time.Second {
		t.Errorf("Database.FlushInterval = %v, want 30s", config.Database.FlushInterval)
	}
	if config.Processing.NumWorkers != 20 {
		t.Errorf("Processing.NumWorkers = %v, want 20", config.Processing.NumWorkers)
	}
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "invalid_config_*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write invalid YAML
	if _, err := tmpFile.WriteString("invalid: yaml: content: [\n"); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	_, err = LoadConfig(tmpFile.Name())
	if err == nil {
		t.Error("LoadConfig() should have failed with invalid YAML")
	}
}

func TestLoadConfig_NonExistentFile(t *testing.T) {
	_, err := LoadConfig("/non/existent/file.yaml")
	if err == nil {
		t.Error("LoadConfig() should have failed with non-existent file")
	}
}

func TestLoadFromEnv(t *testing.T) {
	// Set environment variables
	envVars := map[string]string{
		"PORT":               ":9090",
		"SINK_TYPE":          "postgres",
		"METADATA_NAMESPACE": "env-namespace",
		"DATABASE_URL":       "postgres://env:env@localhost/envdb",
		"TABLE_NAME":         "env_logs",
		"NUM_WORKERS":        "15",
	}

	// Backup original values
	originalValues := make(map[string]string)
	for key := range envVars {
		originalValues[key] = os.Getenv(key)
	}

	// Set test values
	for key, value := range envVars {
		os.Setenv(key, value)
	}

	// Restore original values after test
	defer func() {
		for key, value := range originalValues {
			if value == "" {
				os.Unsetenv(key)
			} else {
				os.Setenv(key, value)
			}
		}
	}()

	config := &Config{}
	loadFromEnv(config)

	// Test environment variable overrides
	if config.Server.Port != ":9090" {
		t.Errorf("Server.Port = %v, want :9090", config.Server.Port)
	}
	if config.Server.SinkType != "postgres" {
		t.Errorf("Server.SinkType = %v, want postgres", config.Server.SinkType)
	}
	if config.Server.MetadataNamespace != "env-namespace" {
		t.Errorf("Server.MetadataNamespace = %v, want env-namespace", config.Server.MetadataNamespace)
	}
	if config.Database.URL != "postgres://env:env@localhost/envdb" {
		t.Errorf("Database.URL = %v, want postgres://env:env@localhost/envdb", config.Database.URL)
	}
	if config.Database.TableName != "env_logs" {
		t.Errorf("Database.TableName = %v, want env_logs", config.Database.TableName)
	}
	if config.Processing.NumWorkers != 15 {
		t.Errorf("Processing.NumWorkers = %v, want 15", config.Processing.NumWorkers)
	}
}
