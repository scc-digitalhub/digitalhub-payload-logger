// SPDX-FileCopyrightText: © 2026 DSLab - Fondazione Bruno Kessler
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for the payload-logger application
type Config struct {
	Server     ServerConfig     `yaml:"server"`
	Database   DatabaseConfig   `yaml:"database"`
	Processing ProcessingConfig `yaml:"processing"`
	Memory     MemoryConfig     `yaml:"memory"`
}

// ServerConfig holds server-related configuration
type ServerConfig struct {
	Port              string `yaml:"port"`
	SinkType          string `yaml:"sink_type"`
	MetadataNamespace string `yaml:"metadata_namespace"`
}

// DatabaseConfig holds database-related configuration
type DatabaseConfig struct {
	URL             string        `yaml:"url"`
	TableName       string        `yaml:"table_name"`
	MaxRetries      int           `yaml:"max_retries"`
	BatchSize       int           `yaml:"batch_size"`
	FlushInterval   time.Duration `yaml:"flush_interval"`
	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
}

// ProcessingConfig holds processing-related configuration
type ProcessingConfig struct {
	NumWorkers        int `yaml:"num_workers"`
	ChannelBufferSize int `yaml:"channel_buffer_size"`
}

// MemoryConfig holds memory management configuration
type MemoryConfig struct {
	MaxBodySize int `yaml:"max_body_size"`
}

// LoadConfig loads configuration from YAML file and environment variables
func LoadConfig(configPath string) (*Config, error) {
	config := &Config{
		Server: ServerConfig{
			Port:              ":5051",
			SinkType:          "console",
			MetadataNamespace: "digitalhub",
		},
		Database: DatabaseConfig{
			TableName:       "request_logs",
			MaxRetries:      3,
			BatchSize:       50,
			FlushInterval:   10 * time.Second,
			MaxOpenConns:    25,
			MaxIdleConns:    5,
			ConnMaxLifetime: 5 * time.Minute,
		},
		Processing: ProcessingConfig{
			NumWorkers:        10,
			ChannelBufferSize: 1000,
		},
		Memory: MemoryConfig{
			MaxBodySize: 10 * 1024 * 1024, // 10MB
		},
	}

	// Load from YAML file if provided
	if configPath != "" {
		if err := loadFromYAML(configPath, config); err != nil {
			return nil, fmt.Errorf("failed to load config from YAML: %w", err)
		}
	}

	// Override with environment variables
	loadFromEnv(config)

	return config, nil
}

// loadFromYAML loads configuration from YAML file
func loadFromYAML(path string, config *Config) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	return decoder.Decode(config)
}

// loadFromEnv overrides configuration with environment variables
func loadFromEnv(config *Config) {
	// Server config
	if port := os.Getenv("PORT"); port != "" {
		config.Server.Port = port
	}
	if sinkType := os.Getenv("SINK_TYPE"); sinkType != "" {
		config.Server.SinkType = sinkType
	}
	if namespace := os.Getenv("METADATA_NAMESPACE"); namespace != "" {
		config.Server.MetadataNamespace = namespace
	}

	// Database config
	if url := os.Getenv("DATABASE_URL"); url != "" {
		config.Database.URL = url
	}
	if table := os.Getenv("TABLE_NAME"); table != "" {
		config.Database.TableName = table
	}
	if retries := os.Getenv("MAX_RETRIES"); retries != "" {
		if val, err := strconv.Atoi(retries); err == nil {
			config.Database.MaxRetries = val
		}
	}
	if batchSize := os.Getenv("BATCH_SIZE"); batchSize != "" {
		if val, err := strconv.Atoi(batchSize); err == nil {
			config.Database.BatchSize = val
		}
	}

	// Processing config
	if workers := os.Getenv("NUM_WORKERS"); workers != "" {
		if val, err := strconv.Atoi(workers); err == nil {
			config.Processing.NumWorkers = val
		}
	}
	if bufferSize := os.Getenv("CHANNEL_BUFFER_SIZE"); bufferSize != "" {
		if val, err := strconv.Atoi(bufferSize); err == nil {
			config.Processing.ChannelBufferSize = val
		}
	}

	// Memory config
	if maxBody := os.Getenv("MAX_BODY_SIZE"); maxBody != "" {
		if val, err := strconv.Atoi(maxBody); err == nil {
			config.Memory.MaxBodySize = val
		}
	}
}
