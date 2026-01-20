// SPDX-FileCopyrightText: © 2026 DSLab - Fondazione Bruno Kessler
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestMetadataProcessing(t *testing.T) {
	// Test case 1: Metadata exists in correct namespace
	t.Run("metadata_found", func(t *testing.T) {
		metadata, err := structpb.NewStruct(map[string]interface{}{
			"service_name": "test-service",
			"version":      "1.0",
			"request_id":   "req-123",
		})
		require.NoError(t, err)

		metadataContext := &corev3.Metadata{
			FilterMetadata: map[string]*structpb.Struct{
				"test-namespace": metadata,
			},
		}

		// Simulate the logic from processor.go
		var attrs map[string]interface{}
		if metadataContext != nil {
			dhMetadata := metadataContext.GetFilterMetadata()["test-namespace"]
			if dhMetadata != nil {
				attrs = dhMetadata.AsMap()
			}
		}

		assert.NotNil(t, attrs)
		assert.Equal(t, "test-service", attrs["service_name"])
		assert.Equal(t, "1.0", attrs["version"])
		assert.Equal(t, "req-123", attrs["request_id"])
	})

	// Test case 2: No metadata context
	t.Run("no_metadata_context", func(t *testing.T) {
		var metadataContext *corev3.Metadata

		var attrs map[string]interface{}
		if metadataContext != nil {
			dhMetadata := metadataContext.GetFilterMetadata()["test-namespace"]
			if dhMetadata != nil {
				attrs = dhMetadata.AsMap()
			}
		}

		assert.Nil(t, attrs)
	})

	// Test case 3: Metadata context exists but wrong namespace
	t.Run("wrong_namespace", func(t *testing.T) {
		metadata, err := structpb.NewStruct(map[string]interface{}{
			"service_name": "test-service",
		})
		require.NoError(t, err)

		metadataContext := &corev3.Metadata{
			FilterMetadata: map[string]*structpb.Struct{
				"wrong-namespace": metadata,
			},
		}

		var attrs map[string]interface{}
		if metadataContext != nil {
			dhMetadata := metadataContext.GetFilterMetadata()["test-namespace"]
			if dhMetadata != nil {
				attrs = dhMetadata.AsMap()
			}
		}

		assert.Nil(t, attrs)
	})

	// Test case 4: Metadata context exists but namespace not found
	t.Run("namespace_not_found", func(t *testing.T) {
		metadataContext := &corev3.Metadata{
			FilterMetadata: map[string]*structpb.Struct{
				"other-namespace": nil, // Empty metadata
			},
		}

		var attrs map[string]interface{}
		if metadataContext != nil {
			dhMetadata := metadataContext.GetFilterMetadata()["test-namespace"]
			if dhMetadata != nil {
				attrs = dhMetadata.AsMap()
			}
		}

		assert.Nil(t, attrs)
	})
}

func TestConfig_MetadataNamespace(t *testing.T) {
	// Test default metadata namespace
	config := &Config{
		Server: ServerConfig{
			Port:              ":5051",
			SinkType:          "console",
			MetadataNamespace: "digitalhub",
		},
	}
	loadFromEnv(config)
	assert.Equal(t, "digitalhub", config.Server.MetadataNamespace)

	// Test environment variable override
	t.Setenv("METADATA_NAMESPACE", "custom-namespace")
	config2 := &Config{
		Server: ServerConfig{
			Port:              ":5051",
			SinkType:          "console",
			MetadataNamespace: "digitalhub",
		},
	}
	loadFromEnv(config2)
	assert.Equal(t, "custom-namespace", config2.Server.MetadataNamespace)
}
