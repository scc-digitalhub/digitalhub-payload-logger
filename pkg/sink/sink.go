// SPDX-FileCopyrightText: © 2026 DSLab - Fondazione Bruno Kessler
//
// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
)

// DatabaseConfig holds database configuration
type DatabaseConfig struct {
	URL             string
	TableName       string
	MaxRetries      int
	BatchSize       int
	FlushInterval   time.Duration
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	MaxBodySize     int
}

// RequestContext holds the complete context of an HTTP request/response cycle
// captured by the Envoy External Processor.
type RequestContext struct {
	Service         string            // Extracted service name
	Function        string            // Extracted function name
	Project         string            // Extracted project name
	RequestHeaders  *corev3.HeaderMap // Request headers from Envoy
	RequestBody     []byte            // Request body content
	ResponseHeaders *corev3.HeaderMap // Response headers from Envoy
	ResponseBody    []byte            // Response body content
	StartTime       time.Time         // When request processing started
	EndTime         time.Time         // When request processing ended
	Metadata        map[string]any    // Additional metadata if needed
}

var nameHeaders = []string{"x-ai-eg-model", "x-service-name"}
var statusHeaders = []string{":status", "status"}
var pathHeaders = []string{":path", "path"}

// extractValueFromHeaders is a helper function that extracts the first matching header value
// from the given header map using the provided keys. Returns defaultValue if not found.
func extractValueFromHeaders(headers *corev3.HeaderMap, keys []string, defaultValue string) string {
	if headers == nil {
		return defaultValue
	}
	for _, hv := range headers.Headers {
		for _, key := range keys {
			if hv.Key == key {
				return hv.Value
			}
		}
	}
	return defaultValue
}

// ExtractServiceName extracts the service name from metadata or request headers.
// Checks for 'service_name' metadata first, then falls back to common service name headers and returns "_unknown_" if not found.
func (ctx *RequestContext) ExtractServiceName() string {
	// first check metadata
	if ctx.Metadata != nil {
		if val, ok := ctx.Metadata["service_name"]; ok {
			if strVal, ok := val.(string); ok && strVal != "" {
				return strVal
			}
		}
	}
	// fallback to headers
	return extractValueFromHeaders(ctx.RequestHeaders, nameHeaders, "_unknown_")
}

// ExtractFunctionName extracts the function name from metadata or request headers.
// Checks for 'function_name' metadata first, then falls back to common service name headers and returns "_unknown_" if not found.
func (ctx *RequestContext) ExtractFunctionName() string {
	// first check metadata
	if ctx.Metadata != nil {
		if val, ok := ctx.Metadata["function_name"]; ok {
			if strVal, ok := val.(string); ok && strVal != "" {
				return strVal
			}
		}
	}
	// fallback
	return ctx.ExtractServiceName()
}

// ExtractProjectName extracts the project name from metadata.
// Returns "_unknown_" if not found.
func (ctx *RequestContext) ExtractProjectName() string {
	// first check metadata
	if ctx.Metadata != nil {
		if val, ok := ctx.Metadata["project_name"]; ok {
			if strVal, ok := val.(string); ok && strVal != "" {
				return strVal
			}
		}
	}
	// fallback
	return "_unknown_"
}

// ExtractStatus extracts the HTTP response status code from response headers.
// Returns "200" as default if status header is not found.
func (ctx *RequestContext) ExtractStatus() string {
	return extractValueFromHeaders(ctx.ResponseHeaders, statusHeaders, "200")
}

// ExtractPath extracts the request path from request headers.
// Returns "/" as default if path header is not found.
func (ctx *RequestContext) ExtractPath() string {
	return extractValueFromHeaders(ctx.RequestHeaders, pathHeaders, "/")
}

// SetAttributes sets additional attributes from the provided map into the Metadata field.
func (ctx *RequestContext) SetAttributes(attrs map[string]any) {
	if ctx.Metadata == nil {
		ctx.Metadata = make(map[string]any)
	}
	for key, val := range attrs {
		ctx.Metadata[key] = val
	}
}

// Sink interface defines the method for sending request context data.
// Implementations can write to different destinations like console, database, etc.
type Sink interface {
	Send(ctx RequestContext) error
	Close() error
}
