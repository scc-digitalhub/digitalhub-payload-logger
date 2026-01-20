// SPDX-FileCopyrightText: © 2026 DSLab - Fondazione Bruno Kessler
//
// SPDX-License-Identifier: Apache-2.0

//go:build !integration

package sink

import (
	"fmt"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
)

func createTestHeaderMap(headers map[string]string) *corev3.HeaderMap {
	hvs := make([]*corev3.HeaderValue, 0, len(headers))
	for k, v := range headers {
		hvs = append(hvs, &corev3.HeaderValue{Key: k, Value: v})
	}
	return &corev3.HeaderMap{Headers: hvs}
}

func TestExtractValueFromHeaders(t *testing.T) {
	tests := []struct {
		name         string
		headers      map[string]string
		keys         []string
		defaultValue string
		expected     string
	}{
		{
			name:         "first key found",
			headers:      map[string]string{"key1": "value1", "key2": "value2"},
			keys:         []string{"key1", "key2"},
			defaultValue: "default",
			expected:     "value1",
		},
		{
			name:         "second key found",
			headers:      map[string]string{"key2": "value2"},
			keys:         []string{"key1", "key2"},
			defaultValue: "default",
			expected:     "value2",
		},
		{
			name:         "no key found",
			headers:      map[string]string{"key3": "value3"},
			keys:         []string{"key1", "key2"},
			defaultValue: "default",
			expected:     "default",
		},
		{
			name:         "nil headers",
			headers:      nil,
			keys:         []string{"key1"},
			defaultValue: "default",
			expected:     "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var headers *corev3.HeaderMap
			if tt.headers != nil {
				headers = createTestHeaderMap(tt.headers)
			}

			result := extractValueFromHeaders(headers, tt.keys, tt.defaultValue)
			if result != tt.expected {
				t.Errorf("extractValueFromHeaders() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestExtractServiceName(t *testing.T) {
	ctx := &RequestContext{}

	// Test with headers
	ctx.RequestHeaders = createTestHeaderMap(map[string]string{"x-service-name": "test-service"})
	result := ctx.ExtractServiceName()
	if result != "test-service" {
		t.Errorf("ExtractServiceName() = %v, want test-service", result)
	}

	// Test without headers
	ctx.RequestHeaders = nil
	result = ctx.ExtractServiceName()
	if result != "_unknown_" {
		t.Errorf("ExtractServiceName() = %v, want _unknown_", result)
	}
}

func TestExtractStatus(t *testing.T) {
	ctx := &RequestContext{}

	// Test with status header
	ctx.ResponseHeaders = createTestHeaderMap(map[string]string{":status": "404"})
	result := ctx.ExtractStatus()
	if result != "404" {
		t.Errorf("ExtractStatus() = %v, want 404", result)
	}

	// Test without headers
	ctx.ResponseHeaders = nil
	result = ctx.ExtractStatus()
	if result != "200" {
		t.Errorf("ExtractStatus() = %v, want 200", result)
	}
}

func TestExtractPath(t *testing.T) {
	ctx := &RequestContext{}

	// Test with path header
	ctx.RequestHeaders = createTestHeaderMap(map[string]string{":path": "/api/v1"})
	result := ctx.ExtractPath()
	if result != "/api/v1" {
		t.Errorf("ExtractPath() = %v, want /api/v1", result)
	}

	// Test without headers
	ctx.RequestHeaders = nil
	result = ctx.ExtractPath()
	if result != "/" {
		t.Errorf("ExtractPath() = %v, want /", result)
	}
}

func TestRequestContext(t *testing.T) {
	start := time.Now()
	end := start.Add(time.Second)

	ctx := &RequestContext{
		Service:         "test-service",
		RequestHeaders:  createTestHeaderMap(map[string]string{"host": "example.com"}),
		RequestBody:     []byte("request body"),
		ResponseHeaders: createTestHeaderMap(map[string]string{":status": "200"}),
		ResponseBody:    []byte("response body"),
		StartTime:       start,
		EndTime:         end,
	}

	if ctx.Service != "test-service" {
		t.Errorf("Service = %v, want test-service", ctx.Service)
	}

	if string(ctx.RequestBody) != "request body" {
		t.Errorf("RequestBody = %v, want request body", string(ctx.RequestBody))
	}

	if string(ctx.ResponseBody) != "response body" {
		t.Errorf("ResponseBody = %v, want response body", string(ctx.ResponseBody))
	}

	if !ctx.StartTime.Equal(start) {
		t.Errorf("StartTime = %v, want %v", ctx.StartTime, start)
	}

	if !ctx.EndTime.Equal(end) {
		t.Errorf("EndTime = %v, want %v", ctx.EndTime, end)
	}
}

func TestConsoleSink_NewConsoleSink(t *testing.T) {
	maxLogBodySize := 1024
	sink, err := NewConsoleSink(maxLogBodySize)
	if err != nil {
		t.Fatalf("NewConsoleSink() error = %v", err)
	}
	if sink == nil {
		t.Fatal("NewConsoleSink() returned nil sink")
	}
	if sink.maxLogBodySize != maxLogBodySize {
		t.Errorf("maxLogBodySize = %v, want %v", sink.maxLogBodySize, maxLogBodySize)
	}
}

func TestConsoleSink_Send(t *testing.T) {
	sink, err := NewConsoleSink(100)
	if err != nil {
		t.Fatalf("NewConsoleSink() error = %v", err)
	}

	ctx := RequestContext{
		RequestHeaders:  createTestHeaderMap(map[string]string{"x-service-name": "test-service"}),
		RequestBody:     []byte("short request"),
		ResponseHeaders: createTestHeaderMap(map[string]string{":status": "200"}),
		ResponseBody:    []byte("short response"),
		StartTime:       time.Now(),
		EndTime:         time.Now().Add(time.Millisecond),
	}

	// This should not return an error
	err = sink.Send(ctx)
	if err != nil {
		t.Errorf("ConsoleSink.Send() error = %v", err)
	}
}

func TestConsoleSink_Send_WithTruncation(t *testing.T) {
	sink, err := NewConsoleSink(10) // Very small limit
	if err != nil {
		t.Fatalf("NewConsoleSink() error = %v", err)
	}

	ctx := RequestContext{
		RequestHeaders:  createTestHeaderMap(map[string]string{"x-service-name": "test-service"}),
		RequestBody:     []byte("this is a very long request body that should be truncated"),
		ResponseHeaders: createTestHeaderMap(map[string]string{":status": "200"}),
		ResponseBody:    []byte("this is a very long response body that should be truncated"),
		StartTime:       time.Now(),
		EndTime:         time.Now().Add(time.Millisecond),
	}

	// This should not return an error even with truncation
	err = sink.Send(ctx)
	if err != nil {
		t.Errorf("ConsoleSink.Send() error = %v", err)
	}
}

func TestConsoleSink_Close(t *testing.T) {
	sink, err := NewConsoleSink(1024)
	if err != nil {
		t.Fatalf("NewConsoleSink() error = %v", err)
	}

	// Close should not return an error
	err = sink.Close()
	if err != nil {
		t.Errorf("ConsoleSink.Close() error = %v", err)
	}
}

func TestFallbackSink_NewFallbackSink(t *testing.T) {
	primary, err := NewConsoleSink(1024)
	if err != nil {
		t.Fatalf("NewConsoleSink() error = %v", err)
	}
	fallback, err := NewConsoleSink(512)
	if err != nil {
		t.Fatalf("NewConsoleSink() error = %v", err)
	}

	fallbackSink := NewFallbackSink(primary, fallback)
	if fallbackSink == nil {
		t.Fatal("NewFallbackSink() returned nil")
	}
	if fallbackSink.Primary != primary {
		t.Error("Primary sink not set correctly")
	}
	if fallbackSink.Fallback != fallback {
		t.Error("Fallback sink not set correctly")
	}
}

func TestFallbackSink_Send_PrimarySuccess(t *testing.T) {
	primary, _ := NewConsoleSink(1024)
	fallback, _ := NewConsoleSink(512)
	fallbackSink := NewFallbackSink(primary, fallback)

	ctx := RequestContext{
		RequestHeaders: createTestHeaderMap(map[string]string{"x-service-name": "test"}),
		StartTime:      time.Now(),
		EndTime:        time.Now().Add(time.Millisecond),
	}

	err := fallbackSink.Send(ctx)
	if err != nil {
		t.Errorf("FallbackSink.Send() error = %v", err)
	}
}

func TestFallbackSink_Send_PrimaryFailsFallbackSuccess(t *testing.T) {
	// Create a mock sink that always fails
	primary := &mockSink{shouldFail: true}
	fallback, _ := NewConsoleSink(512)
	fallbackSink := NewFallbackSink(primary, fallback)

	ctx := RequestContext{
		RequestHeaders: createTestHeaderMap(map[string]string{"x-service-name": "test"}),
		StartTime:      time.Now(),
		EndTime:        time.Now().Add(time.Millisecond),
	}

	// Should succeed with fallback
	err := fallbackSink.Send(ctx)
	if err != nil {
		t.Errorf("FallbackSink.Send() error = %v", err)
	}
}

func TestFallbackSink_Send_BothFail(t *testing.T) {
	primary := &mockSink{shouldFail: true}
	fallback := &mockSink{shouldFail: true}
	fallbackSink := NewFallbackSink(primary, fallback)

	ctx := RequestContext{
		RequestHeaders: createTestHeaderMap(map[string]string{"x-service-name": "test"}),
		StartTime:      time.Now(),
		EndTime:        time.Now().Add(time.Millisecond),
	}

	err := fallbackSink.Send(ctx)
	if err == nil {
		t.Error("FallbackSink.Send() should have failed when both sinks fail")
	}
}

func TestFallbackSink_Close(t *testing.T) {
	primary, _ := NewConsoleSink(1024)
	fallback, _ := NewConsoleSink(512)
	fallbackSink := NewFallbackSink(primary, fallback)

	err := fallbackSink.Close()
	if err != nil {
		t.Errorf("FallbackSink.Close() error = %v", err)
	}
}

func TestFallbackSink_Close_PrimaryFails(t *testing.T) {
	primary := &mockSink{closeShouldFail: true}
	fallback, _ := NewConsoleSink(512)
	fallbackSink := NewFallbackSink(primary, fallback)

	err := fallbackSink.Close()
	if err == nil {
		t.Error("FallbackSink.Close() should have failed when primary close fails")
	}
}

// mockSink is a test helper that implements the Sink interface
type mockSink struct {
	shouldFail      bool
	closeShouldFail bool
}

func (m *mockSink) Send(ctx RequestContext) error {
	if m.shouldFail {
		return fmt.Errorf("mock sink failure")
	}
	return nil
}

func (m *mockSink) Close() error {
	if m.closeShouldFail {
		return fmt.Errorf("mock close failure")
	}
	return nil
}

func TestPostgreSink_NewPostgreSink_InvalidURL(t *testing.T) {
	config := &DatabaseConfig{
		URL:         "",
		TableName:   "test_table",
		MaxRetries:  3,
		BatchSize:   10,
		MaxBodySize: 1024,
	}

	sink, err := NewPostgreSink(config)
	if err == nil {
		t.Error("NewPostgreSink() should have failed with empty URL")
	}
	if sink != nil {
		t.Error("NewPostgreSink() should have returned nil sink")
	}
}

func TestPostgreSink_Send_BodyTruncation(t *testing.T) {
	// This test would require mocking the database, but for now we'll test the logic
	// by checking that the method exists and can be called with a mock setup
	// In a real scenario, we'd use dependency injection with interfaces
	t.Skip("PostgreSink tests require database mocking - skipping for now")
}
