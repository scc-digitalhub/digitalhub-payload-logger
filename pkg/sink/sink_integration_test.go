// SPDX-FileCopyrightText: © 2026 DSLab - Fondazione Bruno Kessler
//
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package sink

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
)

func createTestHeaderMap(headers map[string]string) *corev3.HeaderMap {
	hvs := make([]*corev3.HeaderValue, 0, len(headers))
	for k, v := range headers {
		hvs = append(hvs, &corev3.HeaderValue{Key: k, Value: v})
	}
	return &corev3.HeaderMap{Headers: hvs}
}

func TestPostgreSink_Integration(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgresContainer, err := postgres.Run(ctx,
		"timescale/timescaledb:latest-pg17",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	require.NoError(t, err)
	defer func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	}()

	// Get connection details
	host, err := postgresContainer.Host(ctx)
	require.NoError(t, err)
	port, err := postgresContainer.MappedPort(ctx, "5432")
	require.NoError(t, err)

	// Build database URL
	dbURL := fmt.Sprintf("postgres://testuser:testpass@%s:%s/testdb?sslmode=disable", host, port.Port())

	// Create database config
	dbConfig := &DatabaseConfig{
		URL:             dbURL,
		TableName:       "payloads",
		MaxRetries:      3,
		BatchSize:       1, // Flush immediately after each send
		FlushInterval:   time.Minute,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		MaxBodySize:     10 * 1024 * 1024, // 10MB
	}

	// Create the payloads table before creating the sink
	db, err := sql.Open("postgres", dbURL)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS payloads (
		service_name TEXT,
		function_name TEXT,
		project_name TEXT,
		request_time TIMESTAMP,
		request_duration BIGINT,
		response_status TEXT,
		request_body TEXT,
		response_body TEXT,
		request_path TEXT
		)
		WITH(
			timescaledb.hypertable,
			timescaledb.partition_column='request_time'
		);`)
	require.NoError(t, err)

	// Create PostgreSink
	sink, err := NewPostgreSink(dbConfig)
	require.NoError(t, err)
	defer sink.Close()

	// Test data
	testRequest := &RequestContext{
		RequestHeaders: createTestHeaderMap(map[string]string{
			"Content-Type":   "application/json",
			"x-service-name": "test-service",
		}),
		RequestBody:     []byte(`{"test": "request"}`),
		ResponseHeaders: createTestHeaderMap(map[string]string{":status": "200"}),
		ResponseBody:    []byte(`{"test": "response"}`),
		StartTime:       time.Now().UTC(),
		EndTime:         time.Now().Add(time.Second).UTC(),
	}

	// Test Send method
	err = sink.Send(*testRequest)
	require.NoError(t, err)

	// Verify data was written by querying the database
	var count int
	err = sink.DB.QueryRow("SELECT COUNT(*) FROM log_test_service").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify the data content
	var serviceName, functionName, projectName, responseStatus, requestPath string
	var requestBody, responseBody string
	var requestTime time.Time
	var requestDuration int64
	err = sink.DB.QueryRow("SELECT service_name, function_name, project_name, request_time, request_duration, response_status, request_body, response_body, request_path FROM log_test_service LIMIT 1").
		Scan(&serviceName, &functionName, &projectName, &requestTime, &requestDuration, &responseStatus, &requestBody, &responseBody, &requestPath)
	require.NoError(t, err)

	assert.Equal(t, "test-service", serviceName)
	assert.Equal(t, "test-service", functionName)
	assert.Equal(t, "_unknown_", projectName)
	assert.Equal(t, string(testRequest.RequestBody), requestBody)
	assert.Equal(t, string(testRequest.ResponseBody), responseBody)
	assert.Equal(t, "200", responseStatus)
	assert.True(t, requestTime.After(time.Now().Add(-time.Minute)))
	assert.True(t, requestTime.Add(time.Duration(requestDuration)*time.Millisecond).After(requestTime))
}

func TestPostgreSink_Integration_NewPostgreSink(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start PostgreSQL container
	pgContainer, err := postgres.Run(ctx,
		"timescale/timescaledb:latest-pg17",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	require.NoError(t, err)
	defer func() {
		if err := testcontainers.TerminateContainer(pgContainer); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	// Get connection string
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	// Create table
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS request_logs (
		service_name TEXT,
		function_name TEXT,
		project_name TEXT,
		request_time TIMESTAMP,
		request_duration BIGINT,
		response_status TEXT,
		request_body TEXT,
		response_body TEXT,
		request_path TEXT
	)WITH(
			timescaledb.hypertable,
			timescaledb.partition_column='request_time'
		);`)
	require.NoError(t, err)

	// Test successful creation
	config := &DatabaseConfig{
		URL:             connStr,
		TableName:       "request_logs",
		MaxRetries:      3,
		BatchSize:       10,
		FlushInterval:   time.Minute,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		MaxBodySize:     1024 * 1024,
	}

	sink, err := NewPostgreSink(config)
	require.NoError(t, err)
	require.NotNil(t, sink)
	assert.Equal(t, "request_logs", sink.TableName)
	assert.Equal(t, 10, sink.batchSize)
	assert.Equal(t, 1024*1024, sink.maxBodySize)

	// Test cleanup
	err = sink.Close()
	assert.NoError(t, err)
}

func TestPostgreSink_Integration_Send_Single(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start PostgreSQL container
	pgContainer, err := postgres.Run(ctx,
		"timescale/timescaledb:latest-pg17",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	require.NoError(t, err)
	defer func() {
		if err := testcontainers.TerminateContainer(pgContainer); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	// Get connection string
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	// Create table
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS request_logs (
			service_name TEXT,
			function_name TEXT,
			project_name TEXT,
			request_time TIMESTAMP,
			request_duration BIGINT,
			response_status TEXT,
			request_body TEXT,
			response_body TEXT,
			request_path TEXT
		)WITH(
			timescaledb.hypertable,
			timescaledb.partition_column='request_time'
		);
	`)
	require.NoError(t, err)

	// Test sink
	config := &DatabaseConfig{
		URL:             connStr,
		TableName:       "request_logs",
		MaxRetries:      3,
		BatchSize:       1, // Immediate flush
		FlushInterval:   time.Minute,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		MaxBodySize:     1024 * 1024,
	}

	sink, err := NewPostgreSink(config)
	require.NoError(t, err)
	defer sink.Close()

	// Create test request context
	startTime := time.Now().UTC()
	endTime := startTime.Add(100 * time.Millisecond)

	reqCtx := RequestContext{
		RequestHeaders: createTestHeaderMap(map[string]string{
			"x-service-name": "test-service",
			":path":          "/api/test",
			":status":        "200",
		}),
		ResponseHeaders: createTestHeaderMap(map[string]string{
			":status": "200",
		}),
		RequestBody:  []byte(`{"input": "test"}`),
		ResponseBody: []byte(`{"output": "result"}`),
		StartTime:    startTime,
		EndTime:      endTime,
	}

	// Send data
	err = sink.Send(reqCtx)
	require.NoError(t, err)

	// Give some time for async operations
	time.Sleep(200 * time.Millisecond)

	// Verify data was inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM log_test_service").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify data content
	var serviceName, functionName, projectName, responseStatus, requestPath string
	var requestBody, responseBody string
	var requestStart time.Time
	var requestDuration int64

	err = db.QueryRow(`
		SELECT service_name, function_name, project_name, request_time, request_duration, response_status,
			   request_body, response_body, request_path
		FROM log_test_service LIMIT 1
	`).Scan(&serviceName, &functionName, &projectName, &requestStart, &requestDuration, &responseStatus,
		&requestBody, &responseBody, &requestPath)

	require.NoError(t, err)
	assert.Equal(t, "test-service", serviceName)
	assert.Equal(t, "test-service", functionName)
	assert.Equal(t, "_unknown_", projectName)
	assert.Equal(t, "200", responseStatus)
	assert.Equal(t, `{"input": "test"}`, requestBody)
	assert.Equal(t, `{"output": "result"}`, responseBody)
	assert.Equal(t, "/api/test", requestPath)
	// Check timestamps are within reasonable range
	startDiff := requestStart.Sub(startTime)
	endDiff := (requestStart.Add(time.Duration(requestDuration) * time.Millisecond)).Sub(endTime)

	// Allow for some precision loss in database storage
	assert.True(t, startDiff >= -time.Second && startDiff <= time.Second, "requestStart should be close to startTime")
	assert.True(t, endDiff >= -time.Second && endDiff <= time.Second, "requestEnd should be close to endTime")
}

func TestPostgreSink_Integration_Send_Batch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start PostgreSQL container
	pgContainer, err := postgres.Run(ctx,
		"timescale/timescaledb:latest-pg17",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	require.NoError(t, err)
	defer func() {
		if err := testcontainers.TerminateContainer(pgContainer); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	// Get connection string
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	// Create table
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS request_logs (
			service_name TEXT,
			function_name TEXT,
			project_name TEXT,
			request_time TIMESTAMP,
			request_duration BIGINT,
			response_status TEXT,
			request_body TEXT,
			response_body TEXT,
			request_path TEXT
		)WITH(
			timescaledb.hypertable,
			timescaledb.partition_column='request_time'
		);
	`)
	require.NoError(t, err)

	// Test batch insert
	config := &DatabaseConfig{
		URL:             connStr,
		TableName:       "request_logs",
		MaxRetries:      3,
		BatchSize:       3,                     // Batch of 3
		FlushInterval:   50 * time.Millisecond, // Short flush interval
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		MaxBodySize:     1024 * 1024,
	}

	sink, err := NewPostgreSink(config)
	require.NoError(t, err)
	defer sink.Close()

	// Send multiple requests
	baseTime := time.Now()
	for i := 0; i < 5; i++ {
		reqCtx := RequestContext{
			RequestHeaders: createTestHeaderMap(map[string]string{
				"x-service-name": fmt.Sprintf("service-%d", i),
				":path":          fmt.Sprintf("/api/test/%d", i),
				":status":        "200",
			}),
			ResponseHeaders: createTestHeaderMap(map[string]string{
				":status": "200",
			}),
			RequestBody:  []byte(fmt.Sprintf(`{"input": "test-%d"}`, i)),
			ResponseBody: []byte(fmt.Sprintf(`{"output": "result-%d"}`, i)),
			StartTime:    baseTime.Add(time.Duration(i) * time.Millisecond),
			EndTime:      baseTime.Add(time.Duration(i+1) * time.Millisecond),
		}

		err = sink.Send(reqCtx)
		require.NoError(t, err)
	}

	// Wait for flush
	time.Sleep(200 * time.Millisecond)

	// Verify data was inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM log_service_1").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	expectedServices := []string{"service-0", "service-1", "service-2", "service-3", "service-4"}
	for i := 0; i < 5; i++ {
		// Verify specific records
		rows, err := db.Query(fmt.Sprintf(`SELECT service_name, request_body FROM log_service_%d ORDER BY service_name`, i))
		require.NoError(t, err)
		defer rows.Close()

		var serviceName string
		var requestBody string
		rows.Next()
		err = rows.Scan(&serviceName, &requestBody)
		require.NoError(t, err)
		assert.Equal(t, expectedServices[i], serviceName)
		assert.Equal(t, fmt.Sprintf(`{"input": "test-%d"}`, i), requestBody)
	}
}

func TestPostgreSink_Integration_Send_BodyTruncation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start PostgreSQL container
	pgContainer, err := postgres.Run(ctx,
		"timescale/timescaledb:latest-pg17",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	require.NoError(t, err)
	defer func() {
		if err := testcontainers.TerminateContainer(pgContainer); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	// Get connection string
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	// Create table
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS request_logs (
			service_name TEXT,
			function_name TEXT,
			project_name TEXT,
			request_time TIMESTAMP,
			request_duration BIGINT,
			response_status TEXT,
			request_body TEXT,
			response_body TEXT,
			request_path TEXT
		)WITH(
			timescaledb.hypertable,
			timescaledb.partition_column='request_time'
		);
	`)
	require.NoError(t, err)

	// Test with small body size limit
	config := &DatabaseConfig{
		URL:             connStr,
		TableName:       "request_logs",
		MaxRetries:      3,
		BatchSize:       1,
		FlushInterval:   time.Minute,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		MaxBodySize:     20, // Very small limit
	}

	sink, err := NewPostgreSink(config)
	require.NoError(t, err)
	defer sink.Close()

	// Create large request/response bodies
	largeRequestBody := make([]byte, 100)  // Larger than limit
	largeResponseBody := make([]byte, 100) // Larger than limit
	for i := range largeRequestBody {
		largeRequestBody[i] = 'a'
	}
	for i := range largeResponseBody {
		largeResponseBody[i] = 'b'
	}

	reqCtx := RequestContext{
		RequestHeaders: createTestHeaderMap(map[string]string{
			"x-service-name": "test-service",
			":path":          "/api/test",
			":status":        "200",
		}),
		ResponseHeaders: createTestHeaderMap(map[string]string{
			":status": "200",
		}),
		RequestBody:  largeRequestBody,
		ResponseBody: largeResponseBody,
		StartTime:    time.Now(),
		EndTime:      time.Now().Add(time.Millisecond),
	}

	// Send data
	err = sink.Send(reqCtx)
	require.NoError(t, err)

	// Give some time for async operations
	time.Sleep(200 * time.Millisecond)

	// Verify data was inserted with truncated bodies
	var requestBody, responseBody string
	err = db.QueryRow("SELECT request_body, response_body FROM log_test_service LIMIT 1").
		Scan(&requestBody, &responseBody)
	require.NoError(t, err)

	// Bodies should be truncated to MaxBodySize
	assert.Len(t, requestBody, 20)
	assert.Len(t, responseBody, 20)
	assert.Equal(t, string(largeRequestBody[:20]), requestBody)
	assert.Equal(t, string(largeResponseBody[:20]), responseBody)
}

func TestPostgreSink_Integration_ErrorHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start PostgreSQL container
	pgContainer, err := postgres.Run(ctx,
		"timescale/timescaledb:latest-pg17",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	require.NoError(t, err)
	defer func() {
		if err := testcontainers.TerminateContainer(pgContainer); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	// Get connection string
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	// Test with invalid table name (table doesn't exist)
	config := &DatabaseConfig{
		URL:             connStr,
		TableName:       "nonexistent_table",
		MaxRetries:      3,
		BatchSize:       1,
		FlushInterval:   time.Minute,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		MaxBodySize:     1024 * 1024,
	}

	sink, err := NewPostgreSink(config)
	// This should succeed because tables are created automatically
	require.NoError(t, err)
	assert.NotNil(t, sink)
	assert.Equal(t, "nonexistent_table", sink.TableName)

	// Clean up
	err = sink.Close()
	assert.NoError(t, err)
}

func TestPostgreSink_Integration_Close(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start PostgreSQL container
	pgContainer, err := postgres.Run(ctx,
		"timescale/timescaledb:latest-pg17",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	require.NoError(t, err)
	defer func() {
		if err := testcontainers.TerminateContainer(pgContainer); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	// Get connection string
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	// Create table
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS request_logs (
		service_name TEXT,
		function_name TEXT,
		project_name TEXT,
		request_time TIMESTAMP,
		request_duration BIGINT,
		response_status TEXT,
		request_body TEXT,
		response_body TEXT,
		request_path TEXT
	)WITH(
			timescaledb.hypertable,
			timescaledb.partition_column='request_time'
		);`)
	require.NoError(t, err)

	// Create sink
	config := &DatabaseConfig{
		URL:             connStr,
		TableName:       "request_logs",
		MaxRetries:      3,
		BatchSize:       10,
		FlushInterval:   time.Minute,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		MaxBodySize:     1024 * 1024,
	}

	sink, err := NewPostgreSink(config)
	require.NoError(t, err)

	// Close should succeed
	err = sink.Close()
	assert.NoError(t, err)

	// Multiple closes should be safe
	err = sink.Close()
	assert.NoError(t, err)
}
