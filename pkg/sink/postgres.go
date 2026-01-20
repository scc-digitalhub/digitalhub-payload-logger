// SPDX-FileCopyrightText: © 2026 DSLab - Fondazione Bruno Kessler
//
// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// PostgreSink implements Sink by writing to PostgreSQL.
// It inserts request context data into a specified table.
type PostgreSink struct {
	DB          *sql.DB              // Database connection
	TableName   string               // Default name of the table to insert into
	MaxRetries  int                  // Maximum number of retries for failed operations
	stmts       map[string]*sql.Stmt // Prepared statements for inserts keyed by table name
	batchSize   int                  // Size of batch for inserts
	batch       []batchItem          // Current batch of contexts with table names
	mu          sync.Mutex           // Mutex for batch access
	ticker      *time.Ticker         // Ticker for periodic flushing
	done        chan struct{}        // Channel to stop the flusher
	closeOnce   sync.Once            // Ensures Close is called only once
	maxBodySize int                  // Maximum body size to store
}

// NewPostgreSink creates a new PostgreSink instance.
// It establishes a database connection using DATABASE_URL environment variable.
func NewPostgreSink(dbConfig *DatabaseConfig) (*PostgreSink, error) {
	connStr := dbConfig.URL
	if connStr == "" {
		return nil, fmt.Errorf("DATABASE_URL not set")
	}
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	// Configure connection pooling
	db.SetMaxOpenConns(dbConfig.MaxOpenConns)
	db.SetMaxIdleConns(dbConfig.MaxIdleConns)
	db.SetConnMaxLifetime(dbConfig.ConnMaxLifetime)

	// Prepare statement for default table
	stmts := make(map[string]*sql.Stmt)

	sink := &PostgreSink{
		DB:          db,
		TableName:   dbConfig.TableName,
		MaxRetries:  dbConfig.MaxRetries,
		stmts:       stmts,
		batchSize:   dbConfig.BatchSize,
		batch:       make([]batchItem, 0, dbConfig.BatchSize),
		done:        make(chan struct{}),
		maxBodySize: dbConfig.MaxBodySize,
	}

	// Start periodic flusher
	sink.ticker = time.NewTicker(dbConfig.FlushInterval)
	go sink.flusher()

	return sink, nil
}

// getStmt returns a prepared statement for the given table name, creating it if necessary.
func (p *PostgreSink) getStmt(tableName string) (*sql.Stmt, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if stmt, exists := p.stmts[tableName]; exists {
		return stmt, nil
	}

	// Check if table exists, create if not
	if err := p.ensureTableExists(tableName); err != nil {
		return nil, fmt.Errorf("failed to ensure table %s exists: %w", tableName, err)
	}

	// Prepare new statement for this table
	stmt, err := p.DB.Prepare(`INSERT INTO ` + tableName + ` (service_name, request_time, request_duration, response_status, request_body, response_body, request_path) VALUES ($1, $2, $3, $4, $5, $6, $7)`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement for table %s: %w", tableName, err)
	}

	p.stmts[tableName] = stmt
	return stmt, nil
}

// ensureTableExists checks if the table exists and creates it if it doesn't.
func (p *PostgreSink) ensureTableExists(tableName string) error {
	// Check if table exists
	var exists bool
	err := p.DB.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = 'public'
			AND table_name = $1
		)`, tableName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	if !exists {
		// Create the table
		createTableSQL := `
			CREATE TABLE ` + tableName + ` (
				service_name TEXT,
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
			);`
		_, err := p.DB.Exec(createTableSQL)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}
		log.Printf("Created table %s", tableName)
	}

	return nil
}

// batchItem holds a request context with its target table name
type batchItem struct {
	ctx       RequestContext
	tableName string
}

// Send writes the RequestContext to PostgreSQL table.
// The table must have columns: service_name, request_time, request_duration,
// response_status, request_body, response_body, request_path
func (p *PostgreSink) Send(ctx RequestContext) error {
	ctx.Service = ctx.ExtractServiceName()
	tableName := extractTableName(ctx.Service, p.TableName)

	// Ensure statement exists for this table
	_, err := p.getStmt(tableName)
	if err != nil {
		return fmt.Errorf("failed to get statement for table %s: %w", tableName, err)
	}

	// For memory management, limit body sizes
	if len(ctx.RequestBody) > p.maxBodySize {
		ctx.RequestBody = ctx.RequestBody[:p.maxBodySize]
		log.Printf("Truncated request body for service %s", ctx.Service)
	}
	if len(ctx.ResponseBody) > p.maxBodySize {
		ctx.ResponseBody = ctx.ResponseBody[:p.maxBodySize]
		log.Printf("Truncated response body for service %s", ctx.Service)
	}

	p.mu.Lock()
	p.batch = append(p.batch, batchItem{ctx: ctx, tableName: tableName})
	shouldFlush := len(p.batch) >= p.batchSize
	p.mu.Unlock()

	if shouldFlush {
		return p.flushBatch()
	}
	return nil
}

func extractTableName(serviceName, fallbackTableName string) string {
	if serviceName != "" && serviceName != "_unknown_" {
		return strings.Replace(serviceName, "-", "_", -1)
	}
	return fallbackTableName
}

// flushBatch inserts the current batch of contexts into the database.
func (p *PostgreSink) flushBatch() error {
	p.mu.Lock()
	batch := make([]batchItem, len(p.batch))
	copy(batch, p.batch)
	p.batch = p.batch[:0] // Clear batch
	p.mu.Unlock()

	if len(batch) == 0 {
		return nil
	}

	// Group batch by table name
	tableBatches := make(map[string][]RequestContext)
	for _, item := range batch {
		tableBatches[item.tableName] = append(tableBatches[item.tableName], item.ctx)
	}

	// Process each table batch
	for tableName, contexts := range tableBatches {
		if err := p.flushTableBatch(tableName, contexts); err != nil {
			return fmt.Errorf("failed to flush batch for table %s: %w", tableName, err)
		}
	}

	return nil
}

// flushTableBatch inserts a batch of contexts into a specific table.
func (p *PostgreSink) flushTableBatch(tableName string, contexts []RequestContext) error {
	// Use transaction for batch insert
	tx, err := p.DB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Get prepared statement for this table
	stmt, err := p.getStmt(tableName)
	if err != nil {
		return err
	}

	// Prepare statement in transaction
	txStmt := tx.Stmt(stmt)

	for _, ctx := range contexts {
		status := ctx.ExtractStatus()
		path := ctx.ExtractPath()
		durationMs := ctx.EndTime.Sub(ctx.StartTime).Milliseconds()
		_, err := txStmt.Exec(ctx.Service, ctx.StartTime, durationMs, status, string(ctx.RequestBody), string(ctx.ResponseBody), path)
		if err != nil {
			return fmt.Errorf("failed to insert batch item: %w", err)
		}
	}

	return tx.Commit()
}

// flusher runs in a goroutine to periodically flush the batch.
func (p *PostgreSink) flusher() {
	for {
		select {
		case <-p.ticker.C:
			if err := p.flushBatch(); err != nil {
				log.Printf("Error flushing batch: %v", err)
			}
		case <-p.done:
			// Final flush on shutdown
			p.ticker.Stop()
			if err := p.flushBatch(); err != nil {
				log.Printf("Error final flush: %v", err)
			}
			return
		}
	}
}

// Close closes the database connection and stops the flusher.
func (p *PostgreSink) Close() error {
	p.closeOnce.Do(func() {
		close(p.done) // Stop flusher
		for _, stmt := range p.stmts {
			if stmt != nil {
				stmt.Close()
			}
		}
		p.DB.Close()
	})
	return nil
}
