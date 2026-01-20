// SPDX-FileCopyrightText: © 2026 DSLab - Fondazione Bruno Kessler
//
// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"log"
)

// ConsoleSink implements Sink by printing to console.
// It logs the complete request context information.
type ConsoleSink struct {
	maxLogBodySize int
}

// NewConsoleSink creates a new ConsoleSink instance.
func NewConsoleSink(maxLogBodySize int) (*ConsoleSink, error) {
	return &ConsoleSink{maxLogBodySize: maxLogBodySize}, nil
}

// Send prints the RequestContext to the console with detailed logging.
func (c *ConsoleSink) Send(ctx RequestContext) error {
	ctx.Service = ctx.ExtractServiceName()

	// For memory management, limit body sizes for logging
	reqBody := string(ctx.RequestBody)
	if len(reqBody) > c.maxLogBodySize {
		reqBody = reqBody[:c.maxLogBodySize] + "... (truncated)"
	}
	respBody := string(ctx.ResponseBody)
	if len(respBody) > c.maxLogBodySize {
		respBody = respBody[:c.maxLogBodySize] + "... (truncated)"
	}

	log.Printf("Complete Request Context:")
	log.Printf("  Service: %s", ctx.Service)
	log.Printf("  Start Time: %v", ctx.StartTime)
	log.Printf("  End Time: %v", ctx.EndTime)
	log.Printf("  Duration: %v", ctx.EndTime.Sub(ctx.StartTime))
	log.Printf("  Request Headers: %v", ctx.RequestHeaders)
	log.Printf("  Request Body: %s", reqBody)
	log.Printf("  Response Headers: %v", ctx.ResponseHeaders)
	log.Printf("  Response Body: %s", respBody)
	return nil
}

// Close is a no-op for ConsoleSink.
func (c *ConsoleSink) Close() error {
	return nil
}
