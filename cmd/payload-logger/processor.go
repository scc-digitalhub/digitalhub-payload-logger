// SPDX-FileCopyrightText: © 2026 DSLab - Fondazione Bruno Kessler
//
// SPDX-License-Identifier: Apache-2.0

// Package main implements the Envoy External Processor server for payload logging.
// It processes HTTP requests and responses, accumulating context data and sending
// it to configured sinks (console, database, etc.).
package main

import (
	"fmt"
	"sync"
	"time"

	extproc "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/scc-digitalhub/payload-logger/pkg/sink"
)

// Type aliases for sink types to avoid import conflicts
type RequestContext = sink.RequestContext
type Sink = sink.Sink

// server implements the ExternalProcessorServer interface.
// It handles bidirectional streaming RPC calls from Envoy.
type server struct {
	extproc.UnimplementedExternalProcessorServer
	sink             Sink                // Configured sink for outputting request data
	ctxChan          chan RequestContext // Channel for asynchronous processing
	wg               sync.WaitGroup      // Wait group for workers
	numWorkers       int                 // Number of worker goroutines
	contextNamespace string              // Namespace for context metadata
}

// Process handles the bidirectional streaming RPC from Envoy.
// It accumulates request context data across multiple messages and sends
// the complete context to the configured sink when the stream ends.
func (s *server) Process(stream extproc.ExternalProcessor_ProcessServer) error {
	ctx := &RequestContext{
		StartTime: time.Now(),
	}
	defer func() {
		ctx.EndTime = time.Now()
		// Send to channel for asynchronous processing
		select {
		case s.ctxChan <- *ctx:
		default:
			// Channel full, log error but don't block
			fmt.Printf("Error: context channel full, dropping request\n")
		}
	}()

	for {
		req, err := stream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				return nil
			}
			return err
		}
		// Update context and create response based on request type
		var resp *extproc.ProcessingResponse

		// Extract metadata if present
		if req.MetadataContext != nil {
			dhMetadata := req.MetadataContext.GetFilterMetadata()[s.contextNamespace]
			if dhMetadata != nil {
				ctx.SetAttributes(dhMetadata.AsMap())
			}
		}

		switch r := req.Request.(type) {
		case *extproc.ProcessingRequest_RequestHeaders:
			ctx.RequestHeaders = r.RequestHeaders.Headers
			if ctx.StartTime.IsZero() {
				ctx.StartTime = time.Now()
			}
			resp = &extproc.ProcessingResponse{
				Response: &extproc.ProcessingResponse_RequestHeaders{
					RequestHeaders: &extproc.HeadersResponse{},
				},
			}
		case *extproc.ProcessingRequest_RequestBody:
			ctx.RequestBody = r.RequestBody.Body
			if ctx.StartTime.IsZero() {
				ctx.StartTime = time.Now()
			}
			resp = &extproc.ProcessingResponse{
				Response: &extproc.ProcessingResponse_RequestBody{
					RequestBody: &extproc.BodyResponse{},
				},
			}
		case *extproc.ProcessingRequest_ResponseHeaders:
			ctx.ResponseHeaders = r.ResponseHeaders.Headers
			if ctx.EndTime.IsZero() {
				ctx.EndTime = time.Now()
			}
			resp = &extproc.ProcessingResponse{
				Response: &extproc.ProcessingResponse_ResponseHeaders{
					ResponseHeaders: &extproc.HeadersResponse{},
				},
			}
		case *extproc.ProcessingRequest_ResponseBody:
			ctx.ResponseBody = r.ResponseBody.Body
			if ctx.EndTime.IsZero() {
				ctx.EndTime = time.Now()
			}
			resp = &extproc.ProcessingResponse{
				Response: &extproc.ProcessingResponse_ResponseBody{
					ResponseBody: &extproc.BodyResponse{},
				},
			}
		default:
			return fmt.Errorf("unknown request type")
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// startWorkers starts the worker goroutines for asynchronous processing.
func (s *server) startWorkers() {
	for i := 0; i < s.numWorkers; i++ {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for ctx := range s.ctxChan {
				if err := s.sink.Send(ctx); err != nil {
					fmt.Printf("Error sending to sink: %v\n", err)
				}
			}
		}()
	}
}

// stopWorkers stops the worker goroutines.
func (s *server) stopWorkers() {
	close(s.ctxChan)
	s.wg.Wait()
}
