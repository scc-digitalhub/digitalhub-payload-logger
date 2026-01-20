// SPDX-FileCopyrightText: © 2026 DSLab - Fondazione Bruno Kessler
//
// SPDX-License-Identifier: Apache-2.0

// Package main is the entry point for the payload-logger application.
// It sets up the gRPC server and configures the sink based on command-line arguments.
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	extproc "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	_ "github.com/lib/pq"
	"github.com/scc-digitalhub/payload-logger/pkg/sink"
	"google.golang.org/grpc"
)

// main initializes the application based on command-line flags.
// It creates the appropriate sink, sets up the gRPC server, and starts listening.
func main() {
	configPath := flag.String("config", "", "path to configuration YAML file")
	sinkType := flag.String("sink", "", "type of sink (console, postgres) - overrides config")
	flag.Parse()

	config, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Override sink type from flag if provided
	if *sinkType != "" {
		config.Server.SinkType = *sinkType
	}

	var s sink.Sink
	switch config.Server.SinkType {
	case "console":
		s, err = sink.NewConsoleSink(config.Memory.MaxBodySize)
		if err != nil {
			log.Fatalf("failed to create console sink: %v", err)
		}
	case "postgres":
		dbConfig := &sink.DatabaseConfig{
			URL:             config.Database.URL,
			TableName:       config.Database.TableName,
			MaxRetries:      config.Database.MaxRetries,
			BatchSize:       config.Database.BatchSize,
			FlushInterval:   config.Database.FlushInterval,
			MaxOpenConns:    config.Database.MaxOpenConns,
			MaxIdleConns:    config.Database.MaxIdleConns,
			ConnMaxLifetime: config.Database.ConnMaxLifetime,
			MaxBodySize:     config.Memory.MaxBodySize,
		}
		primary, err := sink.NewPostgreSink(dbConfig)
		if err != nil {
			log.Fatalf("failed to create postgres sink: %v", err)
		}
		fallback, err := sink.NewConsoleSink(config.Memory.MaxBodySize)
		if err != nil {
			log.Fatalf("failed to create console fallback sink: %v", err)
		}
		s = sink.NewFallbackSink(primary, fallback)
	default:
		log.Fatalf("unknown sink type: %s", config.Server.SinkType)
	}
	defer func() {
		if err := s.Close(); err != nil {
			log.Printf("Error closing sink: %v", err)
		}
	}()

	lis, err := net.Listen("tcp", config.Server.Port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	srv := grpc.NewServer()

	// Create server with asynchronous processing
	ctxChan := make(chan sink.RequestContext, config.Processing.ChannelBufferSize)
	server := &server{
		sink:             s,
		ctxChan:          ctxChan,
		numWorkers:       config.Processing.NumWorkers,
		contextNamespace: config.Server.MetadataNamespace,
	}
	server.startWorkers()

	extproc.RegisterExternalProcessorServer(srv, server)

	// Channel to listen for interrupt signals
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("server listening at %v", lis.Addr())
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// Wait for interrupt signal
	<-c
	log.Println("Shutting down server...")

	// Graceful shutdown: stop accepting new connections and wait for existing ones
	srv.GracefulStop()

	// Stop workers and close sinks
	server.stopWorkers()
	if err := s.Close(); err != nil {
		log.Printf("Error closing sink: %v", err)
	}
	log.Println("Server stopped")
}
