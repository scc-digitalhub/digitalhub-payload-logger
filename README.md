# Payload Logger

A gRPC server implementing Envoy Proxy's External Processor functionality for logging HTTP request and response payloads.

## Overview

The Payload Logger acts as an Envoy External Processor that captures complete HTTP request/response cycles, extracts relevant metadata, and logs the data to configurable sinks (console or PostgreSQL).

## Features

- **Complete Request Logging**: Captures headers, body, timestamps, and metadata for both requests and responses
- **Service Name Extraction**: Automatically extracts service names from configurable header keys
- **Metadata Processing**: Extracts and processes Envoy filter metadata from configurable namespaces
- **Multiple Sinks**: Support for console logging and PostgreSQL database storage
- **Envoy Integration**: Seamless integration with Envoy proxy as an external processing filter

## Metadata Processing

The Payload Logger can extract metadata from Envoy's filter metadata context. This allows capturing additional information set by other Envoy filters or plugins.

### Configuration

Configure the metadata namespace in your `config.yaml`:

```yaml
server:
  metadata_namespace: "digitalhub"
```

Or via environment variable:

```bash
export METADATA_NAMESPACE="my-namespace"
```

### Envoy Configuration

To pass metadata to the external processor, configure Envoy filters to set metadata in the configured namespace:

```yaml
filters:
- name: envoy.filters.http.lua
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.lua.v3.Lua
    inline_code: |
      function envoy_on_request(request_handle)
        request_handle:streamInfo():dynamicMetadata():set("digitalhub", "service_name", "my-service")
        request_handle:streamInfo():dynamicMetadata():set("digitalhub", "function_name", "my-function")
        request_handle:streamInfo():dynamicMetadata():set("digitalhub", "project_name", "my-project")
      end
```

### Dynamic Table Selection

The PostgreSQL sink supports dynamic table selection based on metadata. If the metadata contains a `function_name` or `service_name` field, it will be used instead of the configured default table name. This allows routing logs to different tables based on request characteristics.

When a table is dynamically identified, the sink automatically checks if the table exists in the database. If it doesn't exist, the table is created with the appropriate schema before inserting data.

Example Envoy configuration for dynamic table selection:

```yaml
filters:
- name: envoy.filters.http.lua
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.lua.v3.Lua
    inline_code: |
      function envoy_on_request(request_handle)
        local path = request_handle:headers():get(":path")
        if string.match(path, "/api/v1/") then
          request_handle:streamInfo():dynamicMetadata():set("digitalhub", "service_name", "api_v1_logs")
        elseif string.match(path, "/api/v2/") then
          request_handle:streamInfo():dynamicMetadata():set("digitalhub", "service_name", "api_v2_logs")
        else
          request_handle:streamInfo():dynamicMetadata():set("digitalhub", "service_name", "other_logs")
        end
      end
```

The metadata will be captured and included in the logged request context.

## Architecture

The application consists of:
- **gRPC Server**: Implements Envoy's External Processor service
- **Request Context**: Accumulates data across the request lifecycle
- **Sink Interface**: Pluggable output destinations
- **Processor**: Handles Envoy streaming messages and coordinates data flow

## Building

```bash
go mod tidy
go build
```

## Running

### Console Sink (Default)

```bash
./payload-logger
```

Or with config file:
```bash
./payload-logger -config=configs/config.yaml
```

### PostgreSQL Sink (With TimescaleDB extension)

Set environment variables:
```bash
export DATABASE_URL="postgres://user:pass@localhost/dbname?sslmode=disable"
export TABLE_NAME="request_logs"  # optional, defaults to "request_logs"
```

Run with PostgreSQL sink:
```bash
./payload-logger -sink=postgres
```

Or using config file:
```bash
./payload-logger -config=configs/config.yaml
```

### Database Schema

For PostgreSQL sink, create the table:

```sql
CREATE TABLE request_logs (
    service_name TEXT,
    function_name TEXT,
    project_name TEXT,
    request_time TIMESTAMP,
    request_duration BIGINT,
    response_status TEXT,
    request_body TEXT,
    response_body TEXT,
    request_path TEXT
);
```

## Configuration

### Configuration File

The application supports configuration via a YAML file. Create a `configs/config.yaml` file or specify a custom path with the `-config` flag.

Example `config.yaml`:

```yaml
server:
  port: ":50051"
  sink_type: "console"
  metadata_namespace: "digitalhub"

database:
  url: "postgres://user:pass@localhost/dbname?sslmode=disable"
  table_name: "request_logs"
  max_retries: 3
  batch_size: 50
  flush_interval: "10s"
  max_open_conns: 25
  max_idle_conns: 5
  conn_max_lifetime: "5m"

processing:
  num_workers: 10
  channel_buffer_size: 1000

memory:
  max_body_size: 10485760  # 10MB
```

### Command Line Flags

- `-config`: Path to configuration YAML file (optional)
- `-sink`: Sink type override (`console` or `postgres`)

### Environment Variables

All configuration values can be overridden with environment variables:

- `PORT`: Server port (default: ":50051")
- `SINK_TYPE`: Sink type (`console` or `postgres`)
- `METADATA_NAMESPACE`: Namespace for Envoy filter metadata (default: "digitalhub")
- `DATABASE_URL`: PostgreSQL connection string
- `TABLE_NAME`: Default database table name
- `MAX_RETRIES`: Database retry attempts
- `BATCH_SIZE`: Database batch size
- `NUM_WORKERS`: Number of processing workers
- `CHANNEL_BUFFER_SIZE`: Request channel buffer size
- `MAX_BODY_SIZE`: Maximum body size in bytes

### Priority Order

Configuration values are applied in this order (later overrides earlier):
1. Default values
2. YAML configuration file
3. Environment variables
4. Command line flags

## Envoy Configuration

Add the External Processor filter to your Envoy configuration:

```yaml
filters:
- name: envoy.filters.http.ext_proc
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
    grpc_service:
      envoy_grpc:
        cluster: ext_proc_cluster
    processing_mode:
      request_header_mode: "SEND"
      response_header_mode: "SEND"
      request_body_mode: "STREAMED"
      response_body_mode: "STREAMED"

clusters:
- name: ext_proc_cluster
  type: LOGICAL_DNS
  dns_lookup_family: V4_ONLY
  load_assignment:
    cluster_name: ext_proc_cluster
    endpoints:
    - lb_endpoints:
      - endpoint:
          address:
            socket_address:
              address: localhost
              port_value: 50051
```

## Development

### Project Structure

```
payload-logger/
├── cmd/
│   └── payload-logger/
│       ├── main.go           # Application entry point
│       ├── processor.go      # Envoy processor implementation
│       ├── config.go         # Configuration management
│       ├── main_test.go      # Unit tests for main
│       └── processor_test.go # Unit tests for processor
├── pkg/
│   └── sink/
│       ├── sink.go       # Sink interface and common types
│       ├── console.go    # Console sink implementation
│       ├── postgres.go   # PostgreSQL sink implementation
│       ├── fallback.go   # Fallback sink implementation
│       ├── sink_test.go  # Unit tests
│       ├── sink_integration_test.go # Integration tests
│       └── postgres_test.go # PostgreSQL tests
├── configs/
│   └── config.yaml       # Example configuration file
├── deploy/
│   └── helm/             # Kubernetes deployment
├── build/
│   └── Dockerfile        # Container build
├── bin/
│   └── payload-logger    # Compiled binary
├── go.mod
├── go.sum
└── README.md         # This file
```

### Testing

Run unit tests:
```bash
go test ./...
```

Run with coverage:
```bash
go test -cover ./...
```

## Dependencies

- [Envoy Go Control Plane](https://github.com/envoyproxy/go-control-plane) - Envoy protobuf definitions
- [gRPC](https://google.golang.org/grpc) - RPC framework
- [pq](https://github.com/lib/pq) - PostgreSQL driver
- [gopkg.in/yaml.v3](https://gopkg.in/yaml.v3) - YAML configuration parsing

## License

This project is licensed under the MIT License.</content>
<parameter name="filePath">/Users/raman/workspace/aigateway/payload-logger/README.md