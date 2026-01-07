# CLAUDE.md - KLattice Codebase Guide

## Project Overview

**KLattice** is a PostgreSQL protocol-compatible backend that bridges SQL queries with Kafka topics and Confluent Schema Registry. It intercepts PostgreSQL queries, transforms them through a multi-stage pipeline, and executes them using DuckDB while reifying Kafka topics and Schema Registry schemas as SQL tables.

### Core Concept
KLattice implements a **three-pointcut architecture** for query processing:
1. **Query**: Parse and validate SQL (PostgreSQL syntax)
2. **Plan**: Transform to relational algebra using Apache Calcite, convert to Substrait
3. **Execution**: Execute via DuckDB with HTTP-based table value expressions

## Architecture Components

### 1. connector/ (Rust)
**Purpose**: PostgreSQL wire protocol implementation and query entry point

**Tech Stack**:
- `pgwire` (v0.16.0) - PostgreSQL protocol handler
- `tonic` (v0.10.1) - gRPC client for backend communication
- `tokio` - Async runtime

**Key Files**:
- `connector/src/main.rs`: Main entry point, implements PostgreSQL simple protocol
  - `QueryProcessor`: Handles incoming SQL queries via pgwire
  - Connects to `OracleService` via gRPC
  - Type mapping from Substrait types to PostgreSQL types

**Entry Points**:
- Listens on port 5433 (configurable via `KLATTICE_PORT`)
- Connects to planner service (configurable via `KLATTICE_ENDPOINT`)

**Current Limitations**:
- Only implements simple query protocol
- Extended protocol (prepared statements, cancellation) not yet supported

### 2. planner/ (Java/Quarkus)
**Purpose**: Query planning, optimization, and orchestration

**Tech Stack**:
- Quarkus 3.3.3 (framework)
- Apache Calcite 1.35.0 (query planning/optimization)
- Substrait 0.17.0 (plan encoding)
- Isthmus (Calcite → Substrait conversion)
- gRPC for inter-service communication

**Key Packages**:
- `klattice.facade.OracleGrpcService`: Main orchestrator, implements the facade pattern
  - Coordinates Query → Plan → Execution flow
  - Manages environment (schema metadata) from Schema Registry
- `klattice.query.QueryServiceGrpc`: SQL parsing and validation
- `klattice.plan.PlannerServiceGrpc`: Plan expansion and optimization
- `klattice.calcite.*`: Calcite integration
  - `DuckDbDialect`: Custom SQL dialect for DuckDB compatibility
  - `Rules`: Query optimization rules
  - `SchemaHolder`: Schema management
  - `FunctionNames`, `FunctionShapes`: Function translation layer
- `klattice.substrait.*`: Substrait conversion
  - `CalciteToSubstraitConverter`: RelNode → Substrait
  - `SubstraitToCalciteConverter`: Substrait → RelNode
- `klattice.store.*`: Schema Registry integration
  - `SchemaRegistryStoreSource`: Fetch schemas from Confluent Schema Registry
  - `DatabaseStoreSource`: Database metadata management
- `klattice.duckdb.*`: DuckDB REST client for SQL execution

**Configuration** (via `-D` flags in docker-compose):
- `klattice.host.url`: Host service URL
- `quarkus.rest-client.schema-registry-api.url`: Schema Registry URL
- `quarkus.rest-client.duckdb-service-api.url`: DuckDB service URL

### 3. core/ (Java/Quarkus)
**Purpose**: Core data structures, protobuf definitions, and shared utilities

**Tech Stack**:
- Quarkus 3.3.3
- Substrait 0.17.0
- gRPC

**Key Packages**:
- `klattice.schema.*`: Schema Registry resource and service implementations
  - REST endpoints for schema management

**Protobuf Definitions** (`core/src/main/proto/`):
- `messages.proto`: Core data structures
  - `Query`, `QueryDescriptor`: Query representations
  - `Plan`, `PreparedQuery`: Query plans
  - `Batch`, `Row`, `Column`: Result data structures
  - `Environment`, `Schema`, `Rel`: Schema metadata
  - `ExpandedPlan`, `CompiledSql`: Expanded plans for execution
- `query.proto`: QueryService definition (Inflate operation)
- `plan.proto`: PlannerService definition (Expand operation)
- `exec.proto`: ExecService definition (execute operation)
- `oracle.proto`: OracleService facade (answer operation)

### 4. host/ (Java/Quarkus)
**Purpose**: Data export services for Kafka topics and Schema Registry

**Tech Stack**:
- Quarkus 3.3.3
- Kafka & Confluent Schema Registry 7.4.0
- Apache Parquet 1.13.1 & Hadoop 3.3.6
- Apache Avro 1.11.2

**Key Packages**:
- `klattice.endpoint.*`: REST endpoints for data export
  - `ParquetExportResource`: Export data as Parquet files
  - `SysTableExportResource`: System table exports
  - `RangeExposer`: Range-based data access
- `klattice.file.*`: File format converters
  - `AvroParquetExport`, `CsvToAvroParquetExporter`: Format converters
  - `KafkaExporter`: Kafka topic data export
  - `ParquetBufferedWriter`: Buffered Parquet writing

**Configuration**:
- `klattice.kafka.bootstrap`: Kafka bootstrap servers
- `quarkus.rest-client.schema-registry-api.url`: Schema Registry URL

### 5. proto/
Contains Substrait protocol buffer definitions (substrait/). These define the common query plan representation format.

## Data Flow

```
PostgreSQL Client (port 15433)
    ↓
connector (Rust) - PostgreSQL wire protocol
    ↓ gRPC Query
planner (OracleGrpcService) - Orchestrates the flow
    ↓ 1. QueryService.inflate()
planner (QueryService) - Parse SQL with Calcite
    ↓ PreparedQuery (Substrait Plan)
    ↓ 2. PlannerService.expand()
planner (PlannerService) - Optimize & convert to DuckDB SQL
    ↓ ExpandedPlan (CompiledSql)
    ↓ 3. ExecService.execute()
host (ExecService) - Execute via DuckDB REST API
    ↓ Batch (result rows)
connector - Convert to PostgreSQL wire format
    ↓
PostgreSQL Client - Receives results
```

## Development Workflows

### Prerequisites
- Java 17+ (for Gradle/Quarkus)
- Rust stable (see `rust-toolchain`)
- Docker & Docker Compose
- Gradle (use `./gradlew`)

### Building Components

**Java modules** (core, planner, host):
```bash
./gradlew build                 # Build all Java modules
./gradlew :planner:build        # Build specific module
./gradlew :planner:test         # Run tests for module
./gradlew :planner:quarkusDev   # Run in dev mode with hot reload
```

**Rust connector**:
```bash
cd connector
cargo build                     # Debug build
cargo build --release           # Release build
cargo test                      # Run tests (if any)
cargo run                       # Run directly
```

### Local Development with Docker Compose

**Start all services**:
```bash
docker-compose up --build
```

**Services and Ports**:
- PostgreSQL interface (connector): `localhost:15433`
- Planner gRPC: `localhost:8080` (HTTP: 8080, gRPC: 9000)
- Host services: `localhost:8081` (HTTP: 8081, gRPC: 9001)
- Kafka: `localhost:9092`
- Schema Registry: `localhost:8085`
- DuckDB service: `localhost:8091`
- pgAdmin4: `localhost:8082` (user: klattice@klattice.io, pass: klattice)

**Service Dependencies**:
```
connector → planner (oracle) → {query, planner, exec services}
planner → host (exec service)
planner → schema-registry
planner → duckdb
host → kafka, schema-registry
```

### Testing

**Connect via PostgreSQL client**:
```bash
psql -h localhost -p 15433 -U klattice
```

**Run tests**:
```bash
./gradlew test                  # All tests
./gradlew :planner:test         # Planner tests only
```

**Test files locations**:
- `planner/src/test/java/klattice/facade/OracleGrpcServiceTest.java`
- `planner/src/test/java/klattice/query/PrepareTest.java`
- `planner/src/test/java/klattice/plan/rule/MagicValuesReplaceRuleTest.java`
- `core/src/test/java/klattice/endpoint/SchemaRegistryResourceTest.java`

## Key Conventions

### Code Organization

**Java Modules**:
- Follow package-by-feature organization: `klattice.<feature>.*`
- gRPC service implementations use `@GrpcService` annotation
- Use Quarkus dependency injection (`@Inject`)
- REST endpoints use `@Path` annotations

**Rust**:
- Single binary crate (`connector`)
- Generated protobuf code via `tonic-build` in `build.rs`
- Async/await with tokio runtime

### Naming Conventions

**Services**:
- `*Service`: Java service implementations
- `*ServiceGrpc`: gRPC service interfaces (generated from proto)
- `*Resource`: REST endpoint resources
- `*GrpcService`: gRPC service implementations

**Data Structures**:
- Protobuf messages use PascalCase
- Java classes use PascalCase
- Rust uses snake_case for functions/variables

### gRPC Patterns

**Synchronous-style with Uni** (Quarkus Mutiny):
```java
@GrpcService
public class MyService implements ServiceInterface {
    @Blocking
    @Override
    public Uni<Response> method(Request req) {
        // Implementation returns Uni for reactive composition
        return someClient.call(req)
            .flatMap(result -> anotherClient.call(result));
    }
}
```

**Client injection**:
```java
@GrpcClient
ServiceInterface client;
```

### Error Handling

**Protobuf error pattern**:
Messages use `has_error` + `oneof result { success_case | diagnostics }` pattern:
```protobuf
message PreparedQuery {
    bool has_error = 1;
    oneof result {
        Plan plan = 2;
        QueryDiagnostics diagnostics = 3;
    }
}
```

**Rust error handling**:
- Return `PgWireResult<T>` for pgwire operations
- Use `Result<T, Box<dyn std::error::Error>>` for async main

### Protobuf Code Generation

**Java** (automatic via Quarkus gRPC plugin):
- Place `.proto` files in `src/main/proto/`
- Generated code appears in `target/generated-sources/`

**Rust** (via `build.rs`):
```rust
tonic::include_proto!("substrait");
tonic::include_proto!("klattice.msg");
```

## Development Guidelines for AI Assistants

### When Making Changes

1. **Understand the flow**: Changes often span multiple services
   - Connector change → may need proto + planner changes
   - Proto change → requires rebuild of both Rust and Java code
   - Calcite change → may need DuckDB dialect updates

2. **Type system alignment**:
   - Keep Substrait types, Calcite types, DuckDB types, and PostgreSQL types aligned
   - Update type mappings in `connector/src/main.rs` (lines 86-115)
   - Update function translations in `planner/.../calcite/FunctionNames.java`

3. **Testing strategy**:
   - Test end-to-end via PostgreSQL client when possible
   - Unit test individual services with `@QuarkusTest`
   - Mock gRPC clients for isolated testing

4. **Protocol changes**:
   - Update proto files in `core/src/main/proto/`
   - Rebuild all services: `./gradlew clean build && cd connector && cargo build`
   - Update client code in connector to handle new proto messages

### Common Tasks

**Adding a new SQL function**:
1. Add function mapping in `planner/src/main/java/klattice/calcite/FunctionNames.java`
2. If semantics differ, add translation rule in `Rules.java`
3. Test with query through PostgreSQL interface

**Adding Schema Registry table**:
1. Update `SchemaRegistryStoreSource.java` to expose new schema
2. Schema appears automatically as table via Calcite

**Modifying query flow**:
1. Update `OracleGrpcService.answer()` method
2. Consider impact on QueryService, PlannerService, ExecService
3. Update proto messages if data structures change

**Performance optimization**:
- Use Calcite rules in `planner/.../plan/rule/` for query optimization
- Consider DuckDB execution patterns
- Profile with DuckDB's EXPLAIN

### Debugging Tips

1. **Enable Quarkus dev mode**:
   ```bash
   ./gradlew :planner:quarkusDev
   ```
   Hot reload for Java changes

2. **View gRPC traffic**:
   - Check container logs: `docker-compose logs -f planner`
   - Quarkus logs gRPC calls at INFO level

3. **Inspect Substrait plans**:
   - Plans are in protobuf binary format
   - Add logging in `CalciteToSubstraitConverter` to see RelNode trees
   - Use Calcite's `RelOptUtil.toString()` for readable plans

4. **PostgreSQL wire protocol issues**:
   - Check connector logs for parsing errors
   - pgwire library provides detailed error messages

5. **Health checks**:
   - Planner: `http://localhost:8080/q/health`
   - Host: `http://localhost:8081/q/health`

### Architecture Decisions

**Why three separate services?**
- Language flexibility: Rust for protocol, Java for planning
- Isolation: Can swap Query parser, Plan optimizer, or Execution engine independently
- Scalability: Can scale services independently

**Why Calcite?**
- Industry-standard query optimization
- Extensible rule-based optimization
- Clean separation of parsing, planning, execution

**Why Substrait?**
- Cross-language plan exchange format
- Future-proof for different execution engines
- Vendor-neutral representation

**Why DuckDB?**
- Embedded OLAP database optimized for analytics
- Excellent support for Parquet, CSV
- HTTP/S filesystem support (HTTPFS) for remote data

## Current Limitations and TODOs

From README.org:

1. **Protocol compatibility**: Only simple query protocol implemented
   - TODO: Implement extended protocol (prepared statements, bind, execute)
   - TODO: Implement query cancellation
   - TODO: Implement complex subprotocol

2. **Syntax differences**: PostgreSQL vs DuckDB
   - Cast syntax: `::type` vs DuckDB syntax
   - Function name mappings needed
   - Partially addressed via Calcite + custom dialect

3. **Reification**:
   - Schema Registry → CSV export (implemented)
   - Kafka topics → Parquet export (code exists, needs endpoint wiring)
   - Large topics need memory-mapped file handling

## File Structure Reference

```
klattice/
├── connector/              # Rust PostgreSQL protocol handler
│   ├── src/main.rs        # Entry point, QueryProcessor
│   ├── build.rs           # Protobuf code generation
│   ├── Dockerfile
│   └── Cargo.toml
├── planner/               # Java query planning service
│   └── src/main/java/klattice/
│       ├── facade/        # OracleGrpcService (orchestrator)
│       ├── query/         # QueryService (SQL parsing)
│       ├── plan/          # PlannerService (optimization)
│       ├── calcite/       # Calcite integration
│       ├── substrait/     # Substrait conversion
│       ├── store/         # Schema Registry client
│       └── duckdb/        # DuckDB REST client
├── core/                  # Shared protobuf definitions
│   └── src/main/
│       ├── proto/         # Proto definitions
│       └── java/klattice/schema/  # Schema Registry service
├── host/                  # Data export services
│   └── src/main/java/klattice/
│       ├── endpoint/      # REST endpoints
│       └── file/          # Format converters
├── proto/
│   └── substrait/         # Substrait proto definitions
├── docker-compose.yml     # Local development environment
├── settings.gradle        # Gradle multi-module setup
├── Cargo.toml            # Rust workspace
└── README.org            # Project documentation

Build outputs:
├── connector/target/      # Rust build artifacts
├── */build/              # Gradle build outputs
└── .gradle/              # Gradle cache
```

## Quick Reference

**Environment Variables**:
- `KLATTICE_ENDPOINT`: Connector → Planner endpoint (default: `127.0.0.1:8080`)
- `KLATTICE_PORT`: Connector listening port (default: `5433`)
- `DUCKDB_SERVER_PORT`: DuckDB service port (default: `8091`)

**Common Commands**:
```bash
# Full rebuild
./gradlew clean build && cd connector && cargo clean && cargo build

# Run tests
./gradlew test

# Start local environment
docker-compose up --build

# Connect via psql
psql -h localhost -p 15433 -U klattice

# View logs
docker-compose logs -f planner
docker-compose logs -f connector

# Restart single service
docker-compose restart planner
```

**Git Branch Workflow**:
- Develop on branch: `claude/claude-md-mk3ym6w2ryde6sjm-R1t6O`
- Main branch: (empty - check `git branch -r` for remote default)
- Always push to the claude/* branch specified in task context

---

Last Updated: 2026-01-07
