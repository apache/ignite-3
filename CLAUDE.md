# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Building the Project
- **Full build**: `./gradlew clean build`
- **Fast build (skip tests)**: `./gradlew clean build -x test -x integrationTest`
- **Fastest build (skip everything)**: `./gradlew clean build -x assembleDist -x distTar -x distZip -x check`

### Testing
- **Unit tests only**: `./gradlew clean test`
- **Integration tests only**: `./gradlew clean integrationTest`
- **Both unit + integration**: `./gradlew clean test integrationTest`
- **Run single test class**: `./gradlew :ignite-<module>:test --tests "*TestClassName*"`

### Code Quality Checks
- **All checks**: `./gradlew clean check`
- **Checkstyle only**: `./gradlew checkstyleMain checkstyleIntegrationTest checkstyleTest checkstyleTestFixtures`
- **Spotbugs only**: `./gradlew spotbugsMain`
- **PMD only**: `./gradlew pmdMain pmdTest`

### Packaging and Distribution
- **Create distribution packages**: `./gradlew clean allDistZip`
- **CLI package only**: `./gradlew clean packaging-cli:distZip`
- **DB package only**: `./gradlew clean packaging-db:distZip`
- **Docker image**: `./gradlew clean docker -x test -x check`

### Running Ignite (Local Cluster with `just`)
Use the `ignite-cluster-setup` skill for cluster management tasks (start, stop, status, init, etc.). See `.justfile` for all available recipes.

- **Setup**: `just setup` - Build distributions and create node directories in `w/`
- **Start node**: `just start 1` - Start node 1 (use 1, 2, or 3)
- **Stop node**: `just stop 1` - Stop node 1
- **Initialize cluster**: `just init` - Initialize the cluster
- **Launch CLI**: `just cli` - Open interactive CLI
- **Check status**: `just status` - Show status of all nodes
- **Full setup**: `just setup_cluster` - Setup, start all nodes, and initialize

### Running Ignite (Manual)
- **Start node from distribution**: `./bin/ignite3db start` (from unpacked distribution)
- **Connect CLI**: `./bin/ignite3` (from CLI distribution)
- **Docker Compose cluster**: `docker compose -f packaging/docker/docker-compose.yml up -d`

### Benchmarks
- **Run module benchmarks**: `./gradlew clean :ignite-<module>:jmh`
- **Run specific benchmark**: `./gradlew clean :ignite-<module>:jmh -PjmhBench=BenchmarkName.methodName`
- **JFR profiling**: `./gradlew :ignite-<module>:jmh -PjmhProfileJfr`

### Documentation
- **Generate Javadoc**: `./gradlew aggregateJavadoc`
- **Module-specific Javadoc**: `./gradlew javadoc`

## High-Level Architecture

### Core Components
Apache Ignite 3 is built with a modular, component-based architecture where components form an acyclic dependency graph:

1. **Runner Module** (`modules/runner/`): Main entry point that wires up all components and handles node lifecycle
2. **Network Module** (`modules/network/`): Group membership, messaging, and cluster communication
3. **Metastorage Module** (`modules/metastorage/`): Distributed key-value storage for cluster metadata using Raft consensus
4. **Catalog Module** (`modules/catalog/`): Schema management and DDL operations
5. **Table Module** (`modules/table/`): Table API implementation and partition management
6. **SQL Engine** (`modules/sql-engine/`): Distributed SQL query processing with Apache Calcite
7. **Storage Engines**: Pluggable storage with RocksDB (`modules/storage-rocksdb/`) and in-memory (`modules/storage-page-memory/`) options
8. **Transactions** (`modules/transactions/`): ACID transactions with MVCC and serializable isolation
9. **Compute** (`modules/compute/`): Distributed compute framework for job execution
10. **Configuration** (`modules/configuration/`): Dynamic configuration management system

### Component Initialization
Components are initialized in topological sort order based on dependencies. Each component:
- Has clearly defined dependencies provided at construction time
- Receives metastorage watch notifications in dependency order
- Must not create cyclic dependencies
- Uses the Vault (`modules/vault/`) for persistent local state

### Key Architectural Patterns
- **Schema-Driven**: All operations are based on explicit schemas defined through DDL
- **Watch-Based Updates**: Components react to metastorage changes via reliable watch processing
- **Pluggable Storage**: Storage engines can be swapped (RocksDB, in-memory, custom)
- **Partition-Based**: Data is distributed using partition-based sharding
- **Raft for Consensus**: Critical components (metastorage, table partitions) use Raft for consistency

### Testing Structure
- **Unit tests**: Located in `src/test/java/` - use JUnit 5 with `@Test` annotations
- **Integration tests**: Located in `src/integrationTest/java/` - test component interactions
- **Test fixtures**: Located in `src/testFixtures/java/` - shared test utilities
- **Benchmarks**: Located in `src/jmh/java/` - JMH performance benchmarks

### Module Organization
Each module follows standard Gradle structure:
- `src/main/java/` - Main source code
- `src/test/java/` - Unit tests
- `src/integrationTest/java/` - Integration tests
- `src/testFixtures/java/` - Test utilities
- `build.gradle` - Module-specific build configuration

Key modules are organized by functional area (storage, network, sql-engine, etc.) with clear API boundaries and minimal inter-module dependencies.

## Code Style & Commit Rules

### Before Every Commit (MANDATORY)
Run checkstyle and PMD on the modified module(s):
```bash
./gradlew :ignite-<module>:checkstyleMain :ignite-<module>:checkstyleTest :ignite-<module>:pmdMain :ignite-<module>:pmdTest
```

### Git Push
**Never use `git push` without specifying the target branch.** Always push explicitly:
```bash
git push origin <branch-name>
```

### Design Preferences
- **Prefer objects over static methods** - Use constructor injection everywhere possible
- **Always include Jira ticket link in PRs** - Link to https://issues.apache.org/jira/browse/IGNITE-XXXXX

## Jira Workflow

- All tickets **must** have the `ignite-3` label
- When resolving a ticket:
  - **Reviewer** must be set (ask if unsure whom to set)
  - **Fix Version** must be set