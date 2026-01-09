---
name: ignite-cluster-setup
description: Set up and manage a local Apache Ignite 3 cluster using just commands. Use when asked to setup cluster, start nodes, rebuild, initialize cluster, or check status.
allowed-tools: Bash(just:*)
---

# Ignite Cluster Setup

This skill manages a local Apache Ignite 3 cluster for development using `just` commands.

## Available Commands

| Command | Description |
|---------|-------------|
| `just setup` | Full rebuild: build distributions, create node dirs in `w/` |
| `just setup_cli` | Rebuild CLI only (fast) |
| `just start 1` | Start node 1 (use 1, 2, or 3) |
| `just start_all` | Start all 3 nodes |
| `just stop 1` | Stop node 1 |
| `just stop_all` | Stop all nodes |
| `just init` | Initialize cluster (after nodes started) |
| `just status` | Check status of all nodes |
| `just cli` | Launch CLI REPL |
| `just setup_cluster` | Full setup: build + start all + init |

## Common Workflows

### Fresh Setup (First Time or After Code Changes)

```bash
just setup          # Build everything, create node directories
just start 1        # Start node 1 (or start_all for all nodes)
just init           # Initialize the cluster
```

### Rebuild CLI Only (After CLI Code Changes)

```bash
just setup_cli      # Fast rebuild of CLI only
```

### Quick Restart (No Code Changes)

```bash
just start 1        # Start node
just status         # Verify running
```

### Full Automated Setup

```bash
just setup_cluster  # Does: setup + start_all + init
```

## Notes

- Nodes run in `w/ignite3-db-{1,2,3}/`
- CLI is in `w/ignite3-cli/`
- Use `just status` to check which nodes are running
- If nodes fail with RocksDB errors, run `just setup` to rebuild fresh
