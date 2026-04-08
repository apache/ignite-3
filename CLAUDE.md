@AGENTS.md

### Running Ignite (Local Cluster with `just`)
Use the `ignite-cluster-setup` skill for cluster management tasks (start, stop, status, init, etc.). See `.justfile` for all available recipes.

- **Setup**: `just setup` - Build distributions and create node directories in `w/`
- **Start node**: `just start 1` - Start node 1 (use 1, 2, or 3)
- **Stop node**: `just stop 1` - Stop node 1
- **Initialize cluster**: `just init` - Initialize the cluster
- **Launch CLI**: `just cli` - Open interactive CLI
- **Check status**: `just status` - Show status of all nodes
- **Full setup**: `just setup_cluster` - Setup, start all nodes, and initialize
