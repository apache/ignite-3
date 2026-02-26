---
id: architecture-overview
title: Architecture Overview
sidebar_position: 0
---

# Architecture Overview

Apache Ignite 3 is a distributed database built on a modular component architecture. Each node runs the same set of services, enabling any node to handle client requests and participate in data storage. This document provides a deeper look at the system architecture introduced in [What is Apache Ignite 3?](../core-concepts/what-is-ignite).

## System Architecture

```mermaid
flowchart TB
    subgraph "Client Layer"
        TC[Thin Clients<br/>Java, .NET, C++]
        JDBC[JDBC/ODBC]
        REST[REST API]
    end

    subgraph "Node Architecture"
        subgraph "Request Processing"
            CH[Client Handler]
            SQL[SQL Query Processor<br/>Calcite-based]
            TXM[Transaction Manager]
        end

        subgraph "Cluster Coordination"
            CMG[Cluster Management Group]
            MS[MetaStorage]
            PD[Placement Driver]
        end

        subgraph "Data Management"
            RM[Replica Manager]
            TM[Table Manager]
            DZM[Distribution Zone Manager]
        end

        subgraph "Storage"
            SE[Storage Engines<br/>aimem, aipersist, rocksdb]
            RAFT[RAFT Groups<br/>per partition]
        end
    end

    TC & JDBC & REST --> CH
    CH --> SQL
    SQL --> TXM
    TXM --> PD
    PD --> RM
    RM --> RAFT
    RAFT --> SE
    CMG --> MS
    MS --> PD
```

## Node Components

Every Ignite node runs the same component set. There is no distinction between "server" and "coordinator" nodes at the software level. Node roles emerge from RAFT group membership and placement driver lease assignments.

### Core Services

| Component | Responsibility |
|-----------|---------------|
| **Cluster Service** | Network communication, node discovery, message routing |
| **Vault Manager** | Local persistent storage for node-specific data |
| **Hybrid Clock** | Distributed timestamp generation for MVCC |
| **Failure Manager** | Node failure detection and handling |

### Cluster Coordination

| Component | Responsibility |
|-----------|---------------|
| **Cluster Management Group (CMG)** | Cluster initialization, node admission, logical topology |
| **MetaStorage** | Distributed metadata (schemas, configurations, leases) |
| **Placement Driver** | Primary replica selection via time-bounded leases |

### Data Management

| Component | Responsibility |
|-----------|---------------|
| **Table Manager** | Table lifecycle and distributed operations |
| **Replica Manager** | Partition replica lifecycle and request routing |
| **Distribution Zone Manager** | Data placement policies and partition assignments |
| **Index Manager** | Index creation, maintenance, and async building |

### Query and Transaction Processing

| Component | Responsibility |
|-----------|---------------|
| **SQL Query Processor** | Query parsing, optimization, distributed execution |
| **Transaction Manager** | ACID transaction coordination, 2PC protocol |
| **Compute Component** | Distributed job execution |

## Cluster Coordination

### Cluster Management Group (CMG)

The CMG is a dedicated RAFT group responsible for cluster-wide coordination:

```mermaid
flowchart TB
    subgraph "CMG RAFT Group"
        L[Leader Node]
        V1[Voting Node]
        V2[Voting Node]
        LN1[Learner Node]
        LN2[Learner Node]
    end

    subgraph "Responsibilities"
        INIT[Cluster Initialization]
        JOIN[Node Join Validation]
        TOPO[Logical Topology]
        STATE[Cluster State Storage]
    end

    L <--> V1 & V2
    L --> LN1 & LN2
    L --> INIT & JOIN & TOPO & STATE
```

CMG membership is established during cluster initialization. Typically 3 to 5 nodes are selected as voting members; remaining nodes participate as learners. The CMG validates new nodes before admitting them to the cluster.

### MetaStorage

MetaStorage is a distributed key-value store replicated across all nodes via RAFT:

```mermaid
flowchart LR
    subgraph "MetaStorage Contents"
        CAT[System Catalog<br/>tables, indexes]
        CFG[Configuration<br/>zones, profiles]
        LEASE[Placement Leases<br/>primary assignments]
        DEPLOY[Deployment Units<br/>compute code]
    end

    subgraph "Replication"
        LEAD[Leader<br/>handles writes]
        VOTE[Voting Nodes<br/>3-5 nodes]
        LEARN[Learner Nodes<br/>remaining nodes]
    end

    CAT & CFG & LEASE & DEPLOY --> LEAD
    LEAD --> VOTE --> LEARN
```

All cluster metadata changes flow through MetaStorage. Nodes maintain local replicas and receive updates via RAFT log replication. The watch mechanism enables real-time notification of metadata changes.

### Placement Driver

The Placement Driver manages primary replica selection through time-bounded leases:

```mermaid
sequenceDiagram
    participant PD as Placement Driver
    participant MS as MetaStorage
    participant P1 as Primary Replica
    participant P2 as Backup Replica

    Note over PD: Lease Negotiation
    PD->>MS: Store Lease (partition, node, expiry)
    MS-->>P1: Lease Notification
    P1->>P1: Accept Primary Role

    Note over PD: Lease Renewal (every 2.5s for 5s lease)
    loop Every renewal interval
        PD->>MS: Extend Lease
        MS-->>P1: Renewed
    end

    Note over PD: Failover (lease expires)
    PD->>MS: Grant New Lease to P2
    MS-->>P2: Lease Notification
    P2->>P2: Accept Primary Role
```

Lease properties:

- **Duration**: Configurable, default 5 seconds
- **Renewal**: Automatic, every half of expiration interval
- **Selection priority**: Current holder, then RAFT leader, then stable assignments
- **Persistence**: Stored in MetaStorage for cluster-wide visibility

## RAFT Replication

Apache Ignite 3 uses multiple RAFT groups for different purposes:

| RAFT Group | Purpose | Members |
|------------|---------|---------|
| **CMG** | Cluster management | 3-5 voting nodes |
| **MetaStorage** | Cluster metadata | 3-5 voting + learner nodes |
| **Placement Driver** | Lease management | Placement driver nodes |
| **Partition Groups** | Data replication | Nodes in partition assignments |

### Partition RAFT Groups

Each table partition forms its own RAFT group:

```mermaid
flowchart TB
    subgraph "Table: orders (Zone: financial, 25 partitions)"
        subgraph "Partition 0"
            P0L[Leader<br/>Node 1]
            P0F1[Follower<br/>Node 2]
            P0F2[Follower<br/>Node 3]
        end
        subgraph "Partition 1"
            P1L[Leader<br/>Node 2]
            P1F1[Follower<br/>Node 1]
            P1F2[Follower<br/>Node 3]
        end
        subgraph "Partition N..."
            PNL[Leader]
            PNF1[Follower]
            PNF2[Follower]
        end
    end

    P0L <--> P0F1 & P0F2
    P1L <--> P1F1 & P1F2
```

Partition RAFT groups provide:

- **Write linearization**: All writes go through the leader
- **Replication**: Log entries replicated to followers
- **Automatic failover**: New leader elected on failure
- **State machine**: Multi-version storage with B+ trees

## Transaction Processing

### Transaction Coordination

The Transaction Manager coordinates distributed transactions using two-phase commit:

```mermaid
sequenceDiagram
    participant C as Client
    participant TM as Transaction Manager
    participant PD as Placement Driver
    participant P1 as Partition 1 Primary
    participant P2 as Partition 2 Primary

    C->>TM: BEGIN TRANSACTION
    TM->>TM: Create TX Context

    C->>TM: UPDATE table1 SET ...
    TM->>PD: Locate Primary (partition 1)
    PD-->>TM: Node 1
    TM->>P1: Enlist + Execute
    P1-->>TM: Enlisted

    C->>TM: UPDATE table2 SET ...
    TM->>PD: Locate Primary (partition 2)
    PD-->>TM: Node 2
    TM->>P2: Enlist + Execute
    P2-->>TM: Enlisted

    C->>TM: COMMIT
    TM->>P1: PREPARE
    TM->>P2: PREPARE
    P1-->>TM: PREPARED
    P2-->>TM: PREPARED
    TM->>P1: COMMIT
    TM->>P2: COMMIT
    P1-->>TM: COMMITTED
    P2-->>TM: COMMITTED
    TM-->>C: COMMIT SUCCESS
```

### Concurrency Control

Apache Ignite 3 uses a hybrid concurrency control model:

| Transaction Type | Concurrency Control | Isolation |
|-----------------|---------------------|-----------|
| **Read-Write** | Lock-based with MVCC | Serializable |
| **Read-Only** | Timestamp-based snapshot | Snapshot |

Read-write transactions acquire locks through the primary replica. Read-only transactions use timestamp-based reads against MVCC version chains without acquiring locks.

## Query Execution

### SQL Processing Pipeline

```mermaid
flowchart LR
    subgraph "Query Processing"
        SQL[SQL Text] --> PARSE[Parser]
        PARSE --> AST[AST]
        AST --> VAL[Validator]
        VAL --> OPT[Optimizer<br/>Calcite]
        OPT --> PHYS[Physical Plan]
        PHYS --> MAP[Mapper]
        MAP --> FRAG[Fragments]
    end

    subgraph "Distributed Execution"
        FRAG --> N1[Node 1<br/>Fragment A]
        FRAG --> N2[Node 2<br/>Fragment B]
        FRAG --> N3[Node 3<br/>Fragment C]
        N1 & N2 & N3 --> AGG[Aggregation]
        AGG --> RES[Result]
    end
```

The SQL engine is built on Apache Calcite and executes queries in stages:

1. **Parsing**: SQL text converted to abstract syntax tree
2. **Validation**: Semantic validation against the system catalog
3. **Optimization**: Cost-based optimization using Calcite rules
4. **Physical Planning**: Conversion to physical operators (IgniteRel)
5. **Mapping**: Plan fragments assigned to cluster nodes
6. **Execution**: Distributed execution with inter-node data exchange

### Fragment Distribution

Query plans are split into fragments that execute on different nodes:

```mermaid
flowchart TB
    subgraph "Fragment 0 (Coordinator)"
        ROOT[Project]
        JOIN[HashJoin]
        R1[Receiver]
        R2[Receiver]
    end

    subgraph "Fragment 1 (Node 1, 2, 3)"
        S1[Sender]
        TS1[TableScan: orders]
    end

    subgraph "Fragment 2 (Node 1, 2, 3)"
        S2[Sender]
        TS2[TableScan: customers]
    end

    ROOT --> JOIN
    JOIN --> R1 --> S1 --> TS1
    JOIN --> R2 --> S2 --> TS2
```

Exchange operators (Sender/Receiver) handle data movement between fragments. Colocation optimization reduces exchanges when joined data resides on the same nodes.

## Request Flow

A typical SQL query flows through these components:

```mermaid
sequenceDiagram
    participant C as Client
    participant CH as Client Handler
    participant SQL as SQL Processor
    participant TM as TX Manager
    participant PD as Placement Driver
    participant RM as Replica Manager
    participant SE as Storage Engine

    C->>CH: SQL Query
    CH->>SQL: Parse + Plan
    SQL->>TM: Create TX Context
    TM->>PD: Locate Primaries
    PD-->>TM: Lease Info

    loop For each partition
        TM->>RM: Route to Primary
        RM->>SE: Execute on Storage
        SE-->>RM: Results
        RM-->>TM: Partition Results
    end

    TM-->>SQL: Aggregated Results
    SQL-->>CH: Query Response
    CH-->>C: Response
```

## Component Initialization Order

Components start in dependency order:

```
VaultManager → ClusterService → CMG → MetaStorage → PlacementDriver →
ReplicaManager → TxManager → TableManager → SqlQueryProcessor
```

This ordering ensures each component's dependencies are available before it starts.

## Next Steps

- [Storage Architecture](./storage-architecture) for storage layer details
- [Data Partitioning](../core-concepts/data-partitioning) for partition distribution and replication
- [Transactions and MVCC](../core-concepts/transactions-and-mvcc) for transaction processing details
