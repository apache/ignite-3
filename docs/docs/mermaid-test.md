# Mermaid Diagram Test

This page tests Mermaid diagram rendering for Apache Ignite 3 documentation.

## Flowchart

```mermaid
flowchart TD
    A[Client Application] --> B{Connect to Cluster}
    B -->|Success| C[Get Table Reference]
    B -->|Failure| D[Connection Error]
    C --> E[Get Key-Value View]
    E --> F[Perform Operations]
    F --> G{Transaction?}
    G -->|Yes| H[Commit/Rollback]
    G -->|No| I[Direct Operation]
    H --> J[Close Connection]
    I --> J
    J --> K[End]
    D --> K
```

## Sequence Diagram

```mermaid
sequenceDiagram
    participant Client
    participant Node1
    participant Node2
    participant Storage

    Client->>Node1: Connect
    Node1-->>Client: Connection Established

    Client->>Node1: Execute Query
    Node1->>Node1: Parse SQL
    Node1->>Node2: Forward to Primary
    Node2->>Storage: Read Data
    Storage-->>Node2: Return Data
    Node2-->>Node1: Query Result
    Node1-->>Client: Result Set

    Client->>Node1: Close Connection
    Node1-->>Client: Goodbye
```

## Class Diagram

```mermaid
classDiagram
    class IgniteClient {
        +connect(addresses)
        +getTables()
        +sql(query)
        +close()
    }

    class Table {
        +name()
        +getKeyValueView()
        +getRecordView()
        +getTupleView()
    }

    class KeyValueView {
        +get(key)
        +put(key, value)
        +remove(key)
        +getAll(keys)
        +putAll(map)
    }

    class RecordView {
        +get(key)
        +upsert(record)
        +delete(key)
        +getAll(keys)
    }

    IgniteClient --> Table : manages
    Table --> KeyValueView : provides
    Table --> RecordView : provides
```

## State Diagram

```mermaid
stateDiagram-v2
    [*] --> Idle
    Idle --> Connecting : connect()
    Connecting --> Connected : success
    Connecting --> Error : failure
    Connected --> Executing : execute()
    Executing --> Connected : complete
    Executing --> Error : exception
    Connected --> Closing : close()
    Closing --> [*]
    Error --> Closing : close()
```

## Entity Relationship Diagram

```mermaid
erDiagram
    PERSON ||--o{ ORDER : places
    PERSON {
        int id PK
        string name
        int age
        decimal salary
    }
    ORDER ||--|{ ORDER_ITEM : contains
    ORDER {
        int id PK
        int person_id FK
        date order_date
        decimal total
    }
    PRODUCT ||--o{ ORDER_ITEM : "ordered in"
    PRODUCT {
        int id PK
        string name
        decimal price
        int stock
    }
    ORDER_ITEM {
        int order_id FK
        int product_id FK
        int quantity
        decimal subtotal
    }
```

## Gantt Chart

```mermaid
gantt
    title Apache Ignite 3 Migration Timeline
    dateFormat YYYY-MM-DD
    section Infrastructure
    Environment Setup           :done, setup, 2025-10-01, 2d
    Initialize Docusaurus       :done, init, after setup, 2d
    Configure Versioning        :done, version, after init, 2d
    Styling Implementation      :done, style, after version, 3d
    Search Implementation       :done, search, after style, 2d
    Plugins and Dependencies    :active, plugins, after search, 1d
    section Content
    Navigation Structure        :nav, after plugins, 3d
    Code Snippet Infrastructure :snippets, after nav, 5d
    Content Conversion          :convert, after snippets, 10d
    section Testing
    Quality Assurance          :qa, after convert, 5d
    Final Review               :review, after qa, 3d
```

## Git Graph

```mermaid
gitGraph
    commit id: "Initial setup"
    commit id: "Docusaurus init"
    branch feature/versioning
    checkout feature/versioning
    commit id: "Add version 3.1.0"
    commit id: "Add version 3.0.0"
    checkout main
    merge feature/versioning
    branch feature/styling
    checkout feature/styling
    commit id: "Add custom CSS"
    commit id: "Configure theme"
    checkout main
    merge feature/styling
    branch feature/search
    checkout feature/search
    commit id: "Install search plugin"
    commit id: "Configure search"
    checkout main
    merge feature/search
    commit id: "Add Mermaid support"
```

## Pie Chart

```mermaid
pie title Apache Ignite 3 Documentation Pages by Category
    "Developers Guide" : 30
    "Administrator's Guide" : 25
    "SQL Reference" : 20
    "Quick Start" : 10
    "Installation" : 8
    "Other" : 7
```

## Architecture Diagram

```mermaid
flowchart LR
    subgraph "Client Applications"
        A1[Java Client]
        A2[.NET Client]
        A3[C++ Client]
        A4[Python Client]
    end

    subgraph "Ignite Cluster"
        B1[Node 1]
        B2[Node 2]
        B3[Node 3]
    end

    subgraph "Storage"
        C1[(RocksDB)]
        C2[(In-Memory)]
    end

    A1 --> B1
    A2 --> B1
    A3 --> B2
    A4 --> B3

    B1 <--> B2
    B2 <--> B3
    B3 <--> B1

    B1 --> C1
    B2 --> C1
    B3 --> C2
```
