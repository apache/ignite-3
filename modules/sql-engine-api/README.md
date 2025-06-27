# Apache Ignite SQL Engine API module

The module contains an internal API that is used for integration with other components.

## Operation Kill Handler API

### Motivation

We have at least three types of operations that can be interrupted using the SQL KILL command:

* SQL query
* Transaction
* Compute job

The Kill Handler API is designed to simplify the integration of the SQL KILL command
with components that are responsible for canceling an operation of a certain type.

### Classes description

[OperationKillHandler](src/main/java/org/apache/ignite/internal/sql/engine/api/kill/OperationKillHandler.java) -  
handler that can abort an operation of a certain type. It needs to be 
implemented by the component that wants to handle the KILL command.

[KillHandlerRegistry](src/main/java/org/apache/ignite/internal/sql/engine/api/kill/KillHandlerRegistry.java) -
registry of all kill handlers. All handlers must be registered during node startup.

[CancellableOperationType](src/main/java/org/apache/ignite/internal/sql/engine/api/kill/CancellableOperationType.java) -
enumeration of operations that can be cancelled.