#Apache Ignite SQL API

##Overview
[IgniteSql]('IgniteSql') interface is an entry point for SQL query execution and methods for running queries in sync, async or reactive 
ways and provide method for creating [SQL statement](Statement.java).
 
An SQL statement holds a context that queries will be executed against (e.g. default query timeout, or some hints, which may affects 
SQL query execution flow, and SQL extension/plugin specific hints).

The result of SQL query is represented with [ResultSet](ResultSet.java), [AsyncResultSet](./async/AsyncResultSet.java),
and [ReactiveResultSet](./reactive/ReactiveResultSet.java) classes, which provides the result itself and metadata for it.
The query may return either `boolean` value (for a conditional query), or number of affected rows (for DML query), or a set of rows. 

##Async query execution
Note: Asynchronous API offers user methods for asynchronous result processing, which are very similar to synchronous one. Some users may
find this approach easier understanding and using rather than reactive way.

##Reactive query execution
Reactive methods provide reactive primitives of Java Flow API for building reactive flows.
 
Note: These primitives may be hard to use "as is". Thus it is expected users will use some 3-rd party reactive framework for their purpose. 

## Query execution optimization
TBD: cover "query plan caching" topic.

    
