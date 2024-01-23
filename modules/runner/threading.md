## Threading

The following global thread pools are defined:

* `{consistentId}-partition-operations-X` executes operations that are part of transactions (including those that make I/O
  or potentially block on locks). All operations related to the same partition get executed on the same thread in this pool.
