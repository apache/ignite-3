# Ignite workers module
This module provides infrastructure for workers used internally by Ignite.

A worker is a component that does some work. It executes on a thread; in some cases we can even represent
an important thread as a worker.

We might want to monitor whether a worker is stuck (because some operation blocks it for a long period of time).
`CriticalWorkerRegistry` allows `CriticalWorker` instances to be registered with it to be monitored.
