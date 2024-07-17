This package contains classes that are used for implementation of functionality based on causality tokens.

### The purpose

Every component registers configuration update listeners while starting. These listeners receive notifications during the further lifetime
of the node, and also during the [recovery](../manager/RECOVERY.md) process while retrieving notifications from Metastorage. The details of
the component start described [here](../../../../../../../../../runner/README.md#node-components-startup). While handling the notifications,
listeners should rely on the fact that the components that they depend on, won’t return stale or inconsistent data, regardless of the order
in which these listeners will be called. It should be guaranteed by the causality tokens mechanism.

### Causality tokens

Cluster-wide events, such as, for example, configuration changes or Metastorage updates, trigger notifications that are propagated to the
nodes and therefore, to components. Every event can trigger one or several notifications. Every notification has a causality token.
Notifications triggered by the same event have the same causality token. Metastorage updates are bound with updates of revision,
see [Reliable watch processing](../../../../../../../../../runner/README.md#reliable-watch-processing).
It is guaranteed that the last notification produced with a certain causality token, will be a notification about a storage revision update.

Components’ listeners that process these notifications, can be called in any order. So there may be a situation when a listener registered
in component A tries to call component B, but component B is not ready because its listeners haven’t received a corresponding notification
yet. The listener of component A needs to make sure that component B is in a consistent up-to-date state, so it needs to await this state,
and then continue the execution of a code dependent on component B. It is achieved by including a causality token to a call to component B.
The token should be taken from notification. The call to component B now returns a future. This future is completed when component B
receives notifications that have a causality token in the context of which the future was created. After completion of this future, the
listener in component A can continue working – of course, it should happen asynchronously.

More formally, one can make a call for a component using a causality token and receive a future, that will be completed when this component
handles notification about a storage revision update having the given causality token. The result of this future should be guaranteed to be
consistent regarding the given token. This guarantees that notifications will be handled by component listeners in the proper order.

It is also worth mentioning that local events should also be produced within the context of an appropriate causality token.

To simplify the implementation of this pattern, the concept of `VersionedValue` was introduced.

### Versioned value

Versioned value is some value that can be associated with a causality token. Typically, a field of a component that is supposed to be
changed according to configuration changes, should be represented as `org.apache.ignite.internal.causality.VersionedValue` of
needed type. Versioned value stores multiple versions of actual values, each for a certain causality token, i.e. a history of this value.
The default history size is 2, but if the value hasn't been changed within a row of serial tokens, it is counted as one version for the
history.

Aforementioned futures, created for some causality token, are returned from `#get(long causalityToken)` method of `VersionedValue`.

Depending on the implementation of a `VersionedValue`, new version of a value, associated with a causality token, can be set either
explicitly via `#complete` method (see `CompletableVersionedValue`), or via update of a storage revision (see `IncrementalVersionedValue`).
The storage revision updater is set in the constructor. If the value hasn't been changed when the storage revision update
happens (`#update` method was never called), it means that the previous value becomes associated with the new token. When a
value becomes associated with a token, a future created for this token completes.

Versioned value can be also changed multiple times within one causality token, but these changes are not saved to the history and don’t
complete the futures. It should be used for processing multiple notifications having the same causality token. For details, see javadoc
for `IncrementalVersionedValue#update` method.
