# Catalog module

Catalog is a component that responsible for managing descriptors of objects available in cluster.
Additionally, it serves as access point for other components to obtain proper version of descriptors
with respect to provided version of catalog or timestamp.

This module provides implementation of catalog service as well as internal API for accessing the
objects descriptors.

## Base components description

* [CatalogObjectDescriptor](src/main/java/org/apache/ignite/internal/catalog/descriptors/CatalogObjectDescriptor.java) --
  base class for objects managed by catalog, like tables, indexes, zones, etc.
* [CatalogService](src/main/java/org/apache/ignite/internal/catalog/CatalogService.java) -- provides
  methods to access catalog's objects' descriptors of the exact version and/or last actual version at 
  a given timestamp, which is a logical point-in-time. Besides, components may subscribe for catalog's
  events to be notified as soon as the change of interest is happened.
* [CatalogCommand](src/main/java/org/apache/ignite/internal/catalog/CatalogCommand.java) -- denotes
  particular modification of the catalog, like creation of particular table, for example.
* [CatalogManager](src/main/java/org/apache/ignite/internal/catalog/CatalogManager.java) -- provides
  methods for object manipulation (like creation of new object and/or modification of existing ones),
  also takes care of component lifecycle.
* [UpdateEntry](src/main/java/org/apache/ignite/internal/catalog/storage/UpdateEntry.java) -- 
  result of applying [CatalogCommand](src/main/java/org/apache/ignite/internal/catalog/CatalogCommand.java),
  represents delta required to move current version of catalog to state defined in the given command. 
* [UpdateLog](src/main/java/org/apache/ignite/internal/catalog/storage/UpdateLog.java) -- distributed
  log of incremental updates.

## Guarantee

For modify operation (invocation of `CatalogManager.execute()`), a resulting future will be completed
as soon as version in which results of the command take place becomes available on every node of the
cluster. This "availability" is determined as "version activation time plus some duration to make
sure changes are propagated and applied within the cluster", where `some duration` defined by 
`schemaSync.delayDurationMillis` configuration property. This implies, that consequent read access to 
catalog service with latest timestamp (`HybridClock.now()`) from any node will return the catalog of
version that incorporates results of the command (assuming there were no concurrent updates overwriting
modifications made by initial command). This also implies, that concurrent access to the catalog service
may return catalog of version incorporating results of the command even if resulting future has not been
yet resolved. Additionally, there is an extra await to make sure, that locally new version will 
_always_ be available immediately after resulting future is completed. 

For read access, it's up to the caller to make sure, that version of interest is available in local
cache. Use `CatalogService.catalogReadyFuture` to wait for particular version, or 
`SchemaSyncService.waitForMetadataCompleteness` if you need a version which is active at provided 
point in time.

Consumers of catalog's events are allowed to read catalog at version from event. 

## How it works

### CatalogManager

Every modification of catalog is split on two phases: accept and apply. During accept phase, given command
is validated, then used to generate a list of update entries to save, and, finally, list is saved to 
distributed update log. Below is a sequence diagram describing accept phase:
![Accepting catalog update](tech-notes/accept_update_flow.png)

After update entries are saved to the log, it is the job of the update log to propagate updates across the
cluster. On every node, update log notifies catalog manager about new update entries, and latter applies
them and stores new version of a catalog in a local cache. Below is a sequence diagram describing apply phase:
![Applying catalog update](tech-notes/apply_update_flow.png)

### UpdateLog

Current implementation of update log based on a metastorage. Update entries of version N are stored by
`catalog.update.{N}` key. Also, the latest known version is stored by `catalog.version` key. Updates
are saved on CAS manner with condition `newVersion == value(catalog.version)`.

#### Update log compaction

Over time, the log may grow to a humongous size. To address this, snapshotting was introduced to UpdateLog.
When saving snapshot of version N, update entries stored by `catalog.update.{N}` will be overwritten with
catalog's snapshot of this version. Every update entries of version lower that version of snapshot will be
removed. The earliest available version of catalog is tracked under `catalog.snapshot.version` key.

#### Update log recovery

During recovery, we read update entries one by one for all version starting with "earliest available" till
version stored by `catalog.version` key, and apply those updates entries once again.

#### Update log entries serialization

Update log entry contains an hierarchical structure of objects.

For example, `NewSchemaEntry` contains `CatalogSchemaDescriptor` which
contains `CatalogTableDescriptor` which contains
`CatalogTableColumnDescriptor` and so on.

<details>
  <summary>NewSchemaEntry hierarchy example</summary>

* NewSchemaEntry
  * CatalogSchemaDescriptor
    * CatalogTableDescriptor[]
      * CatalogTableSchemaVersions
        * CatalogTableColumnDescriptor
      * CatalogTableColumnDescriptor\[\]
      *  ...
  * ...
  * CatalogIndexDescriptor\[\]
</details>

To simplify versioning of catalog objects, each serializable catalog object must implement  
[MarshallableEntry](src/main/java/org/apache/ignite/internal/catalog/storage/serialization/MarshallableEntry.java)
which allows the serializer to understand what type is being (de)serialized.

For each serializable object, there must be an external serializer that implements the 
[CatalogObjectSerializer](src/main/java/org/apache/ignite/internal/catalog/storage/serialization/CatalogObjectSerializer.java)
interface and is marked with the 
[CatalogSerializer](src/main/java/org/apache/ignite/internal/catalog/storage/serialization/CatalogSerializer.java) annotation.
This annotation specifies the serializer version and is used to dynamically build a registry of all existing serializers
(see [CatalogEntrySerializerProvider](src/main/java/org/apache/ignite/internal/catalog/storage/serialization/CatalogEntrySerializerProvider.java)).

When serializing an object, a header is written for it consisting of the  object type (2 bytes) and the serializer version (1-3 bytes).

[UpdateLogMarshaller](src/main/java/org/apache/ignite/internal/catalog/storage/serialization/UpdateLogMarshaller.java)
is the entry point for catalog object serialization.

Overall serialization format looks as follow

| Field description                   | type   |
|-------------------------------------|--------|
| PROTOCOL VERSION<sup>[1]</sup>      | short  | 
| Object type                         | short  |
| Object serialization format version | varint |
| Object payload                      | ...    |

<sup>[1]</sup> The current description corresponds to protocol version 2.

##### Serialization versioning rules

-   The serializer version is incremented by 1.

-   Each serializer must be annotated with
    `@CatalogSerializer(version=N, since="X.X.X")`. This
    annotation is used to build registry of all serializers. The
    version in `since` field is used to understand which
    serializers are already in use in the "released version" and whose
    data format should not change. For example, all currently existing
    catalog serializers is marked as `since="3.0.0"`.

##### Limitations of serialization protocol version 2

-   No forward compatibility support (but it may be added in future versions if needed)
