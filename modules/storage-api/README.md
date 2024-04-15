# Storage API

This module contains the API for working with data storages.

## Adding a new data store

To add a new data storage you need:

* Add a new module;
* Implement interfaces:
    * `org.apache.ignite.internal.storage.DataStorageModule`;
    * `org.apache.ignite.internal.storage.engine.StorageEngine`;
    * `org.apache.ignite.internal.storage.engine.MvTableStorage`;
    * `org.apache.ignite.internal.storage.MvPartitionStorage`;
    * `org.apache.ignite.internal.storage.index.SortedIndexStorage`;
* Add configuration:
    * Add an inheritor of `org.apache.ignite.configuration.schemas.store.DataStorageConfigurationSchema`, with type equal
      to `org.apache.ignite.internal.storage.engine.StorageEngine.name`;
    * If necessary, add a specific configuration of the data storage engine;
    * Implement `org.apache.ignite.configuration.ConfigurationModule`;
* Add services (which are loaded via `java.util.ServiceLoader.load(java.lang.Class<S>)`):
    * Implementation of `org.apache.ignite.internal.storage.DataStorageModule`;
    * Implementation of `org.apache.ignite.configuration.ConfigurationModule`.

Take `org.apache.ignite.internal.storage.impl.TestStorageEngine` as an example.

## Usage

For each table, you need to specify the data storage, which is located in `org.apache.ignite.configuration.schemas.table.TableConfigurationSchema.dataStorage`.

Configuration example in HOCON:
```
tables.table {
    name = schema.table,
    columns.id {name = id, type.type = STRING, nullable = true},
    primaryKey {columns = [id], colocationColumns = [id]},
    indices.foo {type = HASH, name = foo, colNames = [id]},
    dataStorage {name = rocksdb, dataRegion = default}
}
```

Configuration example in java:
```java
TableConfiguration tableConfig = ...;

// Change data storage.
tableConfig.dataStorage().change(c -> c.convert(RocksDbDataStorageChange.class).changeDataRegion("default")).get(1, TimeUnit.SECONDS);

// Get data storage.
RocksDbDataStorageView dataStorageView = (RocksDbDataStorageView) tableConfig().dataStorage().value();

String dataRegion = dataStorageView.dateRegion();
```


To get the data storage engine, you need to use `org.apache.ignite.internal.storage.DataStorageManager.engine(org.apache.ignite.configuration.schemas.store.DataStorageConfiguration)`.
