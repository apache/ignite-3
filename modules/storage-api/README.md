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
    * Add an inheritor of `org.apache.ignite.internal.storage.configurations.StorageProfileConfigurationSchema`,
      with `@PolymorphicConfigInstance` value equal to `org.apache.ignite.internal.storage.engine.StorageEngine.name`;
    * If necessary, add a specific configuration of the data storage engine:
        * Implement `org.apache.ignite.internal.storage.configurations.StorageEngineConfigurationSchema` with the `@ConfigurationExtension`
          annotation and the `@ConfigValue` field with the name equal to `org.apache.ignite.internal.storage.engine.StorageEngine.name`;
    * Implement `org.apache.ignite.configuration.ConfigurationModule`;
* Add services (which are loaded via `java.util.ServiceLoader.load(java.lang.Class<S>)`):
    * Implementation of `org.apache.ignite.internal.storage.DataStorageModule`;
    * Implementation of `org.apache.ignite.configuration.ConfigurationModule`.

Take `org.apache.ignite.internal.storage.impl.TestStorageEngine` as an example.

## Usage

Storage configuration in HOCON:
```
ignite:
    storage.profiles:
        test_profile1
            engine: test
        test_profile2
            engine: test
```

For each table, you may to specify the storage profile.

Table creation example in DDL:
```
create zone z1 with storage_profiles='test_profile1,test_profile2';

create table t1 with storage_profile='test_profile2' using zone='z1';
create table t2 using zone='z1'; // first storage profile from the zone z1 will be used here: 'test_profile1'.
```
