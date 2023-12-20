package org.apache.ignite.internal.storage.rocksdb.configuration.schema;

import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.internal.storage.configurations.StorageEngineConfigurationSchema;

@ConfigurationExtension
public class RocksDbStorageEngineExtensionConfigurationSchema extends StorageEngineConfigurationSchema {

    @ConfigValue
    public RocksDbProfileStorageEngineConfigurationSchema rocksdb;
}
