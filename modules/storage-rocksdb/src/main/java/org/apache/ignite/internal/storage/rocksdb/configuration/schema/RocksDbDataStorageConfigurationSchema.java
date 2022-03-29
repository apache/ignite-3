package org.apache.ignite.internal.storage.rocksdb.configuration.schema;

import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine.ENGINE_NAME;
import static org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfigurationSchema.DEFAULT_DATA_REGION_NAME;

import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.schemas.store.DataStorageConfigurationSchema;
import org.apache.ignite.configuration.validation.Immutable;

/**
 * Data storage configuration for rocksdb storage engine.
 */
@PolymorphicConfigInstance(ENGINE_NAME)
public class RocksDbDataStorageConfigurationSchema extends DataStorageConfigurationSchema {
    /** Data region. */
    @Immutable
    @Value(hasDefault = true)
    public String dataRegion = DEFAULT_DATA_REGION_NAME;
}
