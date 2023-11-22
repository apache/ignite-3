package org.apache.ignite.internal.storage.rocksdb.configuration.schema;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.OneOf;
import org.apache.ignite.configuration.validation.Range;
import org.apache.ignite.internal.storage.configurations.StorageProfileConfigurationSchema;

@PolymorphicConfigInstance("rocksDb")
public class RocksDbStorageProfileConfigurationSchema extends StorageProfileConfigurationSchema {
    public static final String ROCKSDB_LRU_CACHE = "lru";

    /** Cache type for the RocksDB LRU cache. */
    public static final String ROCKSDB_CLOCK_CACHE = "clock";

    /** Size of the rocksdb offheap cache. */
    @Value(hasDefault = true)
    public long size = 256 * 1024 * 1024;

    /** Size of rocksdb write buffer. */
    @Value(hasDefault = true)
    @Range(min = 1)
    public long writeBufferSize = 64 * 1024 * 1024;

    /** Cache type - only {@code LRU} is supported at the moment. {@code Clock} implementation has known bugs. */
    @OneOf(ROCKSDB_LRU_CACHE)
    @Value(hasDefault = true)
    public String cache = ROCKSDB_LRU_CACHE;

    /** The cache is sharded to 2^numShardBits shards, by hash of the key. */
    @Range(min = -1)
    @Value(hasDefault = true)
    public int numShardBits = -1;
}
