package org.apache.ignite.internal.pagememory.configuration.schema;

import static org.apache.ignite.internal.util.Constants.MiB;

import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.OneOf;
import org.apache.ignite.internal.storage.configurations.StorageProfileConfigurationSchema;


@PolymorphicConfigInstance("aipersist")
public class VolatilePageMemoryStorageProfileConfigurationSchema extends StorageProfileConfigurationSchema {
    /** Memory allocator. */
    @ConfigValue
    public MemoryAllocatorConfigurationSchema memoryAllocator;

    /** Default initial size. */
    public static final long DFLT_DATA_REGION_INITIAL_SIZE = 256 * MiB;

    /** Default max size. */
    public static final long DFLT_DATA_REGION_MAX_SIZE = 256 * MiB;

    /** Eviction is disabled. */
    public static final String DISABLED_EVICTION_MODE = "DISABLED";

    /** Random-LRU algorithm. */
    public static final String RANDOM_LRU_EVICTION_MODE = "RANDOM_LRU";

    /** Random-2-LRU algorithm: scan-resistant version of Random-LRU. */
    public static final String RANDOM_2_LRU_EVICTION_MODE = "RANDOM_2_LRU";

    /** Initial memory region size in bytes, when the used memory size exceeds this value, new chunks of memory will be allocated. */
    @Value(hasDefault = true)
    public long initSize = DFLT_DATA_REGION_INITIAL_SIZE;

    /** Maximum memory region size in bytes. */
    @Value(hasDefault = true)
    public long maxSize = DFLT_DATA_REGION_MAX_SIZE;

    /** Memory pages eviction mode. */
    @OneOf({DISABLED_EVICTION_MODE, RANDOM_LRU_EVICTION_MODE, RANDOM_2_LRU_EVICTION_MODE})
    @Value(hasDefault = true)
    public String evictionMode = DISABLED_EVICTION_MODE;

    /**
     * Threshold for memory pages eviction initiation. For instance, if the threshold is 0.9 it means that the page memory will start the
     * eviction only after 90% of the data region is occupied.
     */
    @Value(hasDefault = true)
    public double evictionThreshold = 0.9;

    /** Maximum amount of empty pages to keep in memory. */
    @Value(hasDefault = true)
    public int emptyPagesPoolSize = 100;
}
