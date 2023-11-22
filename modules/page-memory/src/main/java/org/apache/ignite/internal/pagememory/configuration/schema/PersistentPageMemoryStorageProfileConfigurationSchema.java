package org.apache.ignite.internal.pagememory.configuration.schema;

import static org.apache.ignite.internal.util.Constants.MiB;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.OneOf;
import org.apache.ignite.internal.storage.configurations.StorageProfileConfigurationSchema;

@PolymorphicConfigInstance("aipersist")
public class PersistentPageMemoryStorageProfileConfigurationSchema extends StorageProfileConfigurationSchema {
    /** Memory allocator. */
    @ConfigValue
    public MemoryAllocatorConfigurationSchema memoryAllocator;

    /** Default size. */
    public static final long DFLT_DATA_REGION_SIZE = 256 * MiB;

    /** Random-LRU page replacement algorithm. */
    public static final String RANDOM_LRU_REPLACEMENT_MODE = "RANDOM_LRU";

    /** Segmented-LRU page replacement algorithm. */
    public static final String SEGMENTED_LRU_REPLACEMENT_MODE = "SEGMENTED_LRU";

    /** CLOCK page replacement algorithm. */
    public static final String CLOCK_REPLACEMENT_MODE = "CLOCK";

    /** Memory region size in bytes. */
    @Value(hasDefault = true)
    public long size = DFLT_DATA_REGION_SIZE;

    /** Memory pages replacement mode. */
    @OneOf({RANDOM_LRU_REPLACEMENT_MODE, SEGMENTED_LRU_REPLACEMENT_MODE, CLOCK_REPLACEMENT_MODE})
    @Value(hasDefault = true)
    public String replacementMode = CLOCK_REPLACEMENT_MODE;
}
