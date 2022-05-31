/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.pagememory.configuration.schema;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.OneOf;

/**
 * Data region configuration for Page Memory storage engine.
 */
@Config
public class PageMemoryDataRegionConfigurationSchema {
    /** Default initial size. */
    public static final long DFLT_DATA_REGION_INITIAL_SIZE = 256 * 1024 * 1024;

    /** Default max size. */
    public static final long DFLT_DATA_REGION_MAX_SIZE = 256 * 1024 * 1024;

    /** Eviction is disabled. */
    public static final String DISABLED_EVICTION_MODE = "DISABLED";

    /** Random-LRU algorithm. */
    public static final String RANDOM_LRU_EVICTION_MODE = "RANDOM_LRU";

    /** Random-2-LRU algorithm: scan-resistant version of Random-LRU. */
    public static final String RANDOM_2_LRU_EVICTION_MODE = "RANDOM_2_LRU";

    /** Random-LRU algorithm. */
    public static final String RANDOM_LRU_REPLACEMENT_MODE = "RANDOM_LRU";

    /** Segmented-LRU algorithm. */
    public static final String SEGMENTED_LRU_REPLACEMENT_MODE = "SEGMENTED_LRU";

    /** CLOCK algorithm. */
    public static final String CLOCK_REPLACEMENT_MODE = "CLOCK";

    /** Name of the data region. */
    @InjectedName
    public String name;

    @Value(hasDefault = true)
    public boolean persistent = false;

    @Value(hasDefault = true)
    public long initSize = DFLT_DATA_REGION_INITIAL_SIZE;

    @Value(hasDefault = true)
    public long maxSize = DFLT_DATA_REGION_MAX_SIZE;

    @ConfigValue
    public MemoryAllocatorConfigurationSchema memoryAllocator;

    @OneOf({DISABLED_EVICTION_MODE, RANDOM_LRU_EVICTION_MODE, RANDOM_2_LRU_EVICTION_MODE})
    @Value(hasDefault = true)
    public String evictionMode = DISABLED_EVICTION_MODE;

    @OneOf({RANDOM_LRU_REPLACEMENT_MODE, SEGMENTED_LRU_REPLACEMENT_MODE, CLOCK_REPLACEMENT_MODE})
    @Value(hasDefault = true)
    public String replacementMode = CLOCK_REPLACEMENT_MODE;

    @Value(hasDefault = true)
    public double evictionThreshold = 0.9;

    @Value(hasDefault = true)
    public int emptyPagesPoolSize = 100;

    @Value(hasDefault = true)
    public long checkpointPageBufSize = 0;

    @Value(hasDefault = true)
    public boolean lazyMemoryAllocation = true;

    /**
     * Write to the page store without holding the segment lock (with a delay).
     *
     * <p>Because other thread may require exactly the same page to be loaded from page store, reads are protected by locking.
     */
    @Value(hasDefault = true)
    public boolean delayedReplacedPageWrite = true;
}
