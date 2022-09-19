/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import static org.apache.ignite.internal.util.Constants.MiB;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.OneOf;

/**
 * In-memory data region configuration schema.
 */
@Config
public class VolatilePageMemoryDataRegionConfigurationSchema extends BasePageMemoryDataRegionConfigurationSchema {
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
