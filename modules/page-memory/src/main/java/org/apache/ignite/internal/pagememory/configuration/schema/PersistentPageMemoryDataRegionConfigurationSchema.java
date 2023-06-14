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
 * Persistent data region configuration schema.
 */
@Config
public class PersistentPageMemoryDataRegionConfigurationSchema extends BasePageMemoryDataRegionConfigurationSchema {
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
