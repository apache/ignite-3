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
import static org.apache.ignite.internal.util.IgniteUtils.getTotalMemoryAvailable;

import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.storage.configurations.StorageProfileConfigurationSchema;

/**
 * In-memory storage profile configuration schema.
 */
@PolymorphicConfigInstance("aimem")
public class VolatilePageMemoryProfileConfigurationSchema extends StorageProfileConfigurationSchema {
    /**
     * Default initial volatile page memory data region size, maximum between 256 MiB and 20% of the total physical memory.
     * 256 MiB, if system was unable to retrieve physical memory size.
     */
    @SuppressWarnings("NumericCastThatLosesPrecision")
    public static final long DFLT_DATA_REGION_INITIAL_SIZE = Math.max(256 * MiB, (long) (0.2 * getTotalMemoryAvailable()));

    /** Default max size, matches {@link #DFLT_DATA_REGION_INITIAL_SIZE}. */
    public static final long DFLT_DATA_REGION_MAX_SIZE = DFLT_DATA_REGION_INITIAL_SIZE;

    /** Initial memory region size in bytes, when the used memory size exceeds this value, new chunks of memory will be allocated. */
    @Value(hasDefault = true)
    public long initSize = DFLT_DATA_REGION_INITIAL_SIZE;

    /** Maximum memory region size in bytes. */
    @Value(hasDefault = true)
    public long maxSize = DFLT_DATA_REGION_MAX_SIZE;
}
