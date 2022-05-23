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

import static org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryDataRegionConfigurationSchema.VOLATILE_DATA_REGION_TYPE;

import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;

/**
 * Data region configuration for page memory.
 */
@PolymorphicConfig
public class PageMemoryDataRegionConfigurationSchema {
    /** Default initial size. */
    public static final long DFLT_DATA_REGION_INITIAL_SIZE = 256 * 1024 * 1024;

    /** Default max size. */
    public static final long DFLT_DATA_REGION_MAX_SIZE = 256 * 1024 * 1024;

    /** Type of the data region. */
    @PolymorphicId(hasDefault = true)
    public String typeId = VOLATILE_DATA_REGION_TYPE;

    /** Name of the data region. */
    @InjectedName
    public String name;

    @Value(hasDefault = true)
    public long initSize = DFLT_DATA_REGION_INITIAL_SIZE;

    @Value(hasDefault = true)
    public long maxSize = DFLT_DATA_REGION_MAX_SIZE;

    @ConfigValue
    public MemoryAllocatorConfigurationSchema memoryAllocator;
}
