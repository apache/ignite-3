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

package org.apache.ignite.internal.storage.pagememory.configuration.schema;

import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.storage.configurations.StorageProfileConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryStorageEngine;

/**
 * In-memory storage profile configuration schema.
 */
@PolymorphicConfigInstance(VolatilePageMemoryStorageEngine.ENGINE_NAME)
public class VolatilePageMemoryProfileConfigurationSchema extends StorageProfileConfigurationSchema {
    /**
     * Initial memory region size in bytes.
     *
     * <p>When the used memory size exceeds this value, new chunks of memory will be allocated until it reaches {@link #maxSizeBytes}.
     *
     * <p>When set to {@link #UNSPECIFIED_SIZE}, its value will be equal to {@link #maxSizeBytes}.
     */
    @Value(hasDefault = true)
    @PublicName(legacyNames = "initSize")
    public long initSizeBytes = UNSPECIFIED_SIZE;

    /**
     * Maximum memory region size in bytes.
     *
     * <p>When set to {@link #UNSPECIFIED_SIZE}, its value will be equal to a maximum between 256 MiB and 20% of the total physical memory.
     */
    @Value(hasDefault = true)
    @PublicName(legacyNames = "maxSize")
    public long maxSizeBytes = UNSPECIFIED_SIZE;
}
