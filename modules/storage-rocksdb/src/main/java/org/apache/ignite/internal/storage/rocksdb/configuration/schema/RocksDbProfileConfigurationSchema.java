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

package org.apache.ignite.internal.storage.rocksdb.configuration.schema;

import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;
import org.apache.ignite.internal.storage.configurations.StorageProfileConfigurationSchema;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;

/**
 * Storage profile configuration for {@link RocksDbStorageEngine}.
 */
@PolymorphicConfigInstance("rocksdb")
public class RocksDbProfileConfigurationSchema extends StorageProfileConfigurationSchema {
    /**
     * Size of the rocksdb offheap cache.
     *
     * <p>When set to {@link #UNSPECIFIED_SIZE}, its value will be equal to a maximum between 256 MiB and 20% of the total physical memory.
     */
    @Value(hasDefault = true)
    @PublicName(legacyNames = "size")
    public long sizeBytes = UNSPECIFIED_SIZE;

    /** Size of rocksdb write buffer. */
    @Value(hasDefault = true)
    @Range(min = 1)
    @PublicName(legacyNames = "writeBufferSize")
    public long writeBufferSizeBytes = 64 * 1024 * 1024;
}
