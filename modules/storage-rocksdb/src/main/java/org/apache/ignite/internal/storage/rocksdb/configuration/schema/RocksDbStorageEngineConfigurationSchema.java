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

package org.apache.ignite.internal.storage.rocksdb.configuration.schema;

import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;

import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Name;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.ExceptKeys;
import org.apache.ignite.configuration.validation.Range;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;

/**
 * Root configuration for {@link RocksDbStorageEngine}.
 */
@ConfigurationRoot(rootName = "rocksDb", type = DISTRIBUTED)
public class RocksDbStorageEngineConfigurationSchema {
    /** Name of the default data region. */
    public static final String DEFAULT_DATA_REGION_NAME = "default";

    /** Delay before executing a flush triggered by RAFT. */
    @Range(min = 0)
    @Value(hasDefault = true)
    public int flushDelayMillis = 100;

    /** Default data region. */
    @Name(DEFAULT_DATA_REGION_NAME)
    @ConfigValue
    public RocksDbDataRegionConfigurationSchema defaultRegion;

    /** Other data regions. */
    @ExceptKeys(DEFAULT_DATA_REGION_NAME)
    @NamedConfigValue
    public RocksDbDataRegionConfigurationSchema regions;
}
