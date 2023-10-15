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

import org.apache.ignite.configuration.annotation.AbstractConfiguration;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Immutable;
import org.apache.ignite.configuration.validation.PowerOfTwo;
import org.apache.ignite.configuration.validation.Range;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.storage.configurations.StorageEngineConfigurationSchema;

/**
 * Basic configuration schema for {@link PageMemory} based storage engines.
 */
//@AbstractConfiguration
public class BasePageMemoryStorageEngineConfigurationSchema extends StorageEngineConfigurationSchema {
    /** Name of the default data region. */
    public static final String DEFAULT_DATA_REGION_NAME = "default";
//
//    /** Page size in bytes. */
//    @Immutable
//    @PowerOfTwo
//    @Range(min = 1024, max = 16 * 1024)
//    @Value(hasDefault = true)
//    public int pageSize = 16 * 1024;
}
