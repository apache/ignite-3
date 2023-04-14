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

package org.apache.ignite.internal.storage.impl;

import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageConfigurationSchema;

/**
 * Test persistent storage configuration to fix the configuration loading issues in the test modules,
 * where the real 'aipersist' is not available.
 * This issue appears during the loading configuration for default distribution zone with the default data storage configuration.
 */
@PolymorphicConfigInstance("aipersist")
public class TestPersistStorageConfigurationSchema extends DataStorageConfigurationSchema {
    /** Data region. */
    @Value(hasDefault = true)
    public String dataRegion = "none";
}
