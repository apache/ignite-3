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

package org.apache.ignite.internal.metastorage.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** For {@link MetaStorageCompactionTriggerConfiguration} testing. */
@ExtendWith(ConfigurationExtension.class)
public class MetaStorageCompactionTriggerConfigurationTest extends BaseIgniteAbstractTest {
    private static final String INTERVAL_SYSTEM_PROPERTY_NAME = "metastorageCompactionInterval";

    private static final String DATA_AVAILABILITY_TIME_SYSTEM_PROPERTY_NAME = "metastorageCompactionDataAvailabilityTime";

    private static final long INTERVAL_DEFAULT_VALUE = Long.MAX_VALUE;

    private static final long DATA_AVAILABILITY_TIME_DEFAULT_VALUE = Long.MAX_VALUE;

    @Test
    void testEmptySystemProperties(@InjectConfiguration SystemDistributedConfiguration systemConfig) {
        var config = new MetaStorageCompactionTriggerConfiguration(systemConfig);

        assertEquals(INTERVAL_DEFAULT_VALUE, config.interval());
        assertEquals(DATA_AVAILABILITY_TIME_DEFAULT_VALUE, config.dataAvailabilityTime());
    }
}
