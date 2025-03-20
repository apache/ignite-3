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

import static org.apache.ignite.internal.metastorage.impl.MetaStorageCompactionTriggerConfiguration.DATA_AVAILABILITY_TIME_SYSTEM_PROPERTY_NAME;
import static org.apache.ignite.internal.metastorage.impl.MetaStorageCompactionTriggerConfiguration.INTERVAL_SYSTEM_PROPERTY_NAME;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.utils.SystemConfigurationPropertyCompatibilityChecker;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** For {@link MetaStorageCompactionTriggerConfiguration} testing. */
@ExtendWith(ConfigurationExtension.class)
public class MetaStorageCompactionTriggerConfigurationTest extends BaseIgniteAbstractTest {
    private static final long INTERVAL_DEFAULT_VALUE = TimeUnit.MINUTES.toMillis(1);

    private static final long DATA_AVAILABILITY_TIME_DEFAULT_VALUE = TimeUnit.HOURS.toMillis(1);

    @Test
    void testEmptySystemProperties(@InjectConfiguration SystemDistributedConfiguration systemConfig) {
        var config = new MetaStorageCompactionTriggerConfiguration(systemConfig);
        config.init();

        assertEquals(INTERVAL_DEFAULT_VALUE, config.interval());
        assertEquals(DATA_AVAILABILITY_TIME_DEFAULT_VALUE, config.dataAvailabilityTime());
    }

    @Test
    void testValidSystemPropertiesOnStart(
            @InjectConfiguration("mock.properties = {"
                    + INTERVAL_SYSTEM_PROPERTY_NAME + " = \"100\", "
                    + DATA_AVAILABILITY_TIME_SYSTEM_PROPERTY_NAME + " = \"500\""
                    + "}")
            SystemDistributedConfiguration systemConfig
    ) {
        var config = new MetaStorageCompactionTriggerConfiguration(systemConfig);
        config.init();

        assertEquals(100, config.interval());
        assertEquals(500, config.dataAvailabilityTime());
    }

    @Test
    void testValidSystemPropertiesOnChange(@InjectConfiguration SystemDistributedConfiguration systemConfig) {
        var config = new MetaStorageCompactionTriggerConfiguration(systemConfig);
        config.init();

        changeSystemConfig(systemConfig, "100", "500");

        assertEquals(100, config.interval());
        assertEquals(500, config.dataAvailabilityTime());
    }

    @Test
    void testCompatibilityDataAvailabilityTimesSystemPropertyNameWasNotChanged() {
        SystemConfigurationPropertyCompatibilityChecker.checkSystemConfigurationPropertyNameWasNotChanged(
                "DATA_AVAILABILITY_TIME_SYSTEM_PROPERTY_NAME",
                "metastorageCompactionDataAvailabilityTime",
                DATA_AVAILABILITY_TIME_SYSTEM_PROPERTY_NAME
        );
    }

    @Test
    void testCompatibilityIntervalSystemPropertyNameWasNotChanged() {
        SystemConfigurationPropertyCompatibilityChecker.checkSystemConfigurationPropertyNameWasNotChanged(
                "INTERVAL_SYSTEM_PROPERTY_NAME",
                "metastorageCompactionInterval",
                INTERVAL_SYSTEM_PROPERTY_NAME
        );
    }

    private static void changeSystemConfig(
            SystemDistributedConfiguration systemConfig,
            String intervalValue,
            String dataAvailabilityTimeValue
    ) {
        CompletableFuture<Void> changeFuture = systemConfig.change(c0 -> c0.changeProperties()
                .create(INTERVAL_SYSTEM_PROPERTY_NAME, c1 -> c1.changePropertyValue(intervalValue))
                .create(DATA_AVAILABILITY_TIME_SYSTEM_PROPERTY_NAME, c1 -> c1.changePropertyValue(dataAvailabilityTimeValue))
        );

        assertThat(changeFuture, willCompleteSuccessfully());
    }
}
