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

package org.apache.ignite.internal.distributionzones.configuration;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Tests for {@link DistributionZonesHighAvailabilityConfiguration}. */
@ExtendWith(ConfigurationExtension.class)
public class DistributionZonesHighAvailabilityConfigurationTest extends BaseIgniteAbstractTest {
    private static final String PARTITION_DISTRIBUTION_RESET_TIMEOUT = "partitionDistributionResetTimeout";

    private static final long PARTITION_DISTRIBUTION_RESET_TIMEOUT_DEFAULT_VALUE = 0;

    @Test
    void testEmptySystemProperties(@InjectConfiguration SystemDistributedConfiguration systemConfig) {
        var config = new DistributionZonesHighAvailabilityConfiguration(systemConfig);
        config.startAndInit();

        assertEquals(PARTITION_DISTRIBUTION_RESET_TIMEOUT_DEFAULT_VALUE, config.partitionDistributionResetTimeout());
    }

    @Test
    void testValidSystemPropertiesOnStart(
            @InjectConfiguration("mock.properties = {"
                    + PARTITION_DISTRIBUTION_RESET_TIMEOUT + ".propertyValue = \"5\"}")
            SystemDistributedConfiguration systemConfig
    ) {
        var config = new DistributionZonesHighAvailabilityConfiguration(systemConfig);
        config.startAndInit();

        assertEquals(5, config.partitionDistributionResetTimeout());
    }

    @Test
    void testValidSystemPropertiesOnChange(@InjectConfiguration SystemDistributedConfiguration systemConfig) {
        var config = new DistributionZonesHighAvailabilityConfiguration(systemConfig);
        config.startAndInit();

        changeSystemConfig(systemConfig, "10");

        assertEquals(10, config.partitionDistributionResetTimeout());
    }

    private static void changeSystemConfig(
            SystemDistributedConfiguration systemConfig,
            String partitionDistributionResetScaleDown
    ) {
        CompletableFuture<Void> changeFuture = systemConfig.change(c0 -> c0.changeProperties()
                .create(PARTITION_DISTRIBUTION_RESET_TIMEOUT, c1 -> c1.changePropertyValue(partitionDistributionResetScaleDown))
        );

        assertThat(changeFuture, willCompleteSuccessfully());
    }
}
