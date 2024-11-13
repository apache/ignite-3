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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
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

    private static final BiConsumer<Integer, Long> noOpConsumer = (partitionDistributionResetTimeout, revision) -> {};

    @Test
    void testEmptySystemProperties(@InjectConfiguration SystemDistributedConfiguration systemConfig) {
        var config = new DistributionZonesHighAvailabilityConfiguration(systemConfig, noOpConsumer);
        config.startAndInit();

        assertEquals(PARTITION_DISTRIBUTION_RESET_TIMEOUT_DEFAULT_VALUE, config.partitionDistributionResetTimeoutSeconds());
    }

    @Test
    void testValidSystemPropertiesOnStart(
            @InjectConfiguration("mock.properties = {"
                    + PARTITION_DISTRIBUTION_RESET_TIMEOUT + ".propertyValue = \"5\"}")
            SystemDistributedConfiguration systemConfig
    ) {
        var config = new DistributionZonesHighAvailabilityConfiguration(systemConfig, noOpConsumer);
        config.startAndInit();

        assertEquals(5, config.partitionDistributionResetTimeoutSeconds());
    }

    @Test
    void testValidSystemPropertiesOnChange(@InjectConfiguration SystemDistributedConfiguration systemConfig) {
        var config = new DistributionZonesHighAvailabilityConfiguration(systemConfig, noOpConsumer);
        config.startAndInit();

        changeSystemConfig(systemConfig, "10");

        assertEquals(10, config.partitionDistributionResetTimeoutSeconds());
    }

    @Test
    void testUpdateConfigListener(@InjectConfiguration SystemDistributedConfiguration systemConfig) throws InterruptedException {
        AtomicReference<Integer> partitionDistributionResetTimeoutValue = new AtomicReference<>();
        AtomicReference<Long> revisionValue = new AtomicReference<>();

        var config = new DistributionZonesHighAvailabilityConfiguration(
                systemConfig,
                (partitionDistributionResetTimeout, revision) -> {
                    partitionDistributionResetTimeoutValue.set(partitionDistributionResetTimeout);
                    revisionValue.set(revision);
                }
        );
        config.startAndInit();

        assertNotEquals(10, partitionDistributionResetTimeoutValue.get());
        assertNotEquals(1, revisionValue.get());

        changeSystemConfig(systemConfig, "10");

        assertTrue(waitForCondition(() ->
                partitionDistributionResetTimeoutValue.get() != null
                        && partitionDistributionResetTimeoutValue.get() == 10, 1_000));
        assertEquals(1, revisionValue.get());
    }

    private static void changeSystemConfig(
            SystemDistributedConfiguration systemConfig,
            String partitionDistributionReset
    ) {
        CompletableFuture<Void> changeFuture = systemConfig.change(c0 -> c0.changeProperties()
                .create(PARTITION_DISTRIBUTION_RESET_TIMEOUT, c1 -> c1.changePropertyValue(partitionDistributionReset))
        );

        assertThat(changeFuture, willCompleteSuccessfully());
    }
}
