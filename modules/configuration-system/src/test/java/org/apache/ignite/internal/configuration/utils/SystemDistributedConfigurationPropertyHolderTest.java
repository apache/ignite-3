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

package org.apache.ignite.internal.configuration.utils;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.ObjLongConsumer;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Tests for {@link SystemDistributedConfigurationPropertyHolder}. */
@ExtendWith(ConfigurationExtension.class)
public class SystemDistributedConfigurationPropertyHolderTest extends BaseIgniteAbstractTest {
    private static final String PROPERTY_NAME = "distributedPropertyName";

    private static final String DEFAULT_VALUE = "defaultValue";

    private static final ObjLongConsumer<String> noOpConsumer = (value, revision) -> {};

    @Test
    void testEmptySystemProperties(@InjectConfiguration SystemDistributedConfiguration systemConfig) {
        var config = new SystemDistributedConfigurationPropertyHolder<>(
                systemConfig,
                noOpConsumer,
                PROPERTY_NAME,
                DEFAULT_VALUE,
                Function.identity()
        );
        config.init();

        assertEquals(DEFAULT_VALUE, config.currentValue());
    }

    @Test
    void testValidSystemPropertiesOnStart(
            @InjectConfiguration("mock.properties." + PROPERTY_NAME + " = newValue")
            SystemDistributedConfiguration systemConfig
    ) {
        var config = noopConfigHolder(systemConfig);

        config.init();

        assertEquals("newValue", config.currentValue());
    }

    @Test
    void testValidSystemPropertiesOnChange(@InjectConfiguration SystemDistributedConfiguration systemConfig) {
        var config = noopConfigHolder(systemConfig);

        config.init();

        changeSystemConfig(systemConfig, "newValue");

        assertEquals("newValue", config.currentValue());
    }

    @Test
    void testUpdateConfigListenerWithConverter(
            @InjectConfiguration SystemDistributedConfiguration systemConfig
    ) throws InterruptedException {
        AtomicReference<Integer> currentValue = new AtomicReference<>();
        AtomicReference<Long> revisionValue = new AtomicReference<>();

        var config = new SystemDistributedConfigurationPropertyHolder<>(
                systemConfig,
                (v, revision) -> {
                    currentValue.set(v);
                    revisionValue.set(revision);
                },
                PROPERTY_NAME,
                0,
                Integer::parseInt
        );
        config.init();

        assertNotEquals(10, currentValue.get());
        assertNotEquals(1, revisionValue.get());

        changeSystemConfig(systemConfig, "10");

        assertTrue(waitForCondition(() ->
                currentValue.get() != null
                        && currentValue.get().equals(10), 1_000));
        assertEquals(1, revisionValue.get());
    }

    private static void changeSystemConfig(
            SystemDistributedConfiguration systemConfig,
            String partitionDistributionReset
    ) {
        CompletableFuture<Void> changeFuture = systemConfig.change(c0 -> c0.changeProperties()
                .create(PROPERTY_NAME, c1 -> c1.changePropertyValue(partitionDistributionReset))
        );

        assertThat(changeFuture, willCompleteSuccessfully());
    }

    private static SystemDistributedConfigurationPropertyHolder<String> noopConfigHolder(SystemDistributedConfiguration systemConfig) {
        return new SystemDistributedConfigurationPropertyHolder<>(
                systemConfig,
                noOpConsumer,
                PROPERTY_NAME,
                DEFAULT_VALUE,
                Function.identity()
        );
    }
}
