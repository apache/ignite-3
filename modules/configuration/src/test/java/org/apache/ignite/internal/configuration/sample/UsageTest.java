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

package org.apache.ignite.internal.configuration.sample;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.ForkJoinPool;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Simple usage test of generated configuration schema.
 */
public class UsageTest {
    private ConfigurationRegistry registry;

    @AfterEach
    public void after() {
        assertThat(registry.stopAsync(), willCompleteSuccessfully());
    }

    /**
     * Test creation of configuration and calling configuration API methods.
     */
    @Test
    public void test() throws Exception {
        registry = new ConfigurationRegistry(
                List.of(LocalConfiguration.KEY),
                new TestConfigurationStorage(LOCAL),
                new ConfigurationTreeGenerator(LocalConfiguration.KEY),
                new TestConfigurationValidator()
        );

        assertThat(registry.startAsync(ForkJoinPool.commonPool()), willCompleteSuccessfully());

        LocalConfiguration root = registry.getConfiguration(LocalConfiguration.KEY);

        root.change(local ->
                local.changeTestConfigurationSchema(schema ->
                        schema.changeTestConfigValues(values ->
                                values.create("node1", node ->
                                        node.changeStringValue("test").changeLongValue(1000)
                                )
                        ).changeTestConfigValue(value ->
                                value.changeBooleanValue(true).changeLongValue(100_000L)
                        )
                )
        ).get(1, SECONDS);

        assertTrue(root.testConfigurationSchema().testConfigValue().booleanValue().value());

        root.testConfigurationSchema().testConfigValue().booleanValue().update(false).get(1, SECONDS);

        assertFalse(root.value().testConfigurationSchema().testConfigValue().booleanValue());
        assertFalse(root.testConfigurationSchema().value().testConfigValue().booleanValue());
        assertFalse(root.testConfigurationSchema().testConfigValue().value().booleanValue());
        assertFalse(root.testConfigurationSchema().testConfigValue().booleanValue().value());

        root.testConfigurationSchema().testConfigValues().get("node1").booleanValue().update(true).get(1, SECONDS);

        assertTrue(root.value().testConfigurationSchema().testConfigValues().get("node1").booleanValue());
        assertTrue(root.testConfigurationSchema().value().testConfigValues().get("node1").booleanValue());
        assertTrue(root.testConfigurationSchema().testConfigValues().value().get("node1").booleanValue());
        assertTrue(root.testConfigurationSchema().testConfigValues().get("node1").value().booleanValue());
        assertTrue(root.testConfigurationSchema().testConfigValues().get("node1").booleanValue().value());

        root.testConfigurationSchema().testConfigValues().get("node1").change(node -> node.changeBooleanValue(false)).get(1, SECONDS);
        assertFalse(root.value().testConfigurationSchema().testConfigValues().get("node1").booleanValue());

        root.testConfigurationSchema().testConfigValues().change(values -> values.delete("node1")).get(1, SECONDS);

        assertNull(root.testConfigurationSchema().testConfigValues().get("node1"));
        assertNull(root.value().testConfigurationSchema().testConfigValues().get("node1"));
    }

    /**
     * Test to show an API to work with multiroot configurations.
     */
    @Test
    public void multiRootConfiguration() throws Exception {
        final int failureDetectionTimeout = 30_000;
        final int joinTimeout = 10_000;

        long testLongValue = 30_000L;

        registry = new ConfigurationRegistry(
                List.of(NetworkConfiguration.KEY, LocalConfiguration.KEY),
                new TestConfigurationStorage(LOCAL),
                new ConfigurationTreeGenerator(NetworkConfiguration.KEY, LocalConfiguration.KEY),
                new TestConfigurationValidator()
        );

        assertThat(registry.startAsync(ForkJoinPool.commonPool()), willCompleteSuccessfully());

        registry.getConfiguration(LocalConfiguration.KEY).change(local ->
                local.changeTestConfigurationSchema(schema ->
                        schema.changeTestConfigValue(configValue ->
                                configValue.changeBooleanValue(true).changeLongValue(testLongValue)
                        )
                )
        ).get(1, SECONDS);

        registry.getConfiguration(NetworkConfiguration.KEY).change(network ->
                network.changeDiscovery(discovery ->
                        discovery
                                .changeFailureDetectionTimeout(failureDetectionTimeout)
                                .changeJoinTimeout(joinTimeout)
                )
        ).get(1, SECONDS);

        assertEquals(
                failureDetectionTimeout,
                registry.getConfiguration(NetworkConfiguration.KEY).discovery().failureDetectionTimeout().value()
        );

        assertEquals(
                testLongValue,
                registry.getConfiguration(LocalConfiguration.KEY).testConfigurationSchema().testConfigValue().longValue().value()
        );
    }
}
