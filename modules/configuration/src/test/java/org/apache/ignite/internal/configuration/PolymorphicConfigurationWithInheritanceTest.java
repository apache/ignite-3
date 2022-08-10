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

package org.apache.ignite.internal.configuration;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicConfigIntermediary;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PolymorphicConfigurationWithInheritanceTest {
    private final ConfigurationRegistry registry = new ConfigurationRegistry(
            List.of(ConfigurationHavingPolymorphicConfigWithNonTrivialHierarchyConfiguration.KEY),
            Map.of(),
            new TestConfigurationStorage(ConfigurationType.LOCAL),
            List.of(),
            List.of(LeafUnderMiddlePolymorphicInstanceConfigurationSchema.class)
    );

    @BeforeEach
    void startRegistry() {
        registry.start();
    }

    @AfterEach
    void stopRegistry() throws Exception {
        registry.stop();
    }

    @Test
    void readingPolymorphicConfigWithInteritanceWorks() {
        var rootConfig = registry.getConfiguration(ConfigurationHavingPolymorphicConfigWithNonTrivialHierarchyConfiguration.KEY);

        var polyConfig = (MiddlePolymorphicConfiguration) rootConfig.polymorphicConfiguration();

        assertThat(polyConfig.intVal().value(), is(42));

        var polyView = (MiddlePolymorphicView) polyConfig.value();
        assertThat(polyView.intVal(), is(42));
    }

    @Test
    void changingPolymorphicConfigWithInteritanceWorks() {
        var rootConfig = registry.getConfiguration(ConfigurationHavingPolymorphicConfigWithNonTrivialHierarchyConfiguration.KEY);

        var polyConfig = (MiddlePolymorphicConfiguration) rootConfig.polymorphicConfiguration();

        CompletableFuture<Void> changeFuture = polyConfig.change(change -> {
            var polyChange = (MiddlePolymorphicChange) change;
            polyChange.changeIntVal(100);
        });
        assertThat(changeFuture, willCompleteSuccessfully());

        assertThat(polyConfig.intVal().value(), is(100));
        assertThat(((MiddlePolymorphicView) polyConfig.value()).intVal(), is(100));
    }

    /**
     * Polymorphic configuration that has concrete polymorphic instance extending this configuration indirectly, via another class.
     */
    @PolymorphicConfig
    public static class BasePolymorphicWithNonTrivialHierarchyConfigurationSchema {
        /** Polymorphic type id field. */
        @PolymorphicId(hasDefault = true)
        public String typeId = "leaf-under-middle";

        /** Long value. */
        @Value(hasDefault = true)
        public long longVal = 0;
    }

    /**
     * Non-annotated intermediary that extends base polymorphic configuration and is extended by a concrete polymorphic configuration.
     */
    @PolymorphicConfigIntermediary
    public static class MiddlePolymorphicConfigurationSchema extends BasePolymorphicWithNonTrivialHierarchyConfigurationSchema {
        /** Integer value. */
        @Value(hasDefault = true)
        public int intVal = 42;
    }

    /**
     * Polymorphic instance extending the base polymorphic config indirectly, via an intermediary class.
     */
    @PolymorphicConfigInstance("leaf-under-middle")
    public static class LeafUnderMiddlePolymorphicInstanceConfigurationSchema extends MiddlePolymorphicConfigurationSchema {
    }

    /**
     * Root configuration having {@link BasePolymorphicWithNonTrivialHierarchyConfigurationSchema}.
     */
    @ConfigurationRoot(rootName = "non-trivial-hierarchy")
    public static class ConfigurationHavingPolymorphicConfigWithNonTrivialHierarchyConfigurationSchema {
        @ConfigValue
        public BasePolymorphicWithNonTrivialHierarchyConfigurationSchema polymorphicConfiguration;
    }
}
