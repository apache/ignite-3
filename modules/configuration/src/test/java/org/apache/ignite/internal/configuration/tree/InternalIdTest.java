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

package org.apache.ignite.internal.configuration.tree;


import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InternalId;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.direct.DirectPropertiesTest;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link InternalId}. Doesn't check everything, fair bit of its functionality is covered in {@link DirectPropertiesTest}.
 */
public class InternalIdTest {
    /** Parent configuration for the test. has a single extension and list of polymorphic configurations. */
    @ConfigurationRoot(rootName = "root", type = LOCAL)
    public static class InternalIdParentConfigurationSchema {
        @NamedConfigValue
        public InternalIdPolymorphicConfigurationSchema polymorphic;
    }

    /** Internal extension for the parent configuration. */
    @ConfigurationExtension(internal = true)
    public static class InternalIdInternalConfigurationSchema extends InternalIdParentConfigurationSchema {
        @InternalId
        public UUID id;
    }

    /** Schema for the polymorphic configuration. */
    @PolymorphicConfig
    public static class InternalIdPolymorphicConfigurationSchema {
        @PolymorphicId
        public String type;

        @InternalId
        public UUID id;
    }

    /** Single polymorphic extension. */
    @PolymorphicConfigInstance("foo")
    public static class InternalIdFooConfigurationSchema extends InternalIdPolymorphicConfigurationSchema {
    }

    private ConfigurationRegistry registry;

    private ConfigurationTreeGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new ConfigurationTreeGenerator(
                List.of(InternalIdParentConfiguration.KEY),
                List.of(InternalIdInternalConfigurationSchema.class),
                List.of(InternalIdFooConfigurationSchema.class)
        );

        registry = new ConfigurationRegistry(
                List.of(InternalIdParentConfiguration.KEY),
                new TestConfigurationStorage(LOCAL),
                generator,
                new TestConfigurationValidator()
        );

        assertThat(registry.startAsync(ForkJoinPool.commonPool()), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() {
        assertThat(registry.stopAsync(ForkJoinPool.commonPool()), willCompleteSuccessfully());

        generator.close();
    }

    /**
     * Tests that internal id, declared in internal extension, works properly.
     */
    @Test
    public void testInternalExtension() {
        InternalIdParentConfiguration cfg = registry.getConfiguration(InternalIdParentConfiguration.KEY);

        UUID internalId = UUID.randomUUID();

        assertThat(cfg.change(change -> ((InnerNode) change).internalId(internalId)), willCompleteSuccessfully());

        // Put it there manually, this simplifies the test.
        IgniteTestUtils.setFieldValue(cfg.value(), InnerNode.class, "internalId", internalId);

        // Getting it from the explicit configuration cast should work.
        assertThat(((InternalIdInternalConfiguration) cfg).id().value(), is(equalTo(internalId)));

        // Getting it from the explicit configuration value cast should work as well.
        assertThat(((InternalIdInternalView) cfg.value()).id(), is(equalTo(internalId)));
    }

    /**
     * Tests that internal id, declared in polymorphic configuration, works properly.
     */
    @Test
    public void testPolymorphicExtension() throws Exception {
        InternalIdParentConfiguration cfg = registry.getConfiguration(InternalIdParentConfiguration.KEY);

        // Create polymorphic instance.
        cfg.polymorphic().change(list -> list.create("a", element -> {
            // Check that id is accessible via "raw" instance.
            UUID internalId = element.id();

            assertThat(internalId, is(notNullValue()));

            // Check that id is accessible via "specific" instance.
            InternalIdFooChange foo = element.convert(InternalIdFooChange.class);

            assertThat(foo.id(), is(equalTo(internalId)));
        })).get(1, TimeUnit.SECONDS);

        // Read internal id from the named list directly.
        var list = (NamedListNode<InternalIdPolymorphicView>) cfg.polymorphic().value();

        UUID internalId = list.internalId("a");

        // Check that this internal id matches the one from raw InnerNode list element.
        assertThat(list.getInnerNode("a").internalId(), is(equalTo(internalId)));

        // Check that internal id mathes the one from "specific" configuration view instance.
        assertThat(list.get("a").id(), is(equalTo(internalId)));

        // Check that intetnal id is accessible from the polymorphic Configuration instance.
        assertThat(cfg.polymorphic().get("a").id().value(), is(equalTo(internalId)));
        assertThat(cfg.polymorphic().get("a").value().id(), is(equalTo(internalId)));
    }
}
