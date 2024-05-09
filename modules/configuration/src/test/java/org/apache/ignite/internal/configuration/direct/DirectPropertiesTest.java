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

package org.apache.ignite.internal.configuration.direct;

import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.ConfigurationListenOnlyException;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.InternalId;
import org.apache.ignite.configuration.annotation.Name;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for "direct" configuration properties.
 */
public class DirectPropertiesTest {
    /**
     * Direct root configuration schema.
     */
    @ConfigurationRoot(rootName = "root")
    public static class DirectConfigurationSchema {
        @Name("childTestName")
        @ConfigValue
        public DirectNestedConfigurationSchema child;

        @NamedConfigValue
        public DirectNestedConfigurationSchema children;

        @Value(hasDefault = true)
        public String directStr = "foo";
    }

    /**
     * Direct nested configuration schema.
     */
    @Config
    public static class DirectNestedConfigurationSchema {
        @InjectedName
        public String name;

        @InternalId
        public UUID id;

        @Value(hasDefault = true)
        public String str = "bar";

        @NamedConfigValue
        public DirectNested2ConfigurationSchema children2;
    }

    /**
     * Direct nested 2 configuration schema.
     */
    @Config
    public static class DirectNested2ConfigurationSchema {
        @InternalId
        public UUID id;

        @Value(hasDefault = true)
        public String str = "bar";
    }

    private ConfigurationRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new ConfigurationRegistry(
                List.of(DirectConfiguration.KEY),
                new TestConfigurationStorage(LOCAL),
                new ConfigurationTreeGenerator(DirectConfiguration.KEY),
                new TestConfigurationValidator()
        );

        assertThat(registry.startAsync(), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() {
        assertThat(registry.stopAsync(), willCompleteSuccessfully());
    }

    /**
     * Tests that configuration values and nested configurations work correctly.
     */
    @Test
    public void testDirectProperties() {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        // Check that directProxy method returns the same object when called on the direct proxy.
        DirectConfiguration directProxy = cfg.directProxy();
        assertSame(directProxy, directProxy.directProxy());

        // Check all possible orderings.
        assertThat(cfg.directProxy().value().directStr(), is("foo"));
        assertThat(cfg.directProxy().directStr().value(), is("foo"));
        assertThat(cfg.directStr().directProxy().value(), is("foo"));

        // Check access to more deep sub-configurations.
        assertThat(cfg.directProxy().value().child().str(), is("bar"));
        assertThat(cfg.directProxy().child().value().str(), is("bar"));
        assertThat(cfg.directProxy().child().str().value(), is("bar"));

        assertThat(cfg.child().directProxy().value().str(), is("bar"));
        assertThat(cfg.child().directProxy().str().value(), is("bar"));

        assertThat(cfg.child().str().directProxy().value(), is("bar"));

        // Check that id is null from all possible paths.
        assertThat(cfg.directProxy().value().child().id(), is(nullValue()));
        assertThat(cfg.directProxy().child().value().id(), is(nullValue()));
        assertThat(cfg.directProxy().child().id().value(), is(nullValue()));

        assertThat(cfg.child().directProxy().value().id(), is(nullValue()));
        assertThat(cfg.child().directProxy().id().value(), is(nullValue()));

        assertThat(cfg.child().id().directProxy().value(), is(nullValue()));
    }

    /**
     * Same as {@link #testDirectProperties} but checks Named List properties.
     */
    @Test
    public void testNamedListDirectProperties() throws Exception {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        cfg.children()
                .change(change -> change.create("foo", value -> {}))
                .get(1, TimeUnit.SECONDS);

        UUID fooId = cfg.children().get("foo").id().value();

        assertThat(fooId, is(notNullValue()));

        // Check all possible ways to access "str" of element named "foo".
        assertThat(cfg.directProxy().value().children().get("foo").str(), is("bar"));
        assertThat(cfg.directProxy().children().value().get("foo").str(), is("bar"));
        assertThat(cfg.directProxy().children().get("foo").value().str(), is("bar"));
        assertThat(cfg.directProxy().children().get("foo").str().value(), is("bar"));

        assertThat(cfg.children().directProxy().value().get("foo").str(), is("bar"));
        assertThat(cfg.children().directProxy().get("foo").value().str(), is("bar"));
        assertThat(cfg.children().directProxy().get("foo").str().value(), is("bar"));

        assertThat(cfg.children().get("foo").directProxy().value().str(), is("bar"));
        assertThat(cfg.children().get("foo").directProxy().str().value(), is("bar"));

        assertThat(cfg.children().get("foo").str().directProxy().value(), is("bar"));

        // Check all possible ways to access "str" of element with given internal id.
        assertThat(cfg.directProxy().value().children().get(fooId).str(), is("bar"));
        assertThat(cfg.directProxy().children().value().get(fooId).str(), is("bar"));
        assertThat(cfg.directProxy().children().get(fooId).value().str(), is("bar"));
        assertThat(cfg.directProxy().children().get(fooId).str().value(), is("bar"));

        assertThat(cfg.children().directProxy().value().get(fooId).str(), is("bar"));
        assertThat(cfg.children().directProxy().get(fooId).value().str(), is("bar"));
        assertThat(cfg.children().directProxy().get(fooId).str().value(), is("bar"));

        assertThat(cfg.children().get(fooId).directProxy().value().str(), is("bar"));
        assertThat(cfg.children().get(fooId).directProxy().str().value(), is("bar"));

        assertThat(cfg.children().get(fooId).str().directProxy().value(), is("bar"));
    }

    /**
     * Checks simple scenarios of getting internalId of named list element.
     */
    @Test
    public void testNamedListDirectInternalId() throws Exception {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        cfg.children()
                .change(change -> change.create("foo", value -> {}))
                .get(1, TimeUnit.SECONDS);

        UUID fooId = cfg.children().get("foo").id().value();

        assertThat(fooId, is(notNullValue()));

        // Check all possible ways to access "str" of element named "foo".
        assertThat(cfg.directProxy().value().children().get("foo").id(), is(equalTo(fooId)));
        assertThat(cfg.directProxy().children().value().get("foo").id(), is(equalTo(fooId)));
        assertThat(cfg.directProxy().children().get("foo").value().id(), is(equalTo(fooId)));
        assertThat(cfg.directProxy().children().get("foo").id().value(), is(equalTo(fooId)));

        assertThat(cfg.children().directProxy().value().get("foo").id(), is(equalTo(fooId)));
        assertThat(cfg.children().directProxy().get("foo").value().id(), is(equalTo(fooId)));
        assertThat(cfg.children().directProxy().get("foo").id().value(), is(equalTo(fooId)));

        assertThat(cfg.children().get("foo").directProxy().value().id(), is(equalTo(fooId)));
        assertThat(cfg.children().get("foo").directProxy().id().value(), is(equalTo(fooId)));

        assertThat(cfg.children().get("foo").id().directProxy().value(), is(equalTo(fooId)));

        // Check all possible ways to access "id" of element with given internal id.
        assertThat(cfg.directProxy().value().children().get(fooId).id(), is(equalTo(fooId)));
        assertThat(cfg.directProxy().children().value().get(fooId).id(), is(equalTo(fooId)));
        assertThat(cfg.directProxy().children().get(fooId).value().id(), is(equalTo(fooId)));
        assertThat(cfg.directProxy().children().get(fooId).id().value(), is(equalTo(fooId)));

        assertThat(cfg.children().directProxy().value().get(fooId).id(), is(equalTo(fooId)));
        assertThat(cfg.children().directProxy().get(fooId).value().id(), is(equalTo(fooId)));
        assertThat(cfg.children().directProxy().get(fooId).id().value(), is(equalTo(fooId)));

        assertThat(cfg.children().get(fooId).directProxy().value().id(), is(equalTo(fooId)));
        assertThat(cfg.children().get(fooId).directProxy().id().value(), is(equalTo(fooId)));

        assertThat(cfg.children().get(fooId).id().directProxy().value(), is(equalTo(fooId)));
    }

    /**
     * Checks simple scenarios of getting internalIds of named list.
     */
    @Test
    public void testNamedListDirectInternalIds() throws Exception {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        cfg.children()
                .change(change -> change.create("foo", value -> {
                }))
                .get(1, TimeUnit.SECONDS);

        UUID fooId = cfg.children().get("foo").id().value();

        assertThat(fooId, is(notNullValue()));

        // Check all possible ways to access internal ids.
        assertThat(cfg.directProxy().children().internalIds(), is(equalTo(List.of(fooId))));
        assertThat(cfg.children().directProxy().internalIds(), is(equalTo(List.of(fooId))));
    }

    /**
     * Same as {@link #testDirectProperties} but checks Named List properties.
     */
    @Test
    public void testNamedListDirectNestedProperties() throws Exception {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        cfg.children()
                .change(list -> list.create("foo", e -> e.changeChildren2(list2 -> list2.create("boo", e2 -> {}))))
                .get(1, TimeUnit.SECONDS);

        UUID fooId = cfg.children().get("foo").id().value();
        UUID booId = cfg.children().get("foo").children2().get("boo").id().value();

        assertThat(booId, is(notNullValue()));

        // Check all possible ways to access "str", just to be sure. Some of these checks are clearly excessive, but they look organic.
        // Using names in both lists.
        assertThat(cfg.directProxy().value().children().get("foo").children2().get("boo").str(), is("bar"));
        assertThat(cfg.directProxy().children().value().get("foo").children2().get("boo").str(), is("bar"));
        assertThat(cfg.directProxy().children().get("foo").value().children2().get("boo").str(), is("bar"));
        assertThat(cfg.directProxy().children().get("foo").children2().value().get("boo").str(), is("bar"));
        assertThat(cfg.directProxy().children().get("foo").children2().get("boo").value().str(), is("bar"));
        assertThat(cfg.directProxy().children().get("foo").children2().get("boo").str().value(), is("bar"));

        assertThat(cfg.children().directProxy().value().get("foo").children2().get("boo").str(), is("bar"));
        assertThat(cfg.children().directProxy().get("foo").value().children2().get("boo").str(), is("bar"));
        assertThat(cfg.children().directProxy().get("foo").children2().value().get("boo").str(), is("bar"));
        assertThat(cfg.children().directProxy().get("foo").children2().get("boo").value().str(), is("bar"));
        assertThat(cfg.children().directProxy().get("foo").children2().get("boo").str().value(), is("bar"));

        assertThat(cfg.children().get("foo").directProxy().value().children2().get("boo").str(), is("bar"));
        assertThat(cfg.children().get("foo").directProxy().children2().value().get("boo").str(), is("bar"));
        assertThat(cfg.children().get("foo").directProxy().children2().get("boo").value().str(), is("bar"));
        assertThat(cfg.children().get("foo").directProxy().children2().get("boo").str().value(), is("bar"));

        assertThat(cfg.children().get("foo").children2().directProxy().value().get("boo").str(), is("bar"));
        assertThat(cfg.children().get("foo").children2().directProxy().get("boo").value().str(), is("bar"));
        assertThat(cfg.children().get("foo").children2().directProxy().get("boo").str().value(), is("bar"));

        assertThat(cfg.children().get("foo").children2().get("boo").directProxy().value().str(), is("bar"));
        assertThat(cfg.children().get("foo").children2().get("boo").directProxy().str().value(), is("bar"));

        assertThat(cfg.children().get("foo").children2().get("boo").str().directProxy().value(), is("bar"));

        // Using internalId and name.
        assertThat(cfg.directProxy().value().children().get(fooId).children2().get("boo").str(), is("bar"));
        assertThat(cfg.directProxy().children().value().get(fooId).children2().get("boo").str(), is("bar"));
        assertThat(cfg.directProxy().children().get(fooId).value().children2().get("boo").str(), is("bar"));
        assertThat(cfg.directProxy().children().get(fooId).children2().value().get("boo").str(), is("bar"));
        assertThat(cfg.directProxy().children().get(fooId).children2().get("boo").value().str(), is("bar"));
        assertThat(cfg.directProxy().children().get(fooId).children2().get("boo").str().value(), is("bar"));

        assertThat(cfg.children().directProxy().value().get(fooId).children2().get("boo").str(), is("bar"));
        assertThat(cfg.children().directProxy().get(fooId).value().children2().get("boo").str(), is("bar"));
        assertThat(cfg.children().directProxy().get(fooId).children2().value().get("boo").str(), is("bar"));
        assertThat(cfg.children().directProxy().get(fooId).children2().get("boo").value().str(), is("bar"));
        assertThat(cfg.children().directProxy().get(fooId).children2().get("boo").str().value(), is("bar"));

        assertThat(cfg.children().get(fooId).directProxy().value().children2().get("boo").str(), is("bar"));
        assertThat(cfg.children().get(fooId).directProxy().children2().value().get("boo").str(), is("bar"));
        assertThat(cfg.children().get(fooId).directProxy().children2().get("boo").value().str(), is("bar"));
        assertThat(cfg.children().get(fooId).directProxy().children2().get("boo").str().value(), is("bar"));

        assertThat(cfg.children().get(fooId).children2().directProxy().value().get("boo").str(), is("bar"));
        assertThat(cfg.children().get(fooId).children2().directProxy().get("boo").value().str(), is("bar"));
        assertThat(cfg.children().get(fooId).children2().directProxy().get("boo").str().value(), is("bar"));

        assertThat(cfg.children().get(fooId).children2().get("boo").directProxy().value().str(), is("bar"));
        assertThat(cfg.children().get(fooId).children2().get("boo").directProxy().str().value(), is("bar"));

        assertThat(cfg.children().get(fooId).children2().get("boo").str().directProxy().value(), is("bar"));

        // Using name and internalId.
        assertThat(cfg.directProxy().value().children().get("foo").children2().get(booId).str(), is("bar"));
        assertThat(cfg.directProxy().children().value().get("foo").children2().get(booId).str(), is("bar"));
        assertThat(cfg.directProxy().children().get("foo").value().children2().get(booId).str(), is("bar"));
        assertThat(cfg.directProxy().children().get("foo").children2().value().get(booId).str(), is("bar"));
        assertThat(cfg.directProxy().children().get("foo").children2().get(booId).value().str(), is("bar"));
        assertThat(cfg.directProxy().children().get("foo").children2().get(booId).str().value(), is("bar"));

        assertThat(cfg.children().directProxy().value().get("foo").children2().get(booId).str(), is("bar"));
        assertThat(cfg.children().directProxy().get("foo").value().children2().get(booId).str(), is("bar"));
        assertThat(cfg.children().directProxy().get("foo").children2().value().get(booId).str(), is("bar"));
        assertThat(cfg.children().directProxy().get("foo").children2().get(booId).value().str(), is("bar"));
        assertThat(cfg.children().directProxy().get("foo").children2().get(booId).str().value(), is("bar"));

        assertThat(cfg.children().get("foo").directProxy().value().children2().get(booId).str(), is("bar"));
        assertThat(cfg.children().get("foo").directProxy().children2().value().get(booId).str(), is("bar"));
        assertThat(cfg.children().get("foo").directProxy().children2().get(booId).value().str(), is("bar"));
        assertThat(cfg.children().get("foo").directProxy().children2().get(booId).str().value(), is("bar"));

        assertThat(cfg.children().get("foo").children2().directProxy().value().get(booId).str(), is("bar"));
        assertThat(cfg.children().get("foo").children2().directProxy().get(booId).value().str(), is("bar"));
        assertThat(cfg.children().get("foo").children2().directProxy().get(booId).str().value(), is("bar"));

        assertThat(cfg.children().get("foo").children2().get(booId).directProxy().value().str(), is("bar"));
        assertThat(cfg.children().get("foo").children2().get(booId).directProxy().str().value(), is("bar"));

        assertThat(cfg.children().get("foo").children2().get(booId).str().directProxy().value(), is("bar"));

        // Using internalId and internalId.
        assertThat(cfg.directProxy().value().children().get(fooId).children2().get(booId).str(), is("bar"));
        assertThat(cfg.directProxy().children().value().get(fooId).children2().get(booId).str(), is("bar"));
        assertThat(cfg.directProxy().children().get(fooId).value().children2().get(booId).str(), is("bar"));
        assertThat(cfg.directProxy().children().get(fooId).children2().value().get(booId).str(), is("bar"));
        assertThat(cfg.directProxy().children().get(fooId).children2().get(booId).value().str(), is("bar"));
        assertThat(cfg.directProxy().children().get(fooId).children2().get(booId).str().value(), is("bar"));

        assertThat(cfg.children().directProxy().value().get(fooId).children2().get(booId).str(), is("bar"));
        assertThat(cfg.children().directProxy().get(fooId).value().children2().get(booId).str(), is("bar"));
        assertThat(cfg.children().directProxy().get(fooId).children2().value().get(booId).str(), is("bar"));
        assertThat(cfg.children().directProxy().get(fooId).children2().get(booId).value().str(), is("bar"));
        assertThat(cfg.children().directProxy().get(fooId).children2().get(booId).str().value(), is("bar"));

        assertThat(cfg.children().get(fooId).directProxy().value().children2().get(booId).str(), is("bar"));
        assertThat(cfg.children().get(fooId).directProxy().children2().value().get(booId).str(), is("bar"));
        assertThat(cfg.children().get(fooId).directProxy().children2().get(booId).value().str(), is("bar"));
        assertThat(cfg.children().get(fooId).directProxy().children2().get(booId).str().value(), is("bar"));

        assertThat(cfg.children().get(fooId).children2().directProxy().value().get(booId).str(), is("bar"));
        assertThat(cfg.children().get(fooId).children2().directProxy().get(booId).value().str(), is("bar"));
        assertThat(cfg.children().get(fooId).children2().directProxy().get(booId).str().value(), is("bar"));

        assertThat(cfg.children().get(fooId).children2().get(booId).directProxy().value().str(), is("bar"));
        assertThat(cfg.children().get(fooId).children2().get(booId).directProxy().str().value(), is("bar"));

        assertThat(cfg.children().get(fooId).children2().get(booId).str().directProxy().value(), is("bar"));
    }

    /**
     * Same as {@link #testNamedListDirectInternalId()} but checks Named List properties.
     */
    @Test
    public void testNamedListDirectNestedInternalId() throws Exception {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        cfg.children()
                .change(list -> list.create("foo", e -> e.changeChildren2(list2 -> list2.create("boo", e2 -> {}))))
                .get(1, TimeUnit.SECONDS);

        UUID fooId = cfg.children().get("foo").id().value();
        UUID booId = cfg.children().get("foo").children2().get("boo").id().value();

        assertThat(booId, is(notNullValue()));

        // Check all possible ways to access "id", just to be sure. Some of these checks are clearly excessive, but they look organic.
        // Using names in both lists.
        assertThat(cfg.directProxy().value().children().get("foo").children2().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.directProxy().children().value().get("foo").children2().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.directProxy().children().get("foo").value().children2().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.directProxy().children().get("foo").children2().value().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.directProxy().children().get("foo").children2().get("boo").value().id(), is(equalTo(booId)));
        assertThat(cfg.directProxy().children().get("foo").children2().get("boo").id().value(), is(equalTo(booId)));

        assertThat(cfg.children().directProxy().value().get("foo").children2().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.children().directProxy().get("foo").value().children2().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.children().directProxy().get("foo").children2().value().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.children().directProxy().get("foo").children2().get("boo").value().id(), is(equalTo(booId)));
        assertThat(cfg.children().directProxy().get("foo").children2().get("boo").id().value(), is(equalTo(booId)));

        assertThat(cfg.children().get("foo").directProxy().value().children2().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.children().get("foo").directProxy().children2().value().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.children().get("foo").directProxy().children2().get("boo").value().id(), is(equalTo(booId)));
        assertThat(cfg.children().get("foo").directProxy().children2().get("boo").id().value(), is(equalTo(booId)));

        assertThat(cfg.children().get("foo").children2().directProxy().value().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.children().get("foo").children2().directProxy().get("boo").value().id(), is(equalTo(booId)));
        assertThat(cfg.children().get("foo").children2().directProxy().get("boo").id().value(), is(equalTo(booId)));

        assertThat(cfg.children().get("foo").children2().get("boo").directProxy().value().id(), is(equalTo(booId)));
        assertThat(cfg.children().get("foo").children2().get("boo").directProxy().id().value(), is(equalTo(booId)));

        assertThat(cfg.children().get("foo").children2().get("boo").id().directProxy().value(), is(equalTo(booId)));

        // Using internalId and name.
        assertThat(cfg.directProxy().value().children().get(fooId).children2().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.directProxy().children().value().get(fooId).children2().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.directProxy().children().get(fooId).value().children2().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.directProxy().children().get(fooId).children2().value().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.directProxy().children().get(fooId).children2().get("boo").value().id(), is(equalTo(booId)));
        assertThat(cfg.directProxy().children().get(fooId).children2().get("boo").id().value(), is(equalTo(booId)));

        assertThat(cfg.children().directProxy().value().get(fooId).children2().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.children().directProxy().get(fooId).value().children2().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.children().directProxy().get(fooId).children2().value().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.children().directProxy().get(fooId).children2().get("boo").value().id(), is(equalTo(booId)));
        assertThat(cfg.children().directProxy().get(fooId).children2().get("boo").id().value(), is(equalTo(booId)));

        assertThat(cfg.children().get(fooId).directProxy().value().children2().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.children().get(fooId).directProxy().children2().value().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.children().get(fooId).directProxy().children2().get("boo").value().id(), is(equalTo(booId)));
        assertThat(cfg.children().get(fooId).directProxy().children2().get("boo").id().value(), is(equalTo(booId)));

        assertThat(cfg.children().get(fooId).children2().directProxy().value().get("boo").id(), is(equalTo(booId)));
        assertThat(cfg.children().get(fooId).children2().directProxy().get("boo").value().id(), is(equalTo(booId)));
        assertThat(cfg.children().get(fooId).children2().directProxy().get("boo").id().value(), is(equalTo(booId)));

        assertThat(cfg.children().get(fooId).children2().get("boo").directProxy().value().id(), is(equalTo(booId)));
        assertThat(cfg.children().get(fooId).children2().get("boo").directProxy().id().value(), is(equalTo(booId)));

        assertThat(cfg.children().get(fooId).children2().get("boo").id().directProxy().value(), is(equalTo(booId)));

        // Using name and internalId.
        assertThat(cfg.directProxy().value().children().get("foo").children2().get(booId).id(), is(equalTo(booId)));
        assertThat(cfg.directProxy().children().value().get("foo").children2().get(booId).id(), is(equalTo(booId)));
        assertThat(cfg.directProxy().children().get("foo").value().children2().get(booId).id(), is(equalTo(booId)));
        assertThat(cfg.directProxy().children().get("foo").children2().value().get(booId).id(), is(equalTo(booId)));
        assertThat(cfg.directProxy().children().get("foo").children2().get(booId).value().id(), is(equalTo(booId)));
        assertThat(cfg.directProxy().children().get("foo").children2().get(booId).id().value(), is(equalTo(booId)));

        assertThat(cfg.children().directProxy().value().get("foo").children2().get(booId).id(), is(equalTo(booId)));
        assertThat(cfg.children().directProxy().get("foo").value().children2().get(booId).id(), is(equalTo(booId)));
        assertThat(cfg.children().directProxy().get("foo").children2().value().get(booId).id(), is(equalTo(booId)));
        assertThat(cfg.children().directProxy().get("foo").children2().get(booId).value().id(), is(equalTo(booId)));
        assertThat(cfg.children().directProxy().get("foo").children2().get(booId).id().value(), is(equalTo(booId)));

        assertThat(cfg.children().get("foo").directProxy().value().children2().get(booId).id(), is(equalTo(booId)));
        assertThat(cfg.children().get("foo").directProxy().children2().value().get(booId).id(), is(equalTo(booId)));
        assertThat(cfg.children().get("foo").directProxy().children2().get(booId).value().id(), is(equalTo(booId)));
        assertThat(cfg.children().get("foo").directProxy().children2().get(booId).id().value(), is(equalTo(booId)));

        assertThat(cfg.children().get("foo").children2().directProxy().value().get(booId).id(), is(equalTo(booId)));
        assertThat(cfg.children().get("foo").children2().directProxy().get(booId).value().id(), is(equalTo(booId)));
        assertThat(cfg.children().get("foo").children2().directProxy().get(booId).id().value(), is(equalTo(booId)));

        assertThat(cfg.children().get("foo").children2().get(booId).directProxy().value().id(), is(equalTo(booId)));
        assertThat(cfg.children().get("foo").children2().get(booId).directProxy().id().value(), is(equalTo(booId)));

        assertThat(cfg.children().get("foo").children2().get(booId).id().directProxy().value(), is(equalTo(booId)));

        // Using internalId and internalId.
        assertThat(cfg.directProxy().value().children().get(fooId).children2().get(booId).id(), is(booId));
        assertThat(cfg.directProxy().children().value().get(fooId).children2().get(booId).id(), is(booId));
        assertThat(cfg.directProxy().children().get(fooId).value().children2().get(booId).id(), is(booId));
        assertThat(cfg.directProxy().children().get(fooId).children2().value().get(booId).id(), is(booId));
        assertThat(cfg.directProxy().children().get(fooId).children2().get(booId).value().id(), is(booId));
        assertThat(cfg.directProxy().children().get(fooId).children2().get(booId).id().value(), is(booId));

        assertThat(cfg.children().directProxy().value().get(fooId).children2().get(booId).id(), is(booId));
        assertThat(cfg.children().directProxy().get(fooId).value().children2().get(booId).id(), is(booId));
        assertThat(cfg.children().directProxy().get(fooId).children2().value().get(booId).id(), is(booId));
        assertThat(cfg.children().directProxy().get(fooId).children2().get(booId).value().id(), is(booId));
        assertThat(cfg.children().directProxy().get(fooId).children2().get(booId).id().value(), is(booId));

        assertThat(cfg.children().get(fooId).directProxy().value().children2().get(booId).id(), is(booId));
        assertThat(cfg.children().get(fooId).directProxy().children2().value().get(booId).id(), is(booId));
        assertThat(cfg.children().get(fooId).directProxy().children2().get(booId).value().id(), is(booId));
        assertThat(cfg.children().get(fooId).directProxy().children2().get(booId).id().value(), is(booId));

        assertThat(cfg.children().get(fooId).children2().directProxy().value().get(booId).id(), is(booId));
        assertThat(cfg.children().get(fooId).children2().directProxy().get(booId).value().id(), is(booId));
        assertThat(cfg.children().get(fooId).children2().directProxy().get(booId).id().value(), is(booId));

        assertThat(cfg.children().get(fooId).children2().get(booId).directProxy().value().id(), is(booId));
        assertThat(cfg.children().get(fooId).children2().get(booId).directProxy().id().value(), is(booId));

        assertThat(cfg.children().get(fooId).children2().get(booId).id().directProxy().value(), is(booId));
    }

    /**
     * Same as {@link #testNamedListDirectInternalIds()} but checks Named List properties.
     */
    @Test
    public void testNamedListDirectNestedInternalIds() throws Exception {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        cfg.children()
                .change(list -> list.create("foo", e -> e.changeChildren2(list2 -> list2.create("boo", e2 -> {}))))
                .get(1, TimeUnit.SECONDS);

        UUID fooId = cfg.children().get("foo").id().value();
        UUID booId = cfg.children().get("foo").children2().get("boo").id().value();

        assertThat(booId, is(notNullValue()));

        List<UUID> ids = List.of(booId);

        // Check all possible ways to access internal ids, just to be sure.
        // Using name.
        assertThat(cfg.directProxy().children().get("foo").children2().internalIds(), is(equalTo(ids)));
        assertThat(cfg.children().directProxy().get("foo").children2().internalIds(), is(equalTo(ids)));
        assertThat(cfg.children().get("foo").directProxy().children2().internalIds(), is(equalTo(ids)));
        assertThat(cfg.children().get("foo").children2().directProxy().internalIds(), is(equalTo(ids)));

        // Using internalId.
        assertThat(cfg.directProxy().children().get(fooId).children2().internalIds(), is(equalTo(ids)));
        assertThat(cfg.children().directProxy().get(fooId).children2().internalIds(), is(equalTo(ids)));
        assertThat(cfg.children().get(fooId).children2().directProxy().internalIds(), is(equalTo(ids)));
    }

    @Test
    public void testNamedListNoSuchElement() throws Exception {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        cfg.children()
                .change(list -> list.create("foo", e -> e.changeChildren2(list2 -> list2.create("boo", e2 -> {}))))
                .get(1, TimeUnit.SECONDS);

        UUID fakeId = UUID.randomUUID();

        assertThrows(NoSuchElementException.class, () -> cfg.directProxy().children().get("a").value());
        assertThrows(NoSuchElementException.class, () -> cfg.directProxy().children().get(fakeId).value());

        DirectNestedConfiguration foo = cfg.children().get("foo");

        assertThrows(NoSuchElementException.class, () -> foo.directProxy().children2().get("b").value());
        assertThrows(NoSuchElementException.class, () -> foo.directProxy().children2().get(fakeId).value());
    }

    /**
     * Test for named list configuration.
     *
     * <p>Tests the following scenario:
     * <ol>
     *     <li>A Named List element is created. Both "direct" and regular properties must be the same.</li>
     *     <li>The element is removed. Both "direct" and regular properties must not return any values
     *     (i.e. throw an exception).</li>
     *     <li>A new Named List element is created under the same name. The regular property still must not return
     *     anything: the new element is represented by a different configuration node even if it has the same name.
     *     However, the "direct" property must return the new value, which is the intended behaviour.</li>
     * </ol>
     */
    @Test
    public void testNamedListDeleteCreate() {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        CompletableFuture<Void> changeFuture = cfg.children().change(change -> change.create("x", value -> {}));

        assertThat(changeFuture, willBe(nullValue(Void.class)));

        DirectNestedConfiguration childCfg = cfg.children().get("x");

        assertThat(childCfg.str().value(), is("bar"));
        assertThat(childCfg.str().directProxy().value(), is("bar"));

        changeFuture = cfg.children().change(change -> change.delete("x"));

        assertThat(changeFuture, willBe(nullValue(Void.class)));

        assertThrows(NoSuchElementException.class, () -> childCfg.str().value());
        assertThrows(NoSuchElementException.class, () -> childCfg.str().directProxy().value());

        changeFuture = cfg.children().change(change -> change.create("x", value -> {
        }));

        assertThat(changeFuture, willBe(nullValue(Void.class)));

        assertThrows(NoSuchElementException.class, () -> childCfg.str().value());
        assertThat(childCfg.str().directProxy().value(), is("bar"));
    }

    @Test
    void testDirectAccessForAny() {
        NamedConfigurationTree<DirectNestedConfiguration, DirectNestedView, DirectNestedChange> children =
                registry.getConfiguration(DirectConfiguration.KEY).children();

        assertThrows(ConfigurationListenOnlyException.class, () -> children.any().directProxy());
        assertThrows(ConfigurationListenOnlyException.class, () -> children.any().str().directProxy());
        assertThrows(ConfigurationListenOnlyException.class, () -> children.any().children2().directProxy());
    }

    @Test
    void testInjectedNameNestedConfig() {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        // From root config.
        assertThat(cfg.directProxy().child().name().value(), is("childTestName"));
        assertThat(cfg.directProxy().value().child().name(), is("childTestName"));

        // From nested (child) config.
        assertThat(cfg.child().directProxy().name().value(), is("childTestName"));
        assertThat(cfg.child().directProxy().value().name(), is("childTestName"));

        // From config property.
        assertThat(cfg.child().name().directProxy().value(), is("childTestName"));
    }

    @Test
    void testInjectedNameNamedConfig() throws Exception {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        cfg.change(c0 -> c0.changeChildren(c1 -> c1.create("0", c2 -> {}))).get(1, TimeUnit.SECONDS);

        // From root config.
        assertThat(cfg.directProxy().children().get("0").name().value(), is("0"));
        assertThat(cfg.directProxy().value().children().get("0").name(), is("0"));
        assertThat(cfg.directProxy().children().value().get("0").name(), is("0"));
        assertThat(cfg.directProxy().children().get("0").value().name(), is("0"));

        // From nested (children) config.
        assertThat(cfg.children().directProxy().get("0").name().value(), is("0"));
        assertThat(cfg.children().directProxy().value().get("0").name(), is("0"));
        assertThat(cfg.children().directProxy().get("0").value().name(), is("0"));

        // From named element.
        assertThat(cfg.children().get("0").directProxy().name().value(), is("0"));
        assertThat(cfg.children().get("0").directProxy().value().name(), is("0"));

        // From named element config property.
        assertThat(cfg.children().get("0").name().directProxy().value(), is("0"));
    }
}
