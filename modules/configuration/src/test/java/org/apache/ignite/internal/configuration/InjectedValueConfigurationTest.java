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

package org.apache.ignite.internal.configuration;

import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.hocon.HoconConverter.hoconSource;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.InjectedValue;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.manager.ComponentContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for schemas with {@link InjectedValue}s.
 */
public class InjectedValueConfigurationTest {
    /** Root schema. */
    @ConfigurationRoot(rootName = "rootInjectedValue", type = LOCAL)
    public static class HoconInjectedValueRootConfigurationSchema {
        @NamedConfigValue
        public HoconInjectedValueConfigurationSchema nestedNamed;
    }

    /** Named list element schema. */
    @Config
    public static class HoconInjectedValueConfigurationSchema {
        @InjectedName
        public String someName;

        @InjectedValue(hasDefault = true)
        public String someValue = "default";
    }

    private ConfigurationRegistry registry;

    @BeforeEach
    void setUp() {
        List<RootKey<?, ?, ?>> roots = List.of(
                HoconInjectedValueRootConfiguration.KEY
        );

        registry = new ConfigurationRegistry(
                roots,
                new TestConfigurationStorage(LOCAL),
                new ConfigurationTreeGenerator(roots, List.of(), List.of()),
                new TestConfigurationValidator()
        );

        assertThat(registry.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() {
        assertThat(registry.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @Nested
    class HoconConverterTest {
        @Test
        void testEmpty() {
            assertEquals("nestedNamed=[]", asHoconStr(List.of("rootInjectedValue")));
        }

        @Test
        void testAssignValuesUsingObjectNotation() {
            change("rootInjectedValue.nestedNamed = {foo: bar}");

            assertEquals("nestedNamed{foo=bar}", asHoconStr(List.of("rootInjectedValue")));

            change("rootInjectedValue.nestedNamed = {foo: bar, baz: quux}");

            assertEquals("nestedNamed{baz=quux,foo=bar}", asHoconStr(List.of("rootInjectedValue")));

            change("rootInjectedValue.nestedNamed = {baz: anotherQuux}");

            assertEquals("nestedNamed{baz=anotherQuux,foo=bar}", asHoconStr(List.of("rootInjectedValue")));

            change("rootInjectedValue.nestedNamed = {baz: null}");

            assertEquals("nestedNamed{foo=bar}", asHoconStr(List.of("rootInjectedValue")));
        }

        @Test
        void testAssignValuesUsingListNotation() {
            change("rootInjectedValue.nestedNamed = [{foo=bar}]");

            assertEquals("nestedNamed{foo=bar}", asHoconStr(List.of("rootInjectedValue")));

            change("rootInjectedValue.nestedNamed = [{foo=bar},{baz=quux}]");

            assertEquals("nestedNamed{baz=quux,foo=bar}", asHoconStr(List.of("rootInjectedValue")));

            change("rootInjectedValue.nestedNamed = [{baz=anotherQuux}]");

            assertEquals("nestedNamed{baz=anotherQuux,foo=bar}", asHoconStr(List.of("rootInjectedValue")));

            // Removing a value does not work in this notation.
        }
    }

    @Nested
    class JavaApiTest {
        @Test
        void testEmpty() {
            HoconInjectedValueRootConfiguration cfg = registry.getConfiguration(HoconInjectedValueRootConfiguration.KEY);

            assertThat(cfg.nestedNamed().value().size(), is(0));
        }

        @Test
        void testDefaults() {
            HoconInjectedValueRootConfiguration cfg = registry.getConfiguration(HoconInjectedValueRootConfiguration.KEY);

            CompletableFuture<Void> changeFuture = cfg.change(rootChange -> rootChange
                    .changeNestedNamed(nestedChange -> nestedChange
                            .create("foo", valueChange -> {})));

            assertThat(changeFuture, willCompleteSuccessfully());

            assertThat(cfg.value().nestedNamed().get("foo").someValue(), is("default"));
            assertThat(cfg.nestedNamed().value().get("foo").someValue(), is("default"));
            assertThat(cfg.nestedNamed().get("foo").value().someValue(), is("default"));
            assertThat(cfg.nestedNamed().get("foo").someValue().value(), is("default"));
        }

        @Test
        void testAssignValues() {
            HoconInjectedValueRootConfiguration cfg = registry.getConfiguration(HoconInjectedValueRootConfiguration.KEY);

            CompletableFuture<Void> changeFuture = cfg.change(rootChange -> rootChange
                    .changeNestedNamed(nestedChange -> nestedChange
                            .create("foo", valueChange -> valueChange.changeSomeValue("bar"))));

            assertThat(changeFuture, willCompleteSuccessfully());

            assertThat(cfg.value().nestedNamed().get("foo").someValue(), is("bar"));
            assertThat(cfg.nestedNamed().value().get("foo").someValue(), is("bar"));
            assertThat(cfg.nestedNamed().get("foo").value().someValue(), is("bar"));
            assertThat(cfg.nestedNamed().get("foo").someValue().value(), is("bar"));
        }
    }

    private void change(String hocon) {
        assertThat(
                registry.change(hoconSource(ConfigFactory.parseString(hocon).root())),
                willCompleteSuccessfully()
        );
    }

    private String asHoconStr(List<String> basePath, String... path) {
        List<String> fullPath = Stream.concat(basePath.stream(), Arrays.stream(path)).collect(Collectors.toList());

        ConfigValue hoconCfg = HoconConverter.represent(registry.superRoot(), fullPath);

        return hoconCfg.render(ConfigRenderOptions.concise().setJson(false));
    }
}
