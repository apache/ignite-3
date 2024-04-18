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

package org.apache.ignite.internal.configuration.presentation;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Testing the {@link ConfigurationPresentation}.
 */
public class HoconPresentationTest {
    /** Configuration generator. */
    private static ConfigurationTreeGenerator generator;

    /** Configuration registry. */
    private static ConfigurationRegistry cfgRegistry;

    /** Configuration presentation. */
    private static ConfigurationPresentation<String> cfgPresentation;

    /** Test root configuration. */
    private static TestRootConfiguration cfg;

    /**
     * Before all.
     */
    @BeforeAll
    static void beforeAll() {
        Validator<Value, Object> validator = new Validator<>() {
            /** {@inheritDoc} */
            @Override
            public void validate(Value annotation, ValidationContext<Object> ctx) {
                if (Objects.equals("error", ctx.getNewValue())) {
                    ctx.addIssue(new ValidationIssue(ctx.currentKey(), "Error word"));
                }
            }
        };

        generator = new ConfigurationTreeGenerator(TestRootConfiguration.KEY);

        cfgRegistry = new ConfigurationRegistry(
                List.of(TestRootConfiguration.KEY),
                new TestConfigurationStorage(LOCAL),
                generator,
                ConfigurationValidatorImpl.withDefaultValidators(generator, Set.of(validator))
        );

        cfgRegistry.startAsync();

        cfgPresentation = new HoconPresentation(cfgRegistry);

        cfg = cfgRegistry.getConfiguration(TestRootConfiguration.KEY);
    }

    /**
     * After all.
     */
    @AfterAll
    static void afterAll() throws Exception {
        cfgRegistry.stopAsync();
        cfgRegistry = null;

        generator.close();
        generator = null;

        cfgPresentation = null;

        cfg = null;
    }

    /**
     * Before each.
     */
    @BeforeEach
    void beforeEach() throws Exception {
        cfg.change(cfg -> cfg.changeFoo("foo").changeSubCfg(subCfg -> subCfg.changeBar("bar"))).get(1, SECONDS);
    }

    @Test
    void testRepresentWholeCfg() {
        String s = "{\"root\":{\"foo\":\"foo\",\"subCfg\":{\"bar\":\"bar\"}}}";

        assertEquals(s, cfgPresentation.represent());
        assertEquals(s, cfgPresentation.representByPath(null));
    }

    @Test
    void testCorrectRepresentCfgByPath() {
        assertEquals("{\"foo\":\"foo\",\"subCfg\":{\"bar\":\"bar\"}}", cfgPresentation.representByPath("root"));
        assertEquals("\"foo\"", cfgPresentation.representByPath("root.foo"));
        assertEquals("{\"bar\":\"bar\"}", cfgPresentation.representByPath("root.subCfg"));
        assertEquals("\"bar\"", cfgPresentation.representByPath("root.subCfg.bar"));
    }

    @Test
    void testErrorRepresentCfgByPath() {
        assertThrows(
                IllegalArgumentException.class,
                () -> cfgPresentation.representByPath(UUID.randomUUID().toString())
        );
    }

    @Test
    void testCorrectUpdateFullCfg() {
        String updateVal = "{\"root\":{\"foo\":\"bar\",\"subCfg\":{\"bar\":\"foo\"}}}";

        assertThat(cfgPresentation.update(updateVal), willBe(nullValue(Void.class)));

        assertEquals("bar", cfg.foo().value());
        assertEquals("foo", cfg.subCfg().bar().value());
        assertEquals(updateVal, cfgPresentation.represent());
    }

    @Test
    void testCorrectUpdateSubCfg() {
        String updateVal = "{\"root\":{\"subCfg\":{\"bar\":\"foo\"}}}";

        assertThat(cfgPresentation.update(updateVal), willBe(nullValue(Void.class)));

        assertEquals("foo", cfg.foo().value());
        assertEquals("foo", cfg.subCfg().bar().value());
        assertEquals("{\"root\":{\"foo\":\"foo\",\"subCfg\":{\"bar\":\"foo\"}}}", cfgPresentation.represent());
    }

    @Test
    void testErrorUpdateCfg() {
        assertFutureThrows(
                IllegalArgumentException.class,
                cfgPresentation.update("{\"root\":{\"foo\":100,\"subCfg\":{\"bar\":\"foo\"}}}")
        );

        assertFutureThrows(
                IllegalArgumentException.class,
                cfgPresentation.update("{\"root0\":{\"foo\":\"foo\",\"subCfg\":{\"bar\":\"foo\"}}}")
        );

        assertFutureThrows(IllegalArgumentException.class, cfgPresentation.update("{"));

        assertFutureThrows(IllegalArgumentException.class, cfgPresentation.update(""));

        assertFutureThrows(
                ConfigurationValidationException.class,
                cfgPresentation.update("{\"root\":{\"foo\":\"error\",\"subCfg\":{\"bar\":\"foo\"}}}")
        );
    }

    private static void assertFutureThrows(Class<?> expectedType, CompletableFuture<?> future) {
        ExecutionException e = assertThrows(ExecutionException.class, () -> future.get(1, SECONDS));

        assertThat(e.getCause(), isA(expectedType));
    }

    /**
     * Test root configuration schema.
     */
    @ConfigurationRoot(rootName = "root")
    public static class TestRootConfigurationSchema {
        /** Foo field. */
        @Value(hasDefault = true)
        public String foo = "foo";

        /** Sub configuration schema. */
        @ConfigValue
        public TestSubConfigurationSchema subCfg;
    }

    /**
     * Test sub configuration schema.
     */
    @Config
    public static class TestSubConfigurationSchema {
        /** Bar field. */
        @Value(hasDefault = true)
        public String bar = "bar";
    }
}
