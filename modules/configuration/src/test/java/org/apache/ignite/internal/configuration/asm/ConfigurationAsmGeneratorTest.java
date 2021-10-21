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

package org.apache.ignite.internal.configuration.asm;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.ConfigurationChanger;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.TestConfigurationChanger;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.addDefaults;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Testing the {@link ConfigurationAsmGenerator}.
 */
public class ConfigurationAsmGeneratorTest {
    /** Configuration changer. */
    private static ConfigurationChanger changer;

    /** Configuration generator. */
    private static ConfigurationAsmGenerator generator;

    /** */
    @BeforeAll
    public static void beforeAll() {
        Collection<Class<?>> internalExtensions = List.of(
            ExtendedTestRootConfigurationSchema.class,
            ExtendedSecondTestRootConfigurationSchema.class,
            ExtendedTestConfigurationSchema.class,
            ExtendedSecondTestConfigurationSchema.class
        );

        Collection<Class<?>> polymorphicExtensions = List.of(
            FirstPolymorphicInstanceTestConfigurationSchema.class,
            SecondPolymorphicInstanceTestConfigurationSchema.class
        );

        generator = new ConfigurationAsmGenerator();

        changer = new TestConfigurationChanger(
            generator,
            List.of(TestRootConfiguration.KEY),
            Map.of(),
            new TestConfigurationStorage(LOCAL),
            internalExtensions,
            polymorphicExtensions
        );

        changer.start();
        changer.initializeDefaults();
    }

    /** */
    @AfterAll
    public static void after() {
        changer.stop();

        generator = null;
        changer = null;
    }

    /** */
    @Test
    void testInternalRootConfiguration() throws Exception {
        DynamicConfiguration<?, ?> config = generator.instantiateCfg(TestRootConfiguration.KEY, changer);

        TestRootConfiguration baseRootConfig = (TestRootConfiguration)config;

        ExtendedTestRootConfiguration extendedRootConfig = (ExtendedTestRootConfiguration)config;

        ExtendedSecondTestRootConfiguration extendedSecondRootConfig = (ExtendedSecondTestRootConfiguration)config;

        assertSame(baseRootConfig.i0(), extendedRootConfig.i0());
        assertSame(baseRootConfig.i0(), extendedSecondRootConfig.i0());

        assertSame(baseRootConfig.str0(), extendedRootConfig.str0());
        assertSame(baseRootConfig.str0(), extendedSecondRootConfig.str0());

        assertSame(baseRootConfig.subCfg(), extendedRootConfig.subCfg());
        assertSame(baseRootConfig.subCfg(), extendedSecondRootConfig.subCfg());

        assertSame(baseRootConfig.namedCfg(), extendedRootConfig.namedCfg());
        assertSame(baseRootConfig.namedCfg(), extendedSecondRootConfig.namedCfg());

        assertNotNull(extendedSecondRootConfig.i1());

        // Check view and change interfaces.

        assertTrue(baseRootConfig.value() instanceof ExtendedTestRootView);
        assertTrue(baseRootConfig.value() instanceof ExtendedSecondTestRootView);

        assertSame(baseRootConfig.value(), extendedRootConfig.value());
        assertSame(baseRootConfig.value(), extendedSecondRootConfig.value());

        baseRootConfig.change(c -> {
            assertTrue(c instanceof ExtendedTestRootChange);
            assertTrue(c instanceof ExtendedSecondTestRootChange);

            c.changeI0(10).changeStr0("str0");

            ((ExtendedTestRootChange)c).changeStr1("str1").changeStr0("str0");
            ((ExtendedSecondTestRootChange)c).changeI1(200).changeStr0("str0");
        }).get(1, SECONDS);
    }

    /** */
    @Test
    void testInternalSubConfiguration() throws Exception {
        DynamicConfiguration<?, ?> config = generator.instantiateCfg(TestRootConfiguration.KEY, changer);

        TestRootConfiguration rootConfig = (TestRootConfiguration)config;

        TestConfiguration baseSubConfig = rootConfig.subCfg();

        ExtendedTestConfiguration extendedSubConfig = (ExtendedTestConfiguration)rootConfig.subCfg();

        ExtendedSecondTestConfiguration extendedSecondSubConfig =
            (ExtendedSecondTestConfiguration)rootConfig.subCfg();

        assertSame(baseSubConfig.i0(), extendedSubConfig.i0());
        assertSame(baseSubConfig.i0(), extendedSecondSubConfig.i0());

        assertSame(baseSubConfig.str2(), extendedSubConfig.str2());
        assertSame(baseSubConfig.str2(), extendedSecondSubConfig.str2());

        assertNotNull(extendedSecondSubConfig.i1());

        // Check view and change interfaces.

        assertTrue(baseSubConfig.value() instanceof ExtendedTestView);
        assertTrue(baseSubConfig.value() instanceof ExtendedSecondTestView);

        assertSame(baseSubConfig.value(), extendedSubConfig.value());
        assertSame(baseSubConfig.value(), extendedSecondSubConfig.value());

        baseSubConfig.change(c -> {
            assertTrue(c instanceof ExtendedTestChange);
            assertTrue(c instanceof ExtendedSecondTestChange);

            c.changeI0(10).changeStr2("str2");

            ((ExtendedTestChange)c).changeStr3("str3").changeStr2("str2");
            ((ExtendedSecondTestChange)c).changeI1(200).changeStr2("str2");
        }).get(1, SECONDS);
    }

    /** */
    @Test
    void testInternalNamedConfiguration() throws Exception {
        DynamicConfiguration<?, ?> config = generator.instantiateCfg(TestRootConfiguration.KEY, changer);

        TestRootConfiguration rootConfig = (TestRootConfiguration)config;

        String key = UUID.randomUUID().toString();

        rootConfig.namedCfg().change(c -> c.create(key, c0 -> c0.changeI0(0).changeStr2("foo2"))).get(1, SECONDS);

        TestConfiguration namedConfig = rootConfig.namedCfg().get(key);

        assertTrue(namedConfig instanceof ExtendedTestConfiguration);
        assertTrue(namedConfig instanceof ExtendedSecondTestConfiguration);

        // Check view and change interfaces.

        assertTrue(namedConfig.value() instanceof ExtendedTestView);
        assertTrue(namedConfig.value() instanceof ExtendedSecondTestView);

        namedConfig.change(c -> {
            assertTrue(c instanceof ExtendedTestChange);
            assertTrue(c instanceof ExtendedSecondTestChange);

            c.changeStr2("str2").changeI0(10);

            ((ExtendedTestChange)c).changeStr3("str3").changeStr2("str2");
            ((ExtendedSecondTestChange)c).changeI1(100).changeStr2("str2");
        }).get(1, SECONDS);
    }

    /** */
    @Test
    void testConstructInternalConfig() {
        InnerNode innerNode = generator.instantiateNode(TestRootConfiguration.KEY.schemaClass());

        addDefaults(innerNode);

        InnerNode subInnerNode = (InnerNode)((TestRootView)innerNode).subCfg();

        // Check that no fields for internal configuration will be changed.

        assertThrows(NoSuchElementException.class, () -> innerNode.construct("str1", null, false));
        assertThrows(NoSuchElementException.class, () -> innerNode.construct("i1", null, false));

        assertThrows(NoSuchElementException.class, () -> subInnerNode.construct("str3", null, false));
        assertThrows(NoSuchElementException.class, () -> subInnerNode.construct("i1", null, false));

        // Check that fields for internal configuration will be changed.

        innerNode.construct("str1", null, true);
        innerNode.construct("i1", null, true);

        subInnerNode.construct("str3", null, true);
        subInnerNode.construct("i1", null, true);
    }

    /** */
    @Test
    void testPolymorphicSubConfiguration() throws Exception {
        TestRootConfiguration rootConfig = (TestRootConfiguration)generator.instantiateCfg(TestRootConfiguration.KEY, changer);

        // Check defaults.

        FirstPolymorphicInstanceTestConfiguration firstCfg = (FirstPolymorphicInstanceTestConfiguration)rootConfig.polymorphicSubCfg();
        assertEquals("strVal", firstCfg.strVal().value());
        assertEquals(0, firstCfg.intVal().value());

        FirstPolymorphicInstanceTestView firstVal = (FirstPolymorphicInstanceTestView)firstCfg.value();
        assertEquals("strVal", firstVal.strVal());
        assertEquals(0, firstVal.intVal());

        firstVal = (FirstPolymorphicInstanceTestView)rootConfig.value().polymorphicSubCfg();
        assertEquals("strVal", firstVal.strVal());
        assertEquals(0, firstVal.intVal());

        // Check simple changes.

        firstCfg.strVal().update("strVal1").get(1, SECONDS);
        firstCfg.intVal().update(1).get(1, SECONDS);

        assertEquals("strVal1", firstCfg.strVal().value());
        assertEquals(1, firstCfg.intVal().value());

        firstCfg.change(c -> ((FirstPolymorphicInstanceTestChange)c).changeIntVal(2).changeStrVal("strVal2")).get(1, SECONDS);

        assertEquals("strVal2", firstCfg.strVal().value());
        assertEquals(2, firstCfg.intVal().value());

        rootConfig.change(c -> c.changePolymorphicSubCfg(
            c1 -> ((FirstPolymorphicInstanceTestChange)c1).changeIntVal(3).changeStrVal("strVal3")
        )).get(1, SECONDS);

        assertEquals("strVal3", firstCfg.strVal().value());
        assertEquals(3, firstCfg.intVal().value());

        // Check convert.

        rootConfig.polymorphicSubCfg().change(c -> c.convert(SecondPolymorphicInstanceTestChange.class)).get(1, SECONDS);

        SecondPolymorphicInstanceTestConfiguration secondCfg = (SecondPolymorphicInstanceTestConfiguration)rootConfig.polymorphicSubCfg();
        assertEquals("strVal3", secondCfg.strVal().value());
        assertEquals(0, secondCfg.intVal().value());
        assertEquals(0L, secondCfg.longVal().value());

        SecondPolymorphicInstanceTestView secondView = (SecondPolymorphicInstanceTestView)secondCfg.value();
        assertEquals("strVal3", secondView.strVal());
        assertEquals(0, secondView.intVal());
        assertEquals(0L, secondView.longVal());

        rootConfig.polymorphicSubCfg().change(c -> c.convert(FirstPolymorphicInstanceTestChange.class)).get(1, SECONDS);

        firstCfg = (FirstPolymorphicInstanceTestConfiguration)rootConfig.polymorphicSubCfg();
        assertEquals("strVal3", firstCfg.strVal().value());
        assertEquals(0, firstCfg.intVal().value());
    }

    /**
     * Test root configuration schema.
     */
    @ConfigurationRoot(rootName = "test")
    public static class TestRootConfigurationSchema {
        /** Integer field. */
        @Value(hasDefault = true)
        public int i0 = 0;

        /** String field. */
        @Value(hasDefault = true)
        public String str0 = "str0";

        /** Sub configuration field. */
        @ConfigValue
        public TestConfigurationSchema subCfg;

        /** Named configuration field. */
        @NamedConfigValue
        public TestConfigurationSchema namedCfg;

        /** Polymorphic sub configuration field. */
        @ConfigValue
        public PolymorphicTestConfigurationSchema polymorphicSubCfg;
    }

    /**
     * Extending the {@link TestRootConfigurationSchema}.
     */
    @InternalConfiguration
    public static class ExtendedTestRootConfigurationSchema extends TestRootConfigurationSchema {
        /** String field. */
        @Value(hasDefault = true)
        public String str1 = "str1";
    }

    /**
     * Extending the {@link TestRootConfigurationSchema}.
     */
    @InternalConfiguration
    public static class ExtendedSecondTestRootConfigurationSchema extends TestRootConfigurationSchema {
        /** Integer field. */
        @Value(hasDefault = true)
        public int i1 = 0;
    }

    /**
     * Test configuration schema.
     */
    @Config
    public static class TestConfigurationSchema {
        /** Integer field. */
        @Value(hasDefault = true)
        public int i0 = 0;

        /** String field. */
        @Value(hasDefault = true)
        public String str2 = "str2";
    }

    /**
     * Extending the {@link TestConfigurationSchema}.
     */
    @InternalConfiguration
    public static class ExtendedTestConfigurationSchema extends TestConfigurationSchema {
        /** String field. */
        @Value(hasDefault = true)
        public String str3 = "str3";
    }

    /**
     * Extending the {@link TestConfigurationSchema}.
     */
    @InternalConfiguration
    public static class ExtendedSecondTestConfigurationSchema extends TestConfigurationSchema {
        /** Integer field. */
        @Value(hasDefault = true)
        public int i1 = 0;
    }

    /**
     * Polymorphic configuration scheme.
     */
    @PolymorphicConfig
    public static class PolymorphicTestConfigurationSchema {
        /** Polymorphic type id field. */
        @PolymorphicId(hasDefault = true)
        public String typeId = "first";

        /** String value. */
        @Value(hasDefault = true)
        public String strVal = "strVal";
    }

    /**
     * First instance of the polymorphic configuration schema.
     */
    @PolymorphicConfigInstance(id = "first")
    public static class FirstPolymorphicInstanceTestConfigurationSchema extends PolymorphicTestConfigurationSchema {
        /** Integer value. */
        @Value(hasDefault = true)
        public int intVal = 0;
    }

    /**
     * Second instance of the polymorphic configuration schema.
     */
    @PolymorphicConfigInstance(id = "second")
    public static class SecondPolymorphicInstanceTestConfigurationSchema extends PolymorphicTestConfigurationSchema {
        /** Integer value. */
        @Value(hasDefault = true)
        public int intVal = 0;

        /** Long value. */
        @Value(hasDefault = true)
        public long longVal = 0;
    }
}
