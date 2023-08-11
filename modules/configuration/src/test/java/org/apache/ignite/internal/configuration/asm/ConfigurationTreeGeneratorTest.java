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

package org.apache.ignite.internal.configuration.asm;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.addDefaults;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.configuration.ConfigurationReadOnlyException;
import org.apache.ignite.configuration.ConfigurationWrongPolymorphicTypeIdException;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.AbstractConfiguration;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.InternalId;
import org.apache.ignite.configuration.annotation.Name;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.ConfigurationChanger;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.TestConfigurationChanger;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Testing the {@link ConfigurationTreeGenerator}.
 */
public class ConfigurationTreeGeneratorTest {
    /** Configuration generator. */
    private static ConfigurationTreeGenerator generator;


    private static Collection<Class<?>> extensions = List.of(
            ExtendedTestRootConfigurationSchema.class,
            ExtendedSecondTestRootConfigurationSchema.class,
            ExtendedTestConfigurationSchema.class,
            ExtendedSecondTestConfigurationSchema.class,
            ExtendedPublicTestRootConfigurationSchema.class,
            ExtendedPublicTestConfigurationSchema.class
    );

    private static Collection<Class<?>> polymorphicExtensions = List.of(
            FirstPolymorphicInstanceTestConfigurationSchema.class,
            SecondPolymorphicInstanceTestConfigurationSchema.class,
            NonDefaultPolymorphicInstanceTestConfigurationSchema.class,
            FirstPolymorphicNamedInstanceTestConfigurationSchema.class,
            SecondPolymorphicNamedInstanceTestConfigurationSchema.class,
            PolyInst0InjectedNameConfigurationSchema.class,
            PolyInst1InjectedNameConfigurationSchema.class
    );

    private static Collection<RootKey<?, ?>> rootKeys = List.of(
            TestRootConfiguration.KEY,
            InjectedNameRootConfiguration.KEY,
            RootFromAbstractConfiguration.KEY
    );

    /** Configuration changer. */
    private ConfigurationChanger changer;

    @BeforeAll
    public static void beforeAll() {
        generator = new ConfigurationTreeGenerator(rootKeys, extensions, polymorphicExtensions);
    }

    @AfterAll
    public static void afterAll() {
        generator.close();
    }

    @BeforeEach
    void beforeEach() {
        changer = new TestConfigurationChanger(
                rootKeys,
                new TestConfigurationStorage(LOCAL),
                generator,
                new TestConfigurationValidator()
        );

        changer.start();
    }

    @AfterEach
    void afterEach() throws Exception {
        changer.stop();
    }

    @Test
    void testInternalRootConfiguration() throws Exception {
        DynamicConfiguration<?, ?> config = generator.instantiateCfg(TestRootConfiguration.KEY, changer);

        TestRootConfiguration baseRootConfig = (TestRootConfiguration) config;

        ExtendedTestRootConfiguration extendedRootConfig = (ExtendedTestRootConfiguration) config;

        ExtendedSecondTestRootConfiguration extendedSecondRootConfig = (ExtendedSecondTestRootConfiguration) config;

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

            ((ExtendedTestRootChange) c).changeStr1("str1").changeStr0("str0");
            ((ExtendedSecondTestRootChange) c).changeI1(200).changeStr0("str0");
        }).get(1, SECONDS);
    }

    @Test
    void testInternalSubConfiguration() throws Exception {
        DynamicConfiguration<?, ?> config = generator.instantiateCfg(TestRootConfiguration.KEY, changer);

        TestRootConfiguration rootConfig = (TestRootConfiguration) config;

        TestConfiguration baseSubConfig = rootConfig.subCfg();

        ExtendedTestConfiguration extendedSubConfig = (ExtendedTestConfiguration) rootConfig.subCfg();

        ExtendedSecondTestConfiguration extendedSecondSubConfig =
                (ExtendedSecondTestConfiguration) rootConfig.subCfg();

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

            ((ExtendedTestChange) c).changeStr3("str3").changeStr2("str2");
            ((ExtendedSecondTestChange) c).changeI1(200).changeStr2("str2");
        }).get(1, SECONDS);

        rootConfig.change(r -> {
            TestChange subCfg = r.changeSubCfg().changeStr2("str3");

            assertTrue(subCfg instanceof ExtendedTestChange);
        }).get(1, SECONDS);
    }

    @Test
    void testInternalNamedConfiguration() throws Exception {
        DynamicConfiguration<?, ?> config = generator.instantiateCfg(TestRootConfiguration.KEY, changer);

        TestRootConfiguration rootConfig = (TestRootConfiguration) config;

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

            ((ExtendedTestChange) c).changeStr3("str3").changeStr2("str2");
            ((ExtendedSecondTestChange) c).changeI1(100).changeStr2("str2");
        }).get(1, SECONDS);
    }

    @Test
    void testConstructExtendedConfig() {
        InnerNode innerNode = generator.instantiateNode(TestRootConfiguration.KEY.schemaClass());

        addDefaults(innerNode);

        InnerNode subInnerNode = (InnerNode) ((TestRootView) innerNode).subCfg();

        // Check that no fields for internal configuration will be changed.

        assertThrows(NoSuchElementException.class, () -> innerNode.construct("str1", null, false));
        assertThrows(NoSuchElementException.class, () -> innerNode.construct("i1", null, false));

        assertThrows(NoSuchElementException.class, () -> subInnerNode.construct("str3", null, false));
        assertThrows(NoSuchElementException.class, () -> subInnerNode.construct("i1", null, false));

        // Check that public extensions will not lead to an exception
        innerNode.construct("pub1", null, false);

        // Check that fields for internal configuration will be changed.

        innerNode.construct("str1", null, true);
        innerNode.construct("i1", null, true);

        subInnerNode.construct("str3", null, true);
        subInnerNode.construct("i1", null, true);
        subInnerNode.construct("pub2", null, true);
    }

    @Test
    void testPolymorphicSubConfiguration() throws Exception {
        TestRootConfiguration rootConfig = (TestRootConfiguration) generator.instantiateCfg(TestRootConfiguration.KEY, changer);

        // Check defaults.

        FirstPolymorphicInstanceTestConfiguration firstCfg = (FirstPolymorphicInstanceTestConfiguration) rootConfig.polymorphicSubCfg();
        assertEquals("first", firstCfg.typeId().value());
        assertEquals("strVal", firstCfg.strVal().value());
        assertEquals(0, firstCfg.intVal().value());

        FirstPolymorphicInstanceTestView firstVal = (FirstPolymorphicInstanceTestView) firstCfg.value();
        assertEquals("first", firstVal.typeId());
        assertEquals("strVal", firstVal.strVal());
        assertEquals(0, firstVal.intVal());

        firstVal = (FirstPolymorphicInstanceTestView) rootConfig.value().polymorphicSubCfg();
        assertEquals("first", firstVal.typeId());
        assertEquals("strVal", firstVal.strVal());
        assertEquals(0, firstVal.intVal());

        // Check simple changes.

        firstCfg.strVal().update("strVal1").get(1, SECONDS);
        firstCfg.intVal().update(1).get(1, SECONDS);

        assertEquals("first", firstCfg.typeId().value());
        assertEquals("strVal1", firstCfg.strVal().value());
        assertEquals(1, firstCfg.intVal().value());

        firstCfg.change(c -> ((FirstPolymorphicInstanceTestChange) c).changeIntVal(2).changeStrVal("strVal2")).get(1, SECONDS);

        assertEquals("first", firstCfg.typeId().value());
        assertEquals("strVal2", firstCfg.strVal().value());
        assertEquals(2, firstCfg.intVal().value());

        rootConfig.change(c -> c.changePolymorphicSubCfg(
                c1 -> ((FirstPolymorphicInstanceTestChange) c1).changeIntVal(3).changeStrVal("strVal3")
        )).get(1, SECONDS);

        assertEquals("first", firstCfg.typeId().value());
        assertEquals("strVal3", firstCfg.strVal().value());
        assertEquals(3, firstCfg.intVal().value());

        rootConfig.change(c -> ((FirstPolymorphicInstanceTestChange) c.changePolymorphicSubCfg()).changeIntVal(4).changeStrVal("strVal4"))
                .get(1, SECONDS);

        assertEquals("first", firstCfg.typeId().value());
        assertEquals("strVal4", firstCfg.strVal().value());
        assertEquals(4, firstCfg.intVal().value());

        // Check convert.

        rootConfig.polymorphicSubCfg()
                .change(c -> {
                    assertThat(
                            c.convert(FirstPolymorphicInstanceTestChange.class),
                            allOf(instanceOf(FirstPolymorphicInstanceTestChange.class), instanceOf(ConstructableTreeNode.class))
                    );

                    assertThat(
                            c.convert(SecondPolymorphicInstanceTestChange.class),
                            allOf(instanceOf(SecondPolymorphicInstanceTestChange.class), instanceOf(ConstructableTreeNode.class))
                    );

                    assertThat(
                            c.convert(PolymorphicTestConfigurationSchema.FIRST),
                            allOf(instanceOf(FirstPolymorphicInstanceTestChange.class), instanceOf(ConstructableTreeNode.class))
                    );

                    assertThat(
                            c.convert(PolymorphicTestConfigurationSchema.SECOND),
                            allOf(instanceOf(SecondPolymorphicInstanceTestChange.class), instanceOf(ConstructableTreeNode.class))
                    );

                    assertThrows(ConfigurationWrongPolymorphicTypeIdException.class, () -> c.convert(UUID.randomUUID().toString()));
                })
                .get(1, SECONDS);

        SecondPolymorphicInstanceTestConfiguration secondCfg = (SecondPolymorphicInstanceTestConfiguration) rootConfig.polymorphicSubCfg();
        assertEquals("second", secondCfg.typeId().value());
        assertEquals("strVal4", secondCfg.strVal().value());
        assertEquals(0, secondCfg.intVal().value());
        assertEquals(0L, secondCfg.longVal().value());

        SecondPolymorphicInstanceTestView secondView = (SecondPolymorphicInstanceTestView) secondCfg.value();
        assertEquals("second", secondView.typeId());
        assertEquals("strVal4", secondView.strVal());
        assertEquals(0, secondView.intVal());
        assertEquals(0L, secondView.longVal());

        rootConfig.polymorphicSubCfg().change(c -> c.convert(FirstPolymorphicInstanceTestChange.class)).get(1, SECONDS);

        firstCfg = (FirstPolymorphicInstanceTestConfiguration) rootConfig.polymorphicSubCfg();
        assertEquals("first", firstCfg.typeId().value());
        assertEquals("strVal4", firstCfg.strVal().value());
        assertEquals(0, firstCfg.intVal().value());
    }

    @Test
    void testPolymorphicErrors() {
        TestRootConfiguration rootConfig = (TestRootConfiguration) generator.instantiateCfg(TestRootConfiguration.KEY, changer);

        PolymorphicTestConfiguration polymorphicCfg = rootConfig.polymorphicSubCfg();

        // Checks for an error on an attempt to update polymorphicTypeId field.
        assertThrows(
                ConfigurationReadOnlyException.class,
                () -> polymorphicCfg.typeId().update("second").get(1, SECONDS)
        );

        // Checks for an error on an attempt to read a field of a different type of polymorphic configuration.
        assertThrows(ExecutionException.class, () -> polymorphicCfg.change(c -> {
                            FirstPolymorphicInstanceTestView firstView = (FirstPolymorphicInstanceTestView) c;

                            c.convert(SecondPolymorphicInstanceTestChange.class);

                            firstView.intVal();
                        }
                ).get(1, SECONDS)
        );

        // Checks for an error on an attempt to change a field of a different type of polymorphic configuration.
        assertThrows(ExecutionException.class, () -> polymorphicCfg.change(c -> {
                            FirstPolymorphicInstanceTestChange firstChange = (FirstPolymorphicInstanceTestChange) c;

                            c.convert(SecondPolymorphicInstanceTestChange.class);

                            firstChange.changeIntVal(10);
                        }
                ).get(1, SECONDS)
        );
    }

    @Test
    void testPolymorphicNamedConfigurationAdd() throws Exception {
        TestRootConfiguration rootConfig = (TestRootConfiguration) generator.instantiateCfg(TestRootConfiguration.KEY, changer);

        // Check add named polymorphic config.

        rootConfig.polymorphicNamedCfg()
                .change(c -> c.create("0", c1 -> c1.convert(FirstPolymorphicNamedInstanceTestChange.class))
                        .create("1", c1 -> c1.convert(SecondPolymorphicNamedInstanceTestChange.class)
                                .changeIntVal(1)
                                .changeLongVal(1)
                                .changeStrVal("strVal1")
                        )
                ).get(1, SECONDS);

        FirstPolymorphicNamedInstanceTestConfiguration firstCfg =
                (FirstPolymorphicNamedInstanceTestConfiguration) rootConfig.polymorphicNamedCfg().get("0");

        assertEquals("strVal", firstCfg.strVal().value());
        assertEquals(0, firstCfg.intVal().value());

        SecondPolymorphicNamedInstanceTestConfiguration secondCfg =
                (SecondPolymorphicNamedInstanceTestConfiguration) rootConfig.polymorphicNamedCfg().get("1");

        assertEquals("strVal1", secondCfg.strVal().value());
        assertEquals(1, secondCfg.intVal().value());
        assertEquals(1L, secondCfg.longVal().value());

        // Check config values.
        FirstPolymorphicNamedInstanceTestView firstVal = (FirstPolymorphicNamedInstanceTestView) firstCfg.value();

        assertEquals("strVal", firstVal.strVal());
        assertEquals(0, firstVal.intVal());

        firstVal = (FirstPolymorphicNamedInstanceTestView) rootConfig.polymorphicNamedCfg().value().get("0");

        assertEquals("strVal", firstVal.strVal());
        assertEquals(0, firstVal.intVal());

        firstVal = (FirstPolymorphicNamedInstanceTestView) rootConfig.value().polymorphicNamedCfg().get("0");

        assertEquals("strVal", firstVal.strVal());
        assertEquals(0, firstVal.intVal());

        SecondPolymorphicNamedInstanceTestView secondVal = (SecondPolymorphicNamedInstanceTestView) secondCfg.value();

        assertEquals("strVal1", secondVal.strVal());
        assertEquals(1, secondVal.intVal());
        assertEquals(1L, secondVal.longVal());

        secondVal = (SecondPolymorphicNamedInstanceTestView) rootConfig.polymorphicNamedCfg().value().get("1");

        assertEquals("strVal1", secondVal.strVal());
        assertEquals(1, secondVal.intVal());
        assertEquals(1L, secondVal.longVal());

        secondVal = (SecondPolymorphicNamedInstanceTestView) rootConfig.value().polymorphicNamedCfg().get("1");

        assertEquals("strVal1", secondVal.strVal());
        assertEquals(1, secondVal.intVal());
        assertEquals(1L, secondVal.longVal());
    }

    @Test
    void testPolymorphicNamedConfigurationChange() throws Exception {
        TestRootConfiguration rootConfig = (TestRootConfiguration) generator.instantiateCfg(TestRootConfiguration.KEY, changer);

        rootConfig.polymorphicNamedCfg()
                .change(c -> c.create("0", c1 -> c1.convert(FirstPolymorphicNamedInstanceTestChange.class)))
                .get(1, SECONDS);

        FirstPolymorphicNamedInstanceTestConfiguration firstCfg =
                (FirstPolymorphicNamedInstanceTestConfiguration) rootConfig.polymorphicNamedCfg().get("0");

        firstCfg.intVal().update(1).get(1, SECONDS);

        assertEquals(1, firstCfg.intVal().value());

        firstCfg.change(c -> ((FirstPolymorphicNamedInstanceTestChange) c).changeIntVal(2).changeStrVal("strVal2"))
                .get(1, SECONDS);

        assertEquals(2, firstCfg.intVal().value());
        assertEquals("strVal2", firstCfg.strVal().value());

        // Check convert.

        rootConfig.polymorphicNamedCfg().get("0")
                .change(c -> c.convert(SecondPolymorphicNamedInstanceTestChange.class)
                        .changeLongVal(3)
                        .changeIntVal(3)
                        .changeStrVal("strVal3")
                ).get(1, SECONDS);

        SecondPolymorphicNamedInstanceTestConfiguration secondCfg =
                (SecondPolymorphicNamedInstanceTestConfiguration) rootConfig.polymorphicNamedCfg().get("0");

        assertEquals(3, secondCfg.intVal().value());
        assertEquals(3L, secondCfg.longVal().value());
        assertEquals("strVal3", secondCfg.strVal().value());
    }

    @Test
    void testPolymorphicNamedConfigurationRemove() throws Exception {
        TestRootConfiguration rootConfig = (TestRootConfiguration) generator.instantiateCfg(TestRootConfiguration.KEY, changer);

        rootConfig.polymorphicNamedCfg()
                .change(c -> c.create("0", c1 -> c1.convert(FirstPolymorphicNamedInstanceTestChange.class)))
                .get(1, SECONDS);

        rootConfig.polymorphicNamedCfg().change(c -> c.delete("0")).get(1, SECONDS);

        assertNull(rootConfig.polymorphicNamedCfg().get("0"));
    }

    /**
     * Tests changing type of a Polymorphic Configuration to a type that has a field without a default value.
     */
    @Test
    void testPolymorphicConfigurationNonDefaultValues() throws Exception {
        TestRootConfiguration rootConfig = (TestRootConfiguration) generator.instantiateCfg(TestRootConfiguration.KEY, changer);

        rootConfig.polymorphicSubCfg()
                .change(c -> c.convert(NonDefaultPolymorphicInstanceTestChange.class).changeNonDefaultValue("foo"))
                .get(1, SECONDS);

        PolymorphicTestView view = rootConfig.value().polymorphicSubCfg();

        assertThat(view, is(instanceOf(NonDefaultPolymorphicInstanceTestView.class)));
        assertThat(((NonDefaultPolymorphicInstanceTestView) view).nonDefaultValue(), is("foo"));
    }

    @Test
    void testNestedConfigurationWithInjectedNameField() throws Exception {
        InjectedNameRootConfiguration rootCfg =
                (InjectedNameRootConfiguration) generator.instantiateCfg(InjectedNameRootConfiguration.KEY, changer);

        // Checks for common config.
        assertThrows(
                ConfigurationReadOnlyException.class,
                () -> rootCfg.nested().name().update("test").get(1, SECONDS)
        );

        assertEquals("nestedDefault", rootCfg.nested().name().value());
        assertEquals("nestedDefault", rootCfg.value().nested().name());
        assertEquals("nestedDefault", rootCfg.nested().value().name());

        // Checks for polymorphic configs.
        assertThrows(
                ConfigurationReadOnlyException.class,
                () -> rootCfg.nestedPoly().name().update("test").get(1, SECONDS)
        );

        assertEquals("nestedDefaultPoly", rootCfg.nestedPoly().name().value());
        assertEquals("nestedDefaultPoly", rootCfg.value().nestedPoly().name());
        assertEquals("nestedDefaultPoly", rootCfg.nestedPoly().value().name());

        rootCfg.nestedPoly().change(c -> c.convert(PolyInst1InjectedNameChange.class)).get(1, SECONDS);

        assertEquals("nestedDefaultPoly", rootCfg.nestedPoly().name().value());
        assertEquals("nestedDefaultPoly", rootCfg.value().nestedPoly().name());
        assertEquals("nestedDefaultPoly", rootCfg.nestedPoly().value().name());
    }

    @Test
    void testNestedNamedConfigurationWithInjectedNameField() throws Exception {
        InjectedNameRootConfiguration rootCfg =
                (InjectedNameRootConfiguration) generator.instantiateCfg(InjectedNameRootConfiguration.KEY, changer);

        // Checks for common named config.
        rootCfg.nestedNamed().change(c -> c.create("0", c0 -> assertEquals("0", c0.name()))).get(1, SECONDS);
        rootCfg.nestedNamed().change(c -> c.create(1, "1", c0 -> assertEquals("1", c0.name()))).get(1, SECONDS);
        rootCfg.nestedNamed().change(c -> c.createOrUpdate("2", c0 -> assertEquals("2", c0.name()))).get(1, SECONDS);
        rootCfg.nestedNamed().change(c -> c.createAfter("2", "3", c0 -> assertEquals("3", c0.name()))).get(1, SECONDS);

        rootCfg.nestedNamed().change(c -> c.update("3", c0 -> assertEquals("3", c0.name()))).get(1, SECONDS);

        rootCfg.nestedNamed().change(c -> assertEquals("4", c.rename("3", "4").get("4").name())).get(1, SECONDS);

        assertEquals("0", rootCfg.value().nestedNamed().get("0").name());
        assertEquals("1", rootCfg.nestedNamed().value().get("1").name());
        assertEquals("2", rootCfg.nestedNamed().get("2").value().name());
        assertEquals("4", rootCfg.nestedNamed().get("4").name().value());

        // Checks for polymorphic configs.

        rootCfg.nestedNamedPoly().change(c -> c.create("p0", c0 -> assertEquals("p0", c0.name()))).get(1, SECONDS);
        rootCfg.nestedNamedPoly().change(c -> c.create(1, "p1", c0 -> assertEquals("p1", c0.name()))).get(1, SECONDS);
        rootCfg.nestedNamedPoly().change(c -> c.createOrUpdate("p2", c0 -> assertEquals("p2", c0.name()))).get(1, SECONDS);
        rootCfg.nestedNamedPoly().change(c -> c.createAfter("p2", "p3", c0 -> assertEquals("p3", c0.name()))).get(1, SECONDS);

        rootCfg.nestedNamedPoly().change(c -> c.update("p3", c0 -> assertEquals("p3", c0.name()))).get(1, SECONDS);

        rootCfg.nestedNamedPoly().change(c -> assertEquals("p4", c.rename("p3", "p4").get("p4").name())).get(1, SECONDS);

        assertEquals("p0", rootCfg.value().nestedNamedPoly().get("p0").name());
        assertEquals("p1", rootCfg.nestedNamedPoly().value().get("p1").name());
        assertEquals("p2", rootCfg.nestedNamedPoly().get("p2").value().name());
        assertEquals("p4", rootCfg.nestedNamedPoly().get("p4").name().value());

        rootCfg.nestedNamedPoly().change(c -> c.update("p0", c0 -> c0.convert(PolyInst1InjectedNameChange.class))).get(1, SECONDS);
        rootCfg.nestedNamedPoly().change(c -> c.update("p1", c0 -> c0.convert(PolyInst1InjectedNameChange.class))).get(1, SECONDS);
        rootCfg.nestedNamedPoly().change(c -> c.update("p2", c0 -> c0.convert(PolyInst1InjectedNameChange.class))).get(1, SECONDS);
        rootCfg.nestedNamedPoly().change(c -> c.update("p4", c0 -> c0.convert(PolyInst1InjectedNameChange.class))).get(1, SECONDS);

        assertEquals("p0", rootCfg.value().nestedNamedPoly().get("p0").name());
        assertEquals("p1", rootCfg.nestedNamedPoly().value().get("p1").name());
        assertEquals("p2", rootCfg.nestedNamedPoly().get("p2").value().name());
        assertEquals("p4", rootCfg.nestedNamedPoly().get("p4").name().value());
    }

    @Test
    void testAbstractConfiguration() throws Exception {
        RootFromAbstractConfiguration rootFromAbstractConfig = (RootFromAbstractConfiguration) generator.instantiateCfg(
                RootFromAbstractConfiguration.KEY,
                changer
        );

        // Checks for default values.

        assertEquals("test", rootFromAbstractConfig.configFromAbstract().name().value());

        assertEquals("strVal", rootFromAbstractConfig.strVal().value());
        assertEquals(500100, rootFromAbstractConfig.longVal().value());

        assertEquals(100500, rootFromAbstractConfig.configFromAbstract().intVal().value());
        assertTrue(rootFromAbstractConfig.configFromAbstract().booleanVal().value());

        // Checks for changes to all fields at once.

        rootFromAbstractConfig.change(ch0 -> ch0
                .changeLongVal(1)
                .changeConfigFromAbstract(ch1 -> ch1.changeBooleanVal(false).changeIntVal(1))
                .changeStrVal("1")
        ).get(1, SECONDS);

        RootFromAbstractView fromAbstractView0 = rootFromAbstractConfig.value();

        assertEquals("1", fromAbstractView0.strVal());
        assertEquals(1, fromAbstractView0.longVal());

        assertEquals(1, fromAbstractView0.configFromAbstract().intVal());
        assertFalse(fromAbstractView0.configFromAbstract().booleanVal());

        // Checks for changes to each field.

        rootFromAbstractConfig.longVal().update(2L).get(1, SECONDS);
        rootFromAbstractConfig.strVal().update("2").get(1, SECONDS);

        rootFromAbstractConfig.configFromAbstract().intVal().update(2).get(1, SECONDS);
        rootFromAbstractConfig.configFromAbstract().booleanVal().update(true).get(1, SECONDS);

        assertEquals("2", rootFromAbstractConfig.strVal().value());
        assertEquals(2, rootFromAbstractConfig.longVal().value());

        assertEquals(2, rootFromAbstractConfig.configFromAbstract().intVal().value());
        assertTrue(rootFromAbstractConfig.configFromAbstract().booleanVal().value());

        // Check "short" version of change method.
        rootFromAbstractConfig.change(ch0 -> ch0
                .changeConfigFromAbstract().changeBooleanVal(true).changeIntVal(3)
        ).get(1, SECONDS);

        RootFromAbstractView fromAbstractView2 = rootFromAbstractConfig.value();

        assertEquals(3, fromAbstractView2.configFromAbstract().intVal());
        assertTrue(fromAbstractView2.configFromAbstract().booleanVal());
    }

    @Test
    void testUuidValueAnnotationField() throws Exception {
        TestRootConfiguration rootConfig = (TestRootConfiguration) generator.instantiateCfg(TestRootConfiguration.KEY, changer);

        UUID uuid0 = rootConfig.uuid0().value();

        assertNotNull(uuid0);

        UUID newUuid0 = UUID.randomUUID();

        rootConfig.uuid0().update(newUuid0).get(1, SECONDS);

        assertEquals(newUuid0, rootConfig.uuid0().value());

        UUID uuid1 = rootConfig.subCfg().uuid1().value();

        assertNotNull(uuid1);

        UUID newUuid1 = UUID.randomUUID();

        rootConfig.subCfg().uuid1().update(newUuid1).get(1, SECONDS);

        assertEquals(newUuid1, rootConfig.subCfg().uuid1().value());
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

        /** UUID field. */
        @Value(hasDefault = true)
        public UUID uuid0 = UUID.randomUUID();

        /** Sub configuration field. */
        @ConfigValue
        public TestConfigurationSchema subCfg;

        /** Named configuration field. */
        @NamedConfigValue
        public TestConfigurationSchema namedCfg;

        /** Polymorphic sub configuration field. */
        @ConfigValue
        public PolymorphicTestConfigurationSchema polymorphicSubCfg;

        /** Polymorphic named configuration field. */
        @NamedConfigValue
        public PolymorphicNamedTestConfigurationSchema polymorphicNamedCfg;
    }

    /**
     * Extending the {@link TestRootConfigurationSchema}.
     */
    @ConfigurationExtension(internal = true)
    public static class ExtendedTestRootConfigurationSchema extends TestRootConfigurationSchema {
        /** String field. */
        @Value(hasDefault = true)
        public String str1 = "str1";
    }

    /**
     * Extending the {@link TestRootConfigurationSchema}.
     */
    @ConfigurationExtension(internal = true)
    public static class ExtendedSecondTestRootConfigurationSchema extends TestRootConfigurationSchema {
        /** Integer field. */
        @Value(hasDefault = true)
        public int i1 = 0;
    }

    /**
     * Extending the {@link TestRootConfigurationSchema}  with a public extension.
     */
    @ConfigurationExtension
    public static class ExtendedPublicTestRootConfigurationSchema extends TestRootConfigurationSchema {
        /** Integer field. */
        @Value(hasDefault = true)
        public int pub1 = 42;
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

        /** UUID field. */
        @Value(hasDefault = true)
        public UUID uuid1 = UUID.randomUUID();
    }

    /**
     * Extending the {@link TestConfigurationSchema}.
     */
    @ConfigurationExtension(internal = true)
    public static class ExtendedTestConfigurationSchema extends TestConfigurationSchema {
        /** String field. */
        @Value(hasDefault = true)
        public String str3 = "str3";
    }

    /**
     * Extending the {@link TestConfigurationSchema}.
     */
    @ConfigurationExtension(internal = true)
    public static class ExtendedSecondTestConfigurationSchema extends TestConfigurationSchema {
        /** Integer field. */
        @Value(hasDefault = true)
        public int i1 = 0;
    }

    /**
     * Extending the {@link TestConfigurationSchema} with a public extension.
     */
    @ConfigurationExtension
    public static class ExtendedPublicTestConfigurationSchema extends TestConfigurationSchema {
        /** Integer field. */
        @Value(hasDefault = true)
        public int pub2 = 22;
    }

    /**
     * Polymorphic configuration scheme.
     */
    @PolymorphicConfig
    public static class PolymorphicTestConfigurationSchema {
        static final String FIRST = "first";

        static final String SECOND = "second";

        static final String NON_DEFAULT = "non_default";

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
    @PolymorphicConfigInstance(PolymorphicTestConfigurationSchema.FIRST)
    public static class FirstPolymorphicInstanceTestConfigurationSchema extends PolymorphicTestConfigurationSchema {
        /** Integer value. */
        @Value(hasDefault = true)
        public int intVal = 0;
    }

    /**
     * Second instance of the polymorphic configuration schema.
     */
    @PolymorphicConfigInstance(PolymorphicTestConfigurationSchema.SECOND)
    public static class SecondPolymorphicInstanceTestConfigurationSchema extends PolymorphicTestConfigurationSchema {
        /** Integer value. */
        @Value(hasDefault = true)
        public int intVal = 0;

        /** Long value. */
        @Value(hasDefault = true)
        public long longVal = 0;
    }

    /**
     * Instance of the polymorphic configuration schema that has a field without a default value.
     */
    @PolymorphicConfigInstance(PolymorphicTestConfigurationSchema.NON_DEFAULT)
    public static class NonDefaultPolymorphicInstanceTestConfigurationSchema extends PolymorphicTestConfigurationSchema {
        @Value
        public String nonDefaultValue;
    }

    /**
     * Polymorphic named configuration scheme.
     */
    @PolymorphicConfig
    public static class PolymorphicNamedTestConfigurationSchema {
        /** Polymorphic type id field. */
        @PolymorphicId(hasDefault = true)
        public String typeId;

        /** String value. */
        @Value(hasDefault = true)
        public String strVal = "strVal";
    }

    /**
     * First instance of the named polymorphic configuration schema.
     */
    @PolymorphicConfigInstance("first")
    public static class FirstPolymorphicNamedInstanceTestConfigurationSchema extends PolymorphicNamedTestConfigurationSchema {
        /** Integer value. */
        @Value(hasDefault = true)
        public int intVal = 0;
    }

    /**
     * Second instance of the named polymorphic configuration schema.
     */
    @PolymorphicConfigInstance("second")
    public static class SecondPolymorphicNamedInstanceTestConfigurationSchema extends PolymorphicNamedTestConfigurationSchema {
        /** Integer value. */
        @Value(hasDefault = true)
        public int intVal = 0;

        /** Long value. */
        @Value(hasDefault = true)
        public long longVal = 0;
    }

    /**
     * Root configuration schema for test {@link InjectedName} and {@link Name}.
     */
    @ConfigurationRoot(rootName = "testInjectedName")
    public static class InjectedNameRootConfigurationSchema {
        @Name("nestedDefault")
        @ConfigValue
        public InjectedNameConfigurationSchema nested;

        @NamedConfigValue
        public InjectedNameConfigurationSchema nestedNamed;

        @Name("nestedDefaultPoly")
        @ConfigValue
        public PolyInjectedNameConfigurationSchema nestedPoly;

        @NamedConfigValue
        public PolyInjectedNameConfigurationSchema nestedNamedPoly;
    }

    /**
     * Configuration schema for test {@link InjectedName}.
     */
    @Config
    public static class InjectedNameConfigurationSchema {
        @InjectedName
        public String name;
    }

    /**
     * Polymorphic configuration schema for test {@link InjectedName}.
     */
    @PolymorphicConfig
    public static class PolyInjectedNameConfigurationSchema {
        public static final String FIRST = "first";

        public static final String SECOND = "second";

        @PolymorphicId(hasDefault = true)
        public String type = FIRST;

        @InjectedName
        public String name;
    }

    /**
     * First polymorphic instance of configuration schema for test {@link InjectedName}.
     */
    @PolymorphicConfigInstance(PolyInjectedNameConfigurationSchema.FIRST)
    public static class PolyInst0InjectedNameConfigurationSchema extends PolyInjectedNameConfigurationSchema {
    }

    /**
     * Second polymorphic instance of configuration schema for test {@link InjectedName}.
     */
    @PolymorphicConfigInstance(PolyInjectedNameConfigurationSchema.SECOND)
    public static class PolyInst1InjectedNameConfigurationSchema extends PolyInjectedNameConfigurationSchema {
    }

    /**
     * Simple abstract schema configuration for root configurations.
     */
    @AbstractConfiguration
    public static class AbstractRootConfigurationSchema {
        @Value(hasDefault = true)
        public String strVal = "strVal";
    }

    /**
     * Simple abstract schema configuration for configurations.
     */
    @AbstractConfiguration
    public static class AbstractConfigurationSchema {
        @InjectedName
        public String name;

        @InternalId
        public UUID id;

        @Value(hasDefault = true)
        public int intVal = 100500;
    }

    /**
     * Simple root configuration schema that extends {@link AbstractRootConfigurationSchema}.
     */
    @ConfigurationRoot(rootName = "rootFromAbstract")
    public static class RootFromAbstractConfigurationSchema extends AbstractRootConfigurationSchema {
        @Value(hasDefault = true)
        public long longVal = 500100;

        @Name("test")
        @ConfigValue
        public ConfigFromAbstractConfigurationSchema configFromAbstract;
    }

    /**
     * Simple configuration schema that extends {@link AbstractRootConfigurationSchema}.
     */
    @Config
    public static class ConfigFromAbstractConfigurationSchema extends AbstractConfigurationSchema {
        @Value(hasDefault = true)
        public boolean booleanVal = true;
    }
}
