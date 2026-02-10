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

package org.apache.ignite.internal.configuration.util;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyNavigableMap;
import static java.util.Collections.singletonMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.tree.NamedListNode.IDS;
import static org.apache.ignite.internal.configuration.tree.NamedListNode.NAME;
import static org.apache.ignite.internal.configuration.tree.NamedListNode.ORDER_IDX;
import static org.apache.ignite.internal.configuration.util.ConfigurationFlattener.createFlattenedUpdatesMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.EMPTY_CFG_SRC;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.addDefaults;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.checkConfigurationType;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.collectSchemas;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.compressDeletedEntries;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.extensionsFields;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.find;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.mapIterable;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.polymorphicSchemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.removeLastKey;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.schemaExtensions;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.RootInnerNode;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConverterToMapVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link ConfigurationUtil}.
 */
public class ConfigurationUtilTest {
    private static ConfigurationAsmGenerator cgen;

    /**
     * Before all.
     */
    @BeforeAll
    public static void beforeAll() {
        cgen = new ConfigurationAsmGenerator();

        cgen.compileRootSchema(ParentConfigurationSchema.class, Map.of(), Map.of());

        cgen.compileRootSchema(
                PolymorphicRootConfigurationSchema.class,
                Map.of(),
                Map.of(
                        PolymorphicConfigurationSchema.class,
                        Set.of(FirstPolymorphicInstanceConfigurationSchema.class, SecondPolymorphicInstanceConfigurationSchema.class)
                )
        );
    }

    /**
     * After all.
     */
    @AfterAll
    public static void afterAll() {
        cgen = null;
    }

    /**
     * Creates new instance of {@code *Node} class corresponding to the given configuration Schema.
     *
     * @param schemaClass Configuration schema class.
     * @param <P>         Type of {@link InnerNode}.
     * @return New instance of {@link InnerNode}.
     */
    public static <P extends InnerNode> P newNodeInstance(Class<?> schemaClass) {
        return (P) cgen.instantiateNode(schemaClass);
    }

    @Test
    public void escape() {
        assertEquals("foo", ConfigurationUtil.escape("foo"));

        assertEquals("foo\\.bar", ConfigurationUtil.escape("foo.bar"));

        assertEquals("foo\\\\bar", ConfigurationUtil.escape("foo\\bar"));

        assertEquals("\\\\a\\.b\\\\c\\.", ConfigurationUtil.escape("\\a.b\\c."));
    }

    @Test
    public void unescape() {
        assertEquals("foo", ConfigurationUtil.unescape("foo"));

        assertEquals("foo.bar", ConfigurationUtil.unescape("foo\\.bar"));

        assertEquals("foo\\bar", ConfigurationUtil.unescape("foo\\\\bar"));

        assertEquals("\\a.b\\c.", ConfigurationUtil.unescape("\\\\a\\.b\\\\c\\."));
    }

    @Test
    public void split() {
        assertEquals(List.of("a", "b.b", "c\\c", ""), ConfigurationUtil.split("a.b\\.b.c\\\\c."));
    }

    @Test
    public void join() {
        assertEquals("a.b\\.b.c\\\\c", ConfigurationUtil.join(List.of("a", "b.b", "c\\c")));
    }

    /**
     * Parent root configuration schema.
     */
    @ConfigurationRoot(rootName = "root", type = LOCAL)
    public static class ParentConfigurationSchema {
        @NamedConfigValue
        public NamedElementConfigurationSchema elements;
    }

    /**
     * Child named configuration schema.
     */
    @Config
    public static class NamedElementConfigurationSchema {
        @ConfigValue
        public ChildConfigurationSchema child;
    }

    /**
     * Child configuration schema.
     */
    @Config
    public static class ChildConfigurationSchema {
        @Value
        public String str;
    }

    /**
     * Tests that {@link ConfigurationUtil#find(List, TraversableTreeNode, boolean)} finds proper node when provided with correct path.
     */
    @Test
    public void findSuccessfully() {
        InnerNode parentNode = newNodeInstance(ParentConfigurationSchema.class);

        ParentChange parentChange = (ParentChange) parentNode;

        parentChange.changeElements(elements ->
                elements.createOrUpdate("name", element ->
                        element.changeChild(child ->
                                child.changeStr("value")
                        )
                )
        );

        NodeValue<Object> root = find(List.of(), parentNode, true);
        assertNull(root.field());
        assertSame(parentNode, root.value());

        NodeValue<Object> elements = find(List.of("elements"), parentNode, true);
        assertEquals("elements", elements.field().getName());
        assertSame(NamedElementConfigurationSchema.class, elements.field().getType());
        assertSame(parentChange.elements(), elements.value());

        NodeValue<Object> elementsName = find(List.of("elements", "name"), parentNode, true);
        assertEquals("elements", elementsName.field().getName());
        assertSame(NamedElementConfigurationSchema.class, elementsName.field().getType());
        assertSame(parentChange.elements().get("name"), elementsName.value());

        NodeValue<Object> elementsNameChild = find(List.of("elements", "name", "child"), parentNode, true);
        assertEquals("child", elementsNameChild.field().getName());
        assertSame(ChildConfigurationSchema.class, elementsNameChild.field().getType());
        assertSame(parentChange.elements().get("name").child(), elementsNameChild.value());

        NodeValue<Object> elementsNameChildStr = find(List.of("elements", "name", "child", "str"), parentNode, true);
        assertEquals("str", elementsNameChildStr.field().getName());
        assertSame(String.class, elementsNameChildStr.field().getType());
        assertSame(parentChange.elements().get("name").child().str(), elementsNameChildStr.value());
    }

    @Test
    public void findSuccessfullyPolymorphicConfig() {
        InnerNode parentNode = newNodeInstance(PolymorphicRootConfigurationSchema.class);
        addDefaults(parentNode);

        PolymorphicRootChange parentChange = (PolymorphicRootChange) parentNode;

        parentChange.changePolymorphicSubCfg(sub -> {
            sub.convert(FirstPolymorphicInstanceChange.class)
                    .changeStrVal("str")
                    .changeLongVal(0);
        });

        parentChange.changePolymorphicNamedCfg(named -> {
            named.createOrUpdate("name0", sub -> {
                sub.convert(FirstPolymorphicInstanceChange.class)
                        .changeStrVal("substr0")
                        .changeLongVal(-1);
            });

            named.createOrUpdate("name1", sub -> {
                sub.convert(SecondPolymorphicInstanceChange.class)
                        .changeIntVal(1)
                        .changeLongVal(1);
            });
        });

        NodeValue<Object> subCfgStrVal = find(List.of("polymorphicSubCfg", "strVal"), parentNode, true);
        assertEquals("strVal", subCfgStrVal.field().getName());
        assertSame(String.class, subCfgStrVal.field().getType());
        assertSame(
                ((FirstPolymorphicInstanceView) parentChange.changePolymorphicSubCfg()).strVal(),
                subCfgStrVal.value()
        );

        NodeValue<Object> subCfgLongVal = find(List.of("polymorphicSubCfg", "longVal"), parentNode, true);
        assertEquals("longVal", subCfgLongVal.field().getName());
        assertSame(long.class, subCfgLongVal.field().getGenericType());
        assertSame(
                parentChange.changePolymorphicSubCfg().longVal(),
                subCfgLongVal.value()
        );

        NodeValue<Object> name0strVal = find(List.of("polymorphicNamedCfg", "name0", "strVal"), parentNode, true);
        assertEquals("strVal", name0strVal.field().getName());
        assertSame(String.class, name0strVal.field().getGenericType());
        assertSame(
                ((FirstPolymorphicInstanceView) parentChange.changePolymorphicNamedCfg().get("name0")).strVal(),
                name0strVal.value()
        );

        NodeValue<Object> name0longVal = find(List.of("polymorphicNamedCfg", "name0", "longVal"), parentNode, true);
        assertEquals("longVal", name0longVal.field().getName());
        assertSame(long.class, name0longVal.field().getGenericType());
        assertSame(
                parentChange.changePolymorphicNamedCfg().get("name0").longVal(),
                name0longVal.value()
        );

        NodeValue<Object> name0someVal = find(List.of("polymorphicNamedCfg", "name0", "someVal"), parentNode, true);
        assertEquals("someVal", name0someVal.field().getName());
        assertSame(String.class, name0someVal.field().getGenericType());
        assertSame(
                ((FirstPolymorphicInstanceView) parentChange.changePolymorphicNamedCfg().get("name0")).someVal(),
                name0someVal.value()
        );

        NodeValue<Object> name1intVal = find(List.of("polymorphicNamedCfg", "name1", "intVal"), parentNode, true);
        assertEquals("intVal", name1intVal.field().getName());
        assertSame(int.class, name1intVal.field().getGenericType());
        assertSame(
                ((SecondPolymorphicInstanceView) parentChange.changePolymorphicNamedCfg().get("name1")).intVal(),
                name1intVal.value()
        );

        NodeValue<Object> name1longVal = find(List.of("polymorphicNamedCfg", "name1", "longVal"), parentNode, true);
        assertEquals("longVal", name1longVal.field().getName());
        assertSame(long.class, name1longVal.field().getGenericType());
        assertSame(
                parentChange.changePolymorphicNamedCfg().get("name1").longVal(),
                name1longVal.value()
        );

        NodeValue<Object> name1someVal = find(List.of("polymorphicNamedCfg", "name1", "someVal"), parentNode, true);
        assertEquals("someVal", name1someVal.field().getName());
        assertSame(int.class, name1someVal.field().getGenericType());
        assertSame(
                ((SecondPolymorphicInstanceView) parentChange.changePolymorphicNamedCfg().get("name1")).someVal(),
                name1someVal.value()
        );
    }

    /**
     * Tests that {@link ConfigurationUtil#find(List, TraversableTreeNode, boolean)} returns null when path points to nonexistent named list
     * element.
     */
    @Test
    public void findNulls() {
        InnerNode parentNode = newNodeInstance(ParentConfigurationSchema.class);

        ParentChange parentChange = (ParentChange) parentNode;

        assertNull(find(List.of("elements", "name"), parentNode, true).value());

        parentChange.changeElements(elements -> elements.createOrUpdate("name", element -> {
        }));

        assertNull(find(List.of("elements", "name", "child", "str"), parentNode, true).value());
    }

    /**
     * Tests that {@link ConfigurationUtil#find(List, TraversableTreeNode, boolean)} throws {@link KeyNotFoundException} when provided with
     * a wrong path.
     */
    @Test
    public void findUnsuccessfully() {
        InnerNode parentNode = newNodeInstance(ParentConfigurationSchema.class);

        ParentChange parentChange = (ParentChange) parentNode;

        assertThrows(
                KeyNotFoundException.class,
                () -> find(List.of("elements", "name", "child"), parentNode, true)
        );

        parentChange.changeElements(elements -> elements.createOrUpdate("name", element -> {
        }));

        assertThrows(
                KeyNotFoundException.class,
                () -> find(List.of("elements", "name", "child", "str0"), parentNode, true)
        );

        ((NamedElementChange) parentChange.elements().get("name")).changeChild(child -> child.changeStr("value"));

        assertThrows(
                KeyNotFoundException.class,
                () -> find(List.of("elements", "name", "child", "str", "foo"), parentNode, true)
        );
    }

    /**
     * Tests conversion of flat map to a prefix map.
     */
    @Test
    public void toPrefixMap() {
        assertEquals(
                Map.of("foo", 42),
                ConfigurationUtil.toPrefixMap(Map.of("foo", 42))
        );

        assertEquals(
                Map.of("foo.bar", 42),
                ConfigurationUtil.toPrefixMap(Map.of("foo\\.bar", 42))
        );

        assertEquals(
                Map.of("foo", Map.of("bar1", 10, "bar2", 20)),
                ConfigurationUtil.toPrefixMap(Map.of("foo.bar1", 10, "foo.bar2", 20))
        );

        assertEquals(
                Map.of("root1", Map.of("leaf1", 10), "root2", Map.of("leaf2", 20)),
                ConfigurationUtil.toPrefixMap(Map.of("root1.leaf1", 10, "root2.leaf2", 20))
        );
    }

    /**
     * Tests that patching of configuration node with a prefix map works fine when prefix map is valid.
     */
    @Test
    public void fillFromPrefixMapSuccessfully() {
        InnerNode parentNode = newNodeInstance(ParentConfigurationSchema.class);

        ParentChange parentChange = (ParentChange) parentNode;

        ConfigurationUtil.fillFromPrefixMap(parentNode, Map.of(
                "elements", Map.of(
                        "01234567-89ab-cdef-0123-456789abcdef", Map.of(
                                "child", Map.of("str", "value2"),
                                ORDER_IDX, 1,
                                NAME, "name2"
                        ),
                        "12345678-9abc-def0-1234-56789abcdef0", Map.of(
                                "child", Map.of("str", "value1"),
                                ORDER_IDX, 0,
                                NAME, "name1"
                        )
                )
        ));

        assertEquals("value1", parentChange.elements().get("name1").child().str());
        assertEquals("value2", parentChange.elements().get("name2").child().str());
    }

    @Test
    public void fillFromPrefixMapWithNotFullNamedListSuccessfully() {
        InnerNode parentNode = newNodeInstance(ParentConfigurationSchema.class);

        ParentChange parentChange = (ParentChange) parentNode;

        // Entry with order index 0 is filtered
        ConfigurationUtil.fillFromPrefixMap(parentNode, Map.of(
                "elements", Map.of(
                        "01234567-89ab-cdef-0123-456789abcdef", Map.of(
                                "child", Map.of("str", "value2"),
                                ORDER_IDX, 1,
                                NAME, "name2"
                        ),
                        IDS, Map.of("name2", "01234567-89ab-cdef-0123-456789abcdef")
                )
        ));

        List<String> listKeys = parentChange.elements().namedListKeys();
        assertEquals(1, listKeys.size());

        String key = listKeys.get(0);
        assertEquals("name2", key);

        assertEquals("value2", parentChange.elements().get("name2").child().str());
    }

    /**
     * Tests that patching of configuration node with a prefix map works fine when prefix map is valid.
     */
    @Test
    public void fillFromPrefixMapSuccessfullyWithRemove() {
        InnerNode parentNode = newNodeInstance(ParentConfigurationSchema.class);

        ParentChange parentChange = (ParentChange) parentNode;

        parentChange.changeElements(elements ->
                elements.createOrUpdate("name", element ->
                        element.changeChild(child -> {
                        })
                )
        );

        UUID internalId = ((InnerNode) ((ParentView) parentNode).elements().get("name")).internalId();

        ConfigurationUtil.fillFromPrefixMap(parentNode, Map.of(
                "elements", singletonMap(internalId.toString(), null)
        ));

        assertNull(parentChange.elements().get("node"));
    }

    /**
     * Tests that conversion from "changer" lambda to a flat map of updates for the storage works properly.
     */
    @Test
    public void flattenedUpdatesMap() {
        var superRoot = new SuperRoot(
                key -> null,
                Map.of(ParentConfiguration.KEY, newNodeInstance(ParentConfigurationSchema.class))
        );

        assertThat(flattenedMap(superRoot, ParentConfiguration.KEY, emptyNavigableMap(), node -> {}), is(anEmptyMap()));

        Map<String, Serializable> flattenedMap =
                flattenedMap(superRoot, ParentConfiguration.KEY, emptyNavigableMap(), node -> ((ParentChange) node)
                        .changeElements(elements -> elements
                                .create("name", element -> element
                                        .changeChild(child -> child.changeStr("foo"))
                                )
                        )
                );

        assertThat(
                flattenedMap,
                is(allOf(
                        aMapWithSize(4),
                        hasEntry(matchesPattern("root[.]elements[.]<ids>[.]name"), hasToString(matchesPattern("[-\\w]{36}"))),
                        hasEntry(matchesPattern("root[.]elements[.][-\\w]{36}[.]child[.]str"), hasToString("foo")),
                        hasEntry(matchesPattern("root[.]elements[.][-\\w]{36}[.]<order>"), is(0)),
                        hasEntry(matchesPattern("root[.]elements[.][-\\w]{36}[.]<name>"), hasToString("name"))
                ))
        );

        assertThat(
                flattenedMap(superRoot, ParentConfiguration.KEY, new TreeMap<>(flattenedMap), node -> ((ParentChange) node)
                        .changeElements(elements1 -> elements1.delete("void"))
                ),
                is(anEmptyMap())
        );

        assertThat(
                flattenedMap(superRoot, ParentConfiguration.KEY, new TreeMap<>(flattenedMap), node -> ((ParentChange) node)
                        .changeElements(elements -> elements.delete("name"))
                ),
                is(allOf(
                        aMapWithSize(4),
                        hasEntry(matchesPattern("root[.]elements[.]<ids>[.]name"), nullValue()),
                        hasEntry(matchesPattern("root[.]elements[.][-\\w]{36}[.]child[.]str"), nullValue()),
                        hasEntry(matchesPattern("root[.]elements[.][-\\w]{36}[.]<order>"), nullValue()),
                        hasEntry(matchesPattern("root[.]elements[.][-\\w]{36}[.]<name>"), nullValue())
                ))
        );
    }

    @Test
    void testCheckConfigurationTypeMixedTypes() {
        List<RootKey<?, ?, ?>> rootKeys = List.of(LocalFirstConfiguration.KEY, DistributedFirstConfiguration.KEY);

        assertThrows(
                IllegalArgumentException.class,
                () -> checkConfigurationType(rootKeys, new TestConfigurationStorage(LOCAL))
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> checkConfigurationType(rootKeys, new TestConfigurationStorage(DISTRIBUTED))
        );
    }

    @Test
    void testCheckConfigurationTypeOppositeTypes() {
        assertThrows(
                IllegalArgumentException.class,
                () -> checkConfigurationType(
                        List.of(DistributedFirstConfiguration.KEY, DistributedSecondConfiguration.KEY),
                        new TestConfigurationStorage(LOCAL)
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> checkConfigurationType(
                        List.of(LocalFirstConfiguration.KEY, LocalSecondConfiguration.KEY),
                        new TestConfigurationStorage(DISTRIBUTED)
                )
        );
    }

    @Test
    void testCheckConfigurationTypeNoError() {
        checkConfigurationType(
                List.of(LocalFirstConfiguration.KEY, LocalSecondConfiguration.KEY),
                new TestConfigurationStorage(LOCAL)
        );

        checkConfigurationType(
                List.of(DistributedFirstConfiguration.KEY, DistributedSecondConfiguration.KEY),
                new TestConfigurationStorage(DISTRIBUTED)
        );
    }

    @Test
    void testExtensionsType() {
        // Public extensions.
        assertTrue(ConfigurationUtil.isPublicExtension(PublicConfigurationSchema.class));

        assertTrue(ConfigurationUtil.isPublicExtension(PublicRootConfigurationSchema.class));

        // Internal extensions.
        assertTrue(ConfigurationUtil.isInternalExtension(InternalSecondConfigurationSchema.class));

        assertTrue(ConfigurationUtil.isInternalExtension(InternalSecondRootConfigurationSchema.class));

        // This one is not an extension at all.
        assertFalse(ConfigurationUtil.isPublicExtension(InternalConfigurationSchema.class));

        assertFalse(ConfigurationUtil.isInternalExtension(InternalConfigurationSchema.class));
    }

    @Test
    void testInternalSchemaExtensions() {
        assertTrue(schemaExtensions(List.of()).isEmpty());

        assertThrows(IllegalArgumentException.class, () -> schemaExtensions(List.of(Object.class)));

        assertThrows(
                IllegalArgumentException.class,
                () -> schemaExtensions(List.of(InternalRootConfigurationSchema.class))
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> schemaExtensions(List.of(InternalConfigurationSchema.class))
        );

        Map<Class<?>, Set<Class<?>>> extensions = schemaExtensions(List.of(
                InternalFirstRootConfigurationSchema.class,
                InternalSecondRootConfigurationSchema.class,
                InternalFirstConfigurationSchema.class,
                InternalSecondConfigurationSchema.class
        ));

        assertEquals(2, extensions.size());

        assertEquals(
                Set.of(
                        InternalFirstRootConfigurationSchema.class,
                        InternalSecondRootConfigurationSchema.class
                ),
                extensions.get(InternalRootConfigurationSchema.class)
        );

        assertEquals(
                Set.of(
                        InternalFirstConfigurationSchema.class,
                        InternalSecondConfigurationSchema.class
                ),
                extensions.get(InternalConfigurationSchema.class)
        );
    }

    @Test
    void testPublicSchemaExtensions() {
        Map<Class<?>, Set<Class<?>>> extensions = schemaExtensions(List.of(
                PublicRootConfigurationSchema.class,
                PublicConfigurationSchema.class
        ));

        assertEquals(2, extensions.size());

        assertEquals(
                Set.of(
                        PublicRootConfigurationSchema.class
                ),
                extensions.get(InternalRootConfigurationSchema.class)
        );

        assertEquals(
                Set.of(
                        PublicConfigurationSchema.class
                ),
                extensions.get(InternalConfigurationSchema.class)
        );
    }

    @Test
    void testPublicAndInternalSchemaExtensions() {
        Map<Class<?>, Set<Class<?>>> extensions = schemaExtensions(List.of(
                PublicRootConfigurationSchema.class,
                PublicConfigurationSchema.class,
                InternalFirstRootConfigurationSchema.class,
                InternalFirstConfigurationSchema.class
        ));

        assertEquals(2, extensions.size());

        assertEquals(
                Set.of(
                        InternalFirstRootConfigurationSchema.class,
                        PublicRootConfigurationSchema.class
                ),
                extensions.get(InternalRootConfigurationSchema.class)
        );

        assertEquals(
                Set.of(
                        InternalFirstConfigurationSchema.class,
                        PublicConfigurationSchema.class
                ),
                extensions.get(InternalConfigurationSchema.class)
        );
    }

    @Test
    void testSchemaFields() {
        assertTrue(extensionsFields(List.of(), true).isEmpty());
        assertTrue(extensionsFields(List.of(), false).isEmpty());

        List<Class<?>> extensions0 = List.of(
                InternalExtendedRootConfigurationSchema.class,
                ErrorInternalExtendedRootConfigurationSchema.class
        );

        assertThrows(IllegalArgumentException.class, () -> extensionsFields(extensions0, true));

        assertEquals(
                extensions0.stream().flatMap(cls -> Arrays.stream(cls.getDeclaredFields())).collect(toList()),
                List.copyOf(extensionsFields(extensions0, false))
        );

        List<Class<?>> extensions1 = List.of(
                InternalFirstRootConfigurationSchema.class,
                InternalSecondRootConfigurationSchema.class
        );

        assertEquals(
                extensions1.stream().flatMap(cls -> Arrays.stream(cls.getDeclaredFields())).collect(toList()),
                List.copyOf(extensionsFields(extensions1, true))
        );

        assertEquals(
                extensions1.stream().flatMap(cls -> Arrays.stream(cls.getDeclaredFields())).collect(toList()),
                List.copyOf(extensionsFields(extensions1, false))
        );
    }

    @Test
    void testFindInternalConfigs() {
        Map<Class<?>, Set<Class<?>>> extensions = schemaExtensions(List.of(
                InternalFirstRootConfigurationSchema.class,
                InternalSecondRootConfigurationSchema.class,
                InternalFirstConfigurationSchema.class,
                InternalSecondConfigurationSchema.class
        ));

        ConfigurationAsmGenerator generator = new ConfigurationAsmGenerator();
        generator.compileRootSchema(InternalRootConfigurationSchema.class, extensions, Map.of());

        InnerNode innerNode = generator.instantiateNode(InternalRootConfigurationSchema.class);

        addDefaults(innerNode);

        // Check that no internal configuration will be found.

        assertThrows(KeyNotFoundException.class, () -> find(List.of("str2"), innerNode, false));
        assertThrows(KeyNotFoundException.class, () -> find(List.of("str3"), innerNode, false));
        assertThrows(KeyNotFoundException.class, () -> find(List.of("subCfg1"), innerNode, false));

        assertThrows(KeyNotFoundException.class, () -> find(List.of("subCfg", "str01"), innerNode, false));
        assertThrows(KeyNotFoundException.class, () -> find(List.of("subCfg", "str02"), innerNode, false));

        // Check that internal configuration will be found.

        assertNull(find(List.of("str2"), innerNode, true).value());
        assertEquals("foo", find(List.of("str3"), innerNode, true).value());
        assertNotNull(find(List.of("subCfg1"), innerNode, true).value());

        assertEquals("foo", find(List.of("subCfg", "str01"), innerNode, true).value());
        assertEquals("foo", find(List.of("subCfg", "str02"), innerNode, true).value());
    }

    @Test
    void testFindPublicConfigs() {
        Map<Class<?>, Set<Class<?>>> extensions = schemaExtensions(List.of(
                InternalFirstRootConfigurationSchema.class,
                InternalSecondRootConfigurationSchema.class,
                PublicRootConfigurationSchema.class,
                InternalFirstConfigurationSchema.class,
                InternalSecondConfigurationSchema.class,
                PublicConfigurationSchema.class
        ));

        ConfigurationAsmGenerator generator = new ConfigurationAsmGenerator();
        generator.compileRootSchema(InternalRootConfigurationSchema.class, extensions, Map.of());

        InnerNode innerNode = generator.instantiateNode(InternalRootConfigurationSchema.class);

        addDefaults(innerNode);

        assertNotNull(find(List.of("publicSubCfg1"), innerNode, false).value());

        assertEquals("bar", find(List.of("subCfg", "publicStr03"), innerNode, false).value());
    }

    @Test
    void testGetInternalConfigs() {
        Map<Class<?>, Set<Class<?>>> extensions = schemaExtensions(List.of(
                InternalFirstRootConfigurationSchema.class,
                InternalSecondRootConfigurationSchema.class,
                InternalFirstConfigurationSchema.class,
                InternalSecondConfigurationSchema.class
        ));

        ConfigurationAsmGenerator generator = new ConfigurationAsmGenerator();
        generator.compileRootSchema(InternalRootConfigurationSchema.class, extensions, Map.of());

        InnerNode innerNode = generator.instantiateNode(InternalRootConfigurationSchema.class);

        addDefaults(innerNode);

        Map<String, Object> config = (Map<String, Object>) innerNode.accept(
                null,
                null,
                ConverterToMapVisitor.builder()
                        .includeInternal(false)
                        .build()
        );

        // Check that no internal configuration will be received.

        assertEquals(4, config.size());
        assertNull(config.get("str0"));
        assertEquals("foo", config.get("str1"));
        assertNotNull(config.get("subCfg"));
        assertNotNull(config.get("namedCfg"));

        Map<String, Object> subConfig = (Map<String, Object>) config.get("subCfg");

        assertEquals(1, subConfig.size());
        assertEquals("foo", subConfig.get("str00"));

        // Check that no internal configuration will be received.

        config = (Map<String, Object>) innerNode.accept(
                null,
                null,
                ConverterToMapVisitor.builder()
                        .includeInternal(true)
                        .build()
        );

        assertEquals(7, config.size());
        assertNull(config.get("str0"));
        assertNull(config.get("str2"));
        assertEquals("foo", config.get("str1"));
        assertEquals("foo", config.get("str3"));
        assertNotNull(config.get("subCfg"));
        assertNotNull(config.get("subCfg1"));
        assertNotNull(config.get("namedCfg"));

        subConfig = (Map<String, Object>) config.get("subCfg");

        assertEquals(3, subConfig.size());
        assertEquals("foo", subConfig.get("str00"));
        assertEquals("foo", subConfig.get("str01"));
        assertEquals("foo", subConfig.get("str02"));

        subConfig = (Map<String, Object>) config.get("subCfg1");

        assertEquals(3, subConfig.size());
        assertEquals("foo", subConfig.get("str00"));
        assertEquals("foo", subConfig.get("str01"));
        assertEquals("foo", subConfig.get("str02"));
    }

    @Test
    void testGetPublicWithInternalConfigs() {
        Map<Class<?>, Set<Class<?>>> extensions = schemaExtensions(List.of(
                InternalFirstRootConfigurationSchema.class,
                InternalSecondRootConfigurationSchema.class,
                PublicRootConfigurationSchema.class,
                InternalFirstConfigurationSchema.class,
                InternalSecondConfigurationSchema.class,
                PublicConfigurationSchema.class
        ));

        ConfigurationAsmGenerator generator = new ConfigurationAsmGenerator();
        generator.compileRootSchema(InternalRootConfigurationSchema.class, extensions, Map.of());

        InnerNode innerNode = generator.instantiateNode(InternalRootConfigurationSchema.class);

        addDefaults(innerNode);

        Map<String, Object> config = (Map<String, Object>) innerNode.accept(
                null,
                null,
                ConverterToMapVisitor.builder()
                        .includeInternal(false)
                        .build()
        );

        // Check that public configuration will still be received.
        assertEquals(5, config.size());
        assertNull(config.get("str0"));
        assertEquals("foo", config.get("str1"));
        assertNotNull(config.get("subCfg"));
        assertNotNull(config.get("namedCfg"));
        assertNotNull(config.get("publicSubCfg1"));

        Map<String, Object> subConfig = (Map<String, Object>) config.get("subCfg");

        assertEquals(2, subConfig.size());
        assertEquals("foo", subConfig.get("str00"));
        assertEquals("bar", subConfig.get("publicStr03"));

        // Check that public configuration will be received along with the internal one.
        config = (Map<String, Object>) innerNode.accept(
                null,
                null,
                ConverterToMapVisitor.builder()
                        .includeInternal(true)
                        .build()
        );

        assertEquals(8, config.size());
        assertNull(config.get("str0"));
        assertNull(config.get("str2"));
        assertEquals("foo", config.get("str1"));
        assertEquals("foo", config.get("str3"));
        assertNotNull(config.get("subCfg"));
        assertNotNull(config.get("subCfg1"));
        assertNotNull(config.get("namedCfg"));
        assertNotNull(config.get("publicSubCfg1"));

        subConfig = (Map<String, Object>) config.get("subCfg");

        assertEquals(4, subConfig.size());
        assertEquals("foo", subConfig.get("str00"));
        assertEquals("foo", subConfig.get("str01"));
        assertEquals("foo", subConfig.get("str02"));
        assertEquals("bar", subConfig.get("publicStr03"));

        subConfig = (Map<String, Object>) config.get("subCfg1");

        assertEquals(4, subConfig.size());
        assertEquals("foo", subConfig.get("str00"));
        assertEquals("foo", subConfig.get("str01"));
        assertEquals("foo", subConfig.get("str02"));
        assertEquals("bar", subConfig.get("publicStr03"));
    }

    @Test
    void testSuperRootWithInternalConfig() {
        ConfigurationAsmGenerator generator = new ConfigurationAsmGenerator();

        Class<?> schemaClass = InternalWithoutSuperclassConfigurationSchema.class;
        RootKey<?, ?, ?> schemaKey = InternalWithoutSuperclassConfiguration.KEY;

        generator.compileRootSchema(schemaClass, Map.of(), Map.of());

        SuperRoot superRoot = new SuperRoot(s ->
                s.equals(schemaKey.key())
                        ? new RootInnerNode(schemaKey, generator.instantiateNode(schemaClass))
                        : null
        );

        assertThrows(NoSuchElementException.class, () -> superRoot.construct(schemaKey.key(), null, false));

        superRoot.construct(schemaKey.key(), null, true);

        superRoot.addRoot(schemaKey, generator.instantiateNode(schemaClass));

        assertThrows(KeyNotFoundException.class, () -> find(List.of(schemaKey.key()), superRoot, false));

        assertNotNull(find(List.of(schemaKey.key()), superRoot, true));

        Map<String, Object> config = (Map<String, Object>) superRoot.accept(
                        null,
                        schemaKey.key(),
                        ConverterToMapVisitor.builder()
                                .includeInternal(false)
                                .build()
                );

        assertTrue(config.isEmpty());

        config = (Map<String, Object>) superRoot.accept(
                null,
                schemaKey.key(),
                ConverterToMapVisitor.builder()
                        .includeInternal(true)
                        .build()
        );

        assertEquals(1, config.size());
        assertNotNull(config.get(schemaKey.key()));
    }

    @Test
    void testCollectSchemas() {
        assertTrue(collectSchemas(List.of()).isEmpty());

        assertThrows(IllegalArgumentException.class, () -> collectSchemas(List.of(Object.class)));

        Set<Class<?>> schemas = Set.of(
                LocalFirstConfigurationSchema.class,
                InternalConfigurationSchema.class,
                PolymorphicConfigurationSchema.class
        );

        assertEquals(schemas, collectSchemas(schemas));

        assertEquals(
                Set.of(
                        InternalRootConfigurationSchema.class,
                        PolymorphicRootConfigurationSchema.class,
                        InternalConfigurationSchema.class,
                        PolymorphicConfigurationSchema.class
                ),
                collectSchemas(List.of(InternalRootConfigurationSchema.class, PolymorphicRootConfigurationSchema.class))
        );
    }

    @Test
    void testPolymorphicSchemaExtensions() {
        assertTrue(polymorphicSchemaExtensions(List.of()).isEmpty());

        assertThrows(IllegalArgumentException.class, () -> polymorphicSchemaExtensions(List.of(Object.class)));

        assertThrows(
                IllegalArgumentException.class,
                () -> polymorphicSchemaExtensions(List.of(LocalFirstConfigurationSchema.class))
        );

        Set<Class<?>> extensions = Set.of(
                FirstPolymorphicInstanceConfigurationSchema.class,
                SecondPolymorphicInstanceConfigurationSchema.class
        );

        assertEquals(
                Map.of(PolymorphicConfigurationSchema.class, extensions),
                polymorphicSchemaExtensions(extensions)
        );
    }

    @Test
    void testCompressDeletedEntries() {
        Map<String, String> containsNullLeaf = new HashMap<>();

        containsNullLeaf.put("first", "1");
        containsNullLeaf.put("second", null);

        Map<String, String> deletedNamedListElement = new HashMap<>();

        deletedNamedListElement.put("third", null);
        deletedNamedListElement.put(NAME, null);

        Map<String, Object> regular = new HashMap<>();

        regular.put("strVal", "foo");
        regular.put("intVal", 10);

        Map<String, Object> prefixMap = new HashMap<>();

        prefixMap.put("0", containsNullLeaf);
        prefixMap.put("1", deletedNamedListElement);
        prefixMap.put("2", regular);

        Map<String, Object> exp = new HashMap<>();

        Map<String, Object> expMapZero = new HashMap<>();
        expMapZero.put("first", "1");
        expMapZero.put("second", null);

        exp.put("0", expMapZero);
        exp.put("1", null);
        exp.put("2", Map.of("strVal", "foo", "intVal", 10));

        compressDeletedEntries(prefixMap);

        assertEquals(exp, prefixMap);
    }

    @Test
    void testFlattenedMapPolymorphicConfig() {
        InnerNode polymorphicRootInnerNode = newNodeInstance(PolymorphicRootConfigurationSchema.class);

        addDefaults(polymorphicRootInnerNode);

        RootKey<?, ?, ?> rootKey = PolymorphicRootConfiguration.KEY;

        SuperRoot superRoot = new SuperRoot(key -> null, Map.of(rootKey, polymorphicRootInnerNode));

        final Map<String, Serializable> act = flattenedMap(
                superRoot,
                rootKey,
                emptyNavigableMap(),
                node -> ((PolymorphicRootChange) node).changePolymorphicSubCfg(c -> c.convert(SecondPolymorphicInstanceChange.class))
        );

        Map<String, Serializable> exp = new HashMap<>();

        exp.put("rootPolymorphic.polymorphicSubCfg.typeId", "second");
        exp.put("rootPolymorphic.polymorphicSubCfg.longVal", 0L);

        exp.put("rootPolymorphic.polymorphicSubCfg.strVal", null);
        exp.put("rootPolymorphic.polymorphicSubCfg.intVal", 0);

        exp.put("rootPolymorphic.polymorphicSubCfg.someVal", 10);

        assertEquals(exp, act);
    }

    @Test
    void testFlattenedMapPolymorphicNamedConfig() {
        InnerNode polymorphicRootInnerNode = newNodeInstance(PolymorphicRootConfigurationSchema.class);

        PolymorphicRootChange polymorphicRootChange = ((PolymorphicRootChange) polymorphicRootInnerNode);

        polymorphicRootChange.changePolymorphicNamedCfg(c -> c.create("0", c1 -> {
        }));

        addDefaults(polymorphicRootInnerNode);

        RootKey<?, ?, ?> rootKey = PolymorphicRootConfiguration.KEY;

        SuperRoot superRoot = new SuperRoot(key -> null, Map.of(rootKey, polymorphicRootInnerNode));

        var storageData = new TreeMap<>(flattenedMap(superRoot, rootKey, emptyNavigableMap(), node -> {}));

        Map<String, Serializable> act = flattenedMap(
                superRoot,
                rootKey,
                storageData,
                node -> ((PolymorphicRootChange) node).changePolymorphicNamedCfg(c ->
                        c.createOrUpdate("0", c1 -> c1.convert(SecondPolymorphicInstanceChange.class)))
        );

        NamedListNode<?> polymorphicNamedCfgListNode = (NamedListNode<?>) polymorphicRootChange.polymorphicNamedCfg();
        UUID internalId = polymorphicNamedCfgListNode.internalId("0");

        Map<String, Serializable> exp = new HashMap<>();

        exp.put("rootPolymorphic.polymorphicNamedCfg." + internalId + ".typeId", "second");
        exp.put("rootPolymorphic.polymorphicNamedCfg." + internalId + ".longVal", 0L);

        exp.put("rootPolymorphic.polymorphicNamedCfg." + internalId + ".strVal", null);
        exp.put("rootPolymorphic.polymorphicNamedCfg." + internalId + ".intVal", 0);

        exp.put("rootPolymorphic.polymorphicNamedCfg." + internalId + ".someVal", 10);

        assertEquals(exp, act);
    }

    @Test
    void testRemoveLastKey() {
        assertEquals(List.of(), removeLastKey(List.of()));
        assertEquals(List.of(), removeLastKey(List.of("0")));
        assertEquals(List.of("0"), removeLastKey(List.of("0", "1")));
    }

    /**
     * Tests that {@link ConfigurationUtil#addDefaults} copies the tree when adding default values.
     */
    @Test
    void testAddDefaultsPurity() {
        InnerNode parentNode = newNodeInstance(ParentConfigurationSchema.class);

        ParentChange parentChange = (ParentChange) parentNode;

        parentChange.changeElements(elements -> elements.create("name", element -> {}));

        NamedListView<?> beforeDefaults = parentChange.elements();

        addDefaults(parentNode);

        NamedListView<?> afterDefaults = parentChange.elements();

        assertNotSame(afterDefaults, beforeDefaults);
    }

    /**
     * Collect of elements.
     *
     * @param iterable Iterable.
     * @param <T> Type of the elements.
     * @return Collected elements.
     */
    private <T> List<? extends T> collect(Iterable<? extends T> iterable) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterable.iterator(), 0), false).collect(toList());
    }

    @Test
    void testMapIterable() {
        assertFalse(mapIterable(null, null).iterator().hasNext());
        assertFalse(mapIterable(emptyList(), null).iterator().hasNext());

        assertEquals(List.of(1), collect(mapIterable(List.of(1), null)));
        assertEquals(List.of(1), collect(mapIterable(List.of(1), identity())));

        assertEquals(List.of("1", "2", "3"), collect(mapIterable(List.of(1, 2, 3), String::valueOf)));

        assertThrows(UnsupportedOperationException.class, () -> mapIterable(List.of(1), null).iterator().remove());

        Iterator<Integer> iterator = mapIterable(Arrays.asList(1, 2, null, 3), identity()).iterator();

        // Test iterator contracts
        assertTrue(iterator.hasNext());
        assertEquals(1, iterator.next());

        assertEquals(2, iterator.next());

        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());
        assertEquals(3, iterator.next());

        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
        assertThrows(NoSuchElementException.class, iterator::next);
        assertFalse(iterator.hasNext());
    }

    /**
     * Patches super root and returns flat representation of the changes. Passed {@code superRoot} object will contain patched tree when
     * method execution is completed.
     *
     * @param superRoot Super root to patch.
     * @param storageData Full storage data.
     * @param patch Closure to change inner node.
     * @return Flat map with all changes from the patch.
     */
    private static Map<String, Serializable> flattenedMap(
            SuperRoot superRoot,
            RootKey<?, ?, ?> rootKey,
            NavigableMap<String, ? extends Serializable> storageData,
            Consumer<InnerNode> patch
    ) {
        // Preserve a copy of the super root to use it as a golden source of data.
        SuperRoot originalSuperRoot = superRoot.copy();

        // Make a copy of the root inside the superRoot. This copy will be used for further patching.
        superRoot.construct(rootKey.key(), EMPTY_CFG_SRC, true);

        // Patch root node.
        patch.accept(superRoot.getRoot(rootKey));

        // Create flat diff between two super trees.
        return createFlattenedUpdatesMap(originalSuperRoot, superRoot, storageData);
    }

    /**
     * First local configuration.
     */
    @ConfigurationRoot(rootName = "localFirst", type = LOCAL)
    public static class LocalFirstConfigurationSchema {
        /** String field. */
        @Value(hasDefault = true)
        public String str = "str";
    }

    /**
     * Second local configuration.
     */
    @ConfigurationRoot(rootName = "localSecond", type = LOCAL)
    public static class LocalSecondConfigurationSchema {
        /** String field. */
        @Value(hasDefault = true)
        public String str = "str";
    }

    /**
     * First distributed configuration.
     */
    @ConfigurationRoot(rootName = "distributedFirst", type = DISTRIBUTED)
    public static class DistributedFirstConfigurationSchema {
        /** String field. */
        @Value(hasDefault = true)
        public String str = "str";
    }

    /**
     * Second distributed configuration.
     */
    @ConfigurationRoot(rootName = "distributedSecond", type = DISTRIBUTED)
    public static class DistributedSecondConfigurationSchema {
        /** String field. */
        @Value(hasDefault = true)
        public String str = "str";
    }

    /**
     * Simple root configuration schema.
     */
    @ConfigurationRoot(rootName = "rootInternal")
    public static class InternalRootConfigurationSchema {
        /** String value without default. */
        @Value
        public String str0;

        /** String value with default. */
        @Value(hasDefault = true)
        public String str1 = "foo";

        /** Sub configuration schema. */
        @ConfigValue
        public InternalConfigurationSchema subCfg;

        /** Named configuration schema. */
        @NamedConfigValue
        public InternalConfigurationSchema namedCfg;
    }

    /**
     * Simple configuration schema.
     */
    @Config
    public static class InternalConfigurationSchema {
        /** String value with default. */
        @Value(hasDefault = true)
        public String str00 = "foo";
    }

    /**
     * Internal schema extension without superclass.
     */
    @ConfigurationExtension(internal = true)
    @ConfigurationRoot(rootName = "testRootInternal")
    public static class InternalWithoutSuperclassConfigurationSchema {
    }

    /**
     * First simple internal schema extension.
     */
    @ConfigurationExtension(internal = true)
    public static class InternalFirstConfigurationSchema extends InternalConfigurationSchema {
        /** String value with default. */
        @Value(hasDefault = true)
        public String str01 = "foo";
    }

    /**
     * Second simple internal schema extension.
     */
    @ConfigurationExtension(internal = true)
    public static class InternalSecondConfigurationSchema extends InternalConfigurationSchema {
        /** String value with default. */
        @Value(hasDefault = true)
        public String str02 = "foo";
    }

    /**
     * Public schema extension.
     */
    @ConfigurationExtension
    public static class PublicConfigurationSchema extends InternalConfigurationSchema {
        /** String value with default. */
        @Value(hasDefault = true)
        public String publicStr03 = "bar";
    }

    /**
     * First root simple internal schema extension.
     */
    @ConfigurationExtension(internal = true)
    public static class InternalFirstRootConfigurationSchema extends InternalRootConfigurationSchema {
        /** Second string value without default. */
        @Value
        public String str2;

        /** Second string value with default. */
        @Value(hasDefault = true)
        public String str3 = "foo";
    }

    /**
     * Second root simple internal schema extension.
     */
    @ConfigurationExtension(internal = true)
    public static class InternalSecondRootConfigurationSchema extends InternalRootConfigurationSchema {
        /** Second sub configuration schema. */
        @ConfigValue
        public InternalConfigurationSchema subCfg1;
    }

    /**
     * Public root schema extension.
     */
    @ConfigurationExtension
    public static class PublicRootConfigurationSchema extends InternalRootConfigurationSchema {
        /** Second sub configuration schema. */
        @ConfigValue
        public InternalConfigurationSchema publicSubCfg1;
    }

    /**
     * Internal extended simple root configuration schema.
     */
    @ConfigurationExtension(internal = true)
    public static class InternalExtendedRootConfigurationSchema extends InternalRootConfigurationSchema {
        /** String value without default. */
        @Value
        public String str00;

        /** String value with default. */
        @Value(hasDefault = true)
        public String str01 = "foo";

        /** Sub configuration schema. */
        @ConfigValue
        public InternalConfigurationSchema subCfg0;

        /** Named configuration schema. */
        @NamedConfigValue
        public InternalConfigurationSchema namedCfg0;
    }

    /**
     * Error: Duplicate field.
     */
    @ConfigurationExtension(internal = true)
    public static class ErrorInternalExtendedRootConfigurationSchema extends InternalRootConfigurationSchema {
        /** String value without default. */
        @Value
        public String str00;
    }

    /**
     * Simple root polymorphic configuration.
     */
    @ConfigurationRoot(rootName = "rootPolymorphic")
    public static class PolymorphicRootConfigurationSchema {
        /** Polymorphic sub configuration schema. */
        @ConfigValue
        public PolymorphicConfigurationSchema polymorphicSubCfg;

        /** Polymorphic named configuration schema. */
        @NamedConfigValue
        public PolymorphicConfigurationSchema polymorphicNamedCfg;
    }

    /**
     * Simple polymorphic configuration.
     */
    @PolymorphicConfig
    public static class PolymorphicConfigurationSchema {
        /** Polymorphic type id field. */
        @PolymorphicId(hasDefault = true)
        public String typeId = "first";

        /** Long value. */
        @Value(hasDefault = true)
        public long longVal = 0;
    }

    /**
     * First {@link PolymorphicConfigurationSchema} extension.
     */
    @PolymorphicConfigInstance("first")
    public static class FirstPolymorphicInstanceConfigurationSchema extends PolymorphicConfigurationSchema {
        /** String value. */
        @Value(hasDefault = true)
        public String strVal = "strVal";

        @Value(hasDefault = true)
        public String someVal = "someVal";
    }

    /**
     * Second {@link PolymorphicConfigurationSchema} extension.
     */
    @PolymorphicConfigInstance("second")
    public static class SecondPolymorphicInstanceConfigurationSchema extends PolymorphicConfigurationSchema {
        /** Integer value. */
        @Value(hasDefault = true)
        public int intVal = 0;

        @Value(hasDefault = true)
        public int someVal = 10;
    }
}
