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

package org.apache.ignite.internal.configuration.util;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitorImpl;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNode;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static java.util.Collections.singletonMap;
import static java.util.Comparator.comparing;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.tree.NamedListNode.NAME;
import static org.apache.ignite.internal.configuration.tree.NamedListNode.ORDER_IDX;
import static org.apache.ignite.internal.configuration.util.ConfigurationFlattener.createFlattenedUpdatesMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.addDefaults;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.checkConfigurationType;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.collectShemas;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.find;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.internalSchemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.mergedSchemaFields;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** */
public class ConfigurationUtilTest {
    private static ConfigurationAsmGenerator cgen;

    @BeforeAll
    public static void beforeAll() {
        cgen = new ConfigurationAsmGenerator();

        cgen.compileRootSchema(ParentConfigurationSchema.class, Map.of());
    }

    @AfterAll
    public static void afterAll() {
        cgen = null;
    }

    public static <P extends InnerNode & ParentChange> P newParentInstance() {
        return (P)cgen.instantiateNode(ParentConfigurationSchema.class);
    }

    /** */
    @Test
    public void escape() {
        assertEquals("foo", ConfigurationUtil.escape("foo"));

        assertEquals("foo\\.bar", ConfigurationUtil.escape("foo.bar"));

        assertEquals("foo\\\\bar", ConfigurationUtil.escape("foo\\bar"));

        assertEquals("\\\\a\\.b\\\\c\\.", ConfigurationUtil.escape("\\a.b\\c."));
    }

    /** */
    @Test
    public void unescape() {
        assertEquals("foo", ConfigurationUtil.unescape("foo"));

        assertEquals("foo.bar", ConfigurationUtil.unescape("foo\\.bar"));

        assertEquals("foo\\bar", ConfigurationUtil.unescape("foo\\\\bar"));

        assertEquals("\\a.b\\c.", ConfigurationUtil.unescape("\\\\a\\.b\\\\c\\."));
    }

    /** */
    @Test
    public void split() {
        assertEquals(List.of("a", "b.b", "c\\c", ""), ConfigurationUtil.split("a.b\\.b.c\\\\c."));
    }

    /** */
    @Test
    public void join() {
        assertEquals("a.b\\.b.c\\\\c", ConfigurationUtil.join(List.of("a", "b.b", "c\\c")));
    }

    /** */
    @ConfigurationRoot(rootName = "root", type = LOCAL)
    public static class ParentConfigurationSchema {
        /** */
        @NamedConfigValue
        public NamedElementConfigurationSchema elements;
    }

    /** */
    @Config
    public static class NamedElementConfigurationSchema {
        /** */
        @ConfigValue
        public ChildConfigurationSchema child;
    }

    /** */
    @Config
    public static class ChildConfigurationSchema {
        /** */
        @Value
        public String str;
    }

    /**
     * Tests that {@link ConfigurationUtil#find(List, TraversableTreeNode, boolean)} finds proper node when provided
     * with correct path.
     */
    @Test
    public void findSuccessfully() {
        var parent = newParentInstance();

        parent.changeElements(elements ->
            elements.createOrUpdate("name", element ->
                element.changeChild(child ->
                    child.changeStr("value")
                )
            )
        );

        assertSame(
            parent,
            ConfigurationUtil.find(List.of(), parent, true)
        );

        assertSame(
            parent.elements(),
            ConfigurationUtil.find(List.of("elements"), parent, true)
        );

        assertSame(
            parent.elements().get("name"),
            ConfigurationUtil.find(List.of("elements", "name"), parent, true)
        );

        assertSame(
            parent.elements().get("name").child(),
            ConfigurationUtil.find(List.of("elements", "name", "child"), parent, true)
        );

        assertSame(
            parent.elements().get("name").child().str(),
            ConfigurationUtil.find(List.of("elements", "name", "child", "str"), parent, true)
        );
    }

    /**
     * Tests that {@link ConfigurationUtil#find(List, TraversableTreeNode, boolean)} returns null when path points to
     * nonexistent named list element.
     */
    @Test
    public void findNulls() {
        var parent = newParentInstance();

        assertNull(ConfigurationUtil.find(List.of("elements", "name"), parent, true));

        parent.changeElements(elements -> elements.createOrUpdate("name", element -> {}));

        assertNull(ConfigurationUtil.find(List.of("elements", "name", "child"), parent, true));

        ((NamedElementChange)parent.elements().get("name")).changeChild(child -> {});

        assertNull(ConfigurationUtil.find(List.of("elements", "name", "child", "str"), parent, true));
    }

    /**
     * Tests that {@link ConfigurationUtil#find(List, TraversableTreeNode, boolean)} throws
     * {@link KeyNotFoundException} when provided with a wrong path.
     */
    @Test
    public void findUnsuccessfully() {
        var parent = newParentInstance();

        assertThrows(
            KeyNotFoundException.class,
            () -> ConfigurationUtil.find(List.of("elements", "name", "child"), parent, true)
        );

        parent.changeElements(elements -> elements.createOrUpdate("name", element -> {}));

        assertThrows(
            KeyNotFoundException.class,
            () -> ConfigurationUtil.find(List.of("elements", "name", "child", "str"), parent, true)
        );

        ((NamedElementChange)parent.elements().get("name")).changeChild(child -> child.changeStr("value"));

        assertThrows(
            KeyNotFoundException.class,
            () -> ConfigurationUtil.find(List.of("elements", "name", "child", "str", "foo"), parent, true)
        );
    }

    /**
     * Tests convertion of flat map to a prefix map.
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
        var parentNode = newParentInstance();

        ConfigurationUtil.fillFromPrefixMap(parentNode, Map.of(
            "elements", Map.of(
                "0123456789abcde0123456789abcde", Map.of(
                    "child", Map.of("str", "value2"),
                    ORDER_IDX, 1,
                    NAME, "name2"
                ),
                "12345689abcdef0123456789abcdef0", Map.of(
                    "child", Map.of("str", "value1"),
                    ORDER_IDX, 0,
                    NAME, "name1"
                )
            )
        ));

        assertEquals("value1", parentNode.elements().get("name1").child().str());
        assertEquals("value2", parentNode.elements().get("name2").child().str());
    }

    /**
     * Tests that patching of configuration node with a prefix map works fine when prefix map is valid.
     */
    @Test
    public void fillFromPrefixMapSuccessfullyWithRemove() {
        var parentNode = newParentInstance();

        parentNode.changeElements(elements ->
            elements.createOrUpdate("name", element ->
                element.changeChild(child -> {})
            )
        );

        ConfigurationUtil.fillFromPrefixMap(parentNode, Map.of(
            "elements", singletonMap("name", null)
        ));

        assertNull(parentNode.elements().get("node"));
    }

    /**
     * Tests that conversion from "changer" lambda to a flat map of updates for the storage works properly.
     */
    @Test
    public void flattenedUpdatesMap() {
        var superRoot = new SuperRoot(key -> null, Map.of(ParentConfiguration.KEY, newParentInstance()));

        assertThat(flattenedMap(superRoot, parent -> {}), is(anEmptyMap()));

        assertThat(
            flattenedMap(superRoot, parent -> parent
                .changeElements(elements -> elements
                    .create("name", element -> element
                        .changeChild(child -> child.changeStr("foo"))
                    )
                )
            ),
            is(allOf(
                aMapWithSize(3),
                hasEntry(matchesPattern("root[.]elements[.]\\w{32}[.]child[.]str"), hasToString("foo")),
                hasEntry(matchesPattern("root[.]elements[.]\\w{32}[.]<order>"), is(0)),
                hasEntry(matchesPattern("root[.]elements[.]\\w{32}[.]<name>"), hasToString("name"))
            ))
        );

        assertThat(
            flattenedMap(superRoot, parent -> parent
                .changeElements(elements1 -> elements1.delete("void"))
            ),
            is(anEmptyMap())
        );

        assertThat(
            flattenedMap(superRoot, parent -> parent
                .changeElements(elements -> elements.delete("name"))
            ),
            is(allOf(
                aMapWithSize(3),
                hasEntry(matchesPattern("root[.]elements[.]\\w{32}[.]child[.]str"), nullValue()),
                hasEntry(matchesPattern("root[.]elements[.]\\w{32}[.]<order>"), nullValue()),
                hasEntry(matchesPattern("root[.]elements[.]\\w{32}[.]<name>"), nullValue())
            ))
        );
    }

    /** */
    @Test
    void testCheckConfigurationTypeMixedTypes() {
        List<RootKey<?, ?>> rootKeys = List.of(LocalFirstConfiguration.KEY, DistributedFirstConfiguration.KEY);

        assertThrows(
            IllegalArgumentException.class,
            () -> checkConfigurationType(rootKeys, new TestConfigurationStorage(LOCAL))
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> checkConfigurationType(rootKeys, new TestConfigurationStorage(DISTRIBUTED))
        );
    }

    /** */
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

    /** */
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

    /** */
    @Test
    void testErrorInternalSchemaExtensions() {
        assertThrows(
            IllegalArgumentException.class,
            () -> internalSchemaExtensions(List.of(SimpleConfigurationSchema.class))
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> internalSchemaExtensions(List.of(SimpleRootConfigurationSchema.class))
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> internalSchemaExtensions(List.of(InternalWithoutSuperclassConfigurationSchema.class))
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> internalSchemaExtensions(List.of(InternalSuperclassConfigurationSchema.class))
        );
    }

    /** */
    @Test
    void testSuccessInternalSchemaExtensions() {
        assertTrue(internalSchemaExtensions(List.of()).isEmpty());

        Map<Class<?>, Set<Class<?>>> res = internalSchemaExtensions(List.of(
            InternalFirstSimpleConfigurationSchema.class,
            InternalSecondSimpleConfigurationSchema.class,
            InternalFirstSimpleRootConfigurationSchema.class,
            InternalSecondSimpleRootConfigurationSchema.class
        ));

        assertEquals(2, res.size());
        assertTrue(res.containsKey(SimpleConfigurationSchema.class));
        assertTrue(res.containsKey(SimpleRootConfigurationSchema.class));

        Set<Class<?>> simpleInternalSchemaExtensions = Set.of(
            InternalFirstSimpleConfigurationSchema.class,
            InternalSecondSimpleConfigurationSchema.class
        );

        Set<Class<?>> simpleRootInternalSchemaExtensions = Set.of(
            InternalFirstSimpleRootConfigurationSchema.class,
            InternalSecondSimpleRootConfigurationSchema.class
        );

        assertEquals(simpleInternalSchemaExtensions, res.get(SimpleConfigurationSchema.class));
        assertEquals(simpleRootInternalSchemaExtensions, res.get(SimpleRootConfigurationSchema.class));
    }

    /** */
    @Test
    void testErrorMergeSchemaFields() {
        Class<?> schema = SimpleRootConfigurationSchema.class;

        assertThrows(
            IllegalArgumentException.class,
            () -> mergedSchemaFields(schema, List.of(InternalError0SimpleRootConfigurationSchema.class))
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> mergedSchemaFields(schema, List.of(InternalError1SimpleRootConfigurationSchema.class))
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> mergedSchemaFields(schema, List.of(InternalError2SimpleRootConfigurationSchema.class))
        );
    }

    /** */
    @Test
    void testSuccessMergeSchemaFields() {
        Class<?> schema = SimpleRootConfigurationSchema.class;

        assertEquals(
            Set.of(schema.getDeclaredFields()),
            mergedSchemaFields(schema, List.of())
        );

        List<Class<?>> extensions = List.of(
            InternalFirstSimpleRootConfigurationSchema.class,
            InternalSecondSimpleRootConfigurationSchema.class
        );

        Map<String, Field> exp = Arrays.stream(schema.getDeclaredFields()).collect(toMap(Field::getName, identity()));

        extensions.stream()
            .flatMap(cls -> Stream.of(cls.getDeclaredFields()))
            .filter(field -> !exp.containsKey(field.getName()))
            .forEach(field -> exp.put(field.getName(), field));

        assertEquals(
            exp.values().stream().sorted(comparing(Field::getName)).collect(toList()),
            mergedSchemaFields(schema, extensions).stream().sorted(comparing(Field::getName)).collect(toList())
        );
    }

    /** */
    @Test
    void testFindInternalConfigs() {
        Map<Class<?>, Set<Class<?>>> extensions = internalSchemaExtensions(List.of(
            InternalFirstSimpleRootConfigurationSchema.class,
            InternalSecondSimpleRootConfigurationSchema.class,
            InternalFirstSimpleConfigurationSchema.class,
            InternalSecondSimpleConfigurationSchema.class
        ));

        ConfigurationAsmGenerator generator = new ConfigurationAsmGenerator();
        generator.compileRootSchema(SimpleRootConfigurationSchema.class, extensions);

        InnerNode innerNode = generator.instantiateNode(SimpleRootConfigurationSchema.class);

        addDefaults(innerNode);

        // Check that no internal configuration will be found.

        assertThrows(KeyNotFoundException.class, () -> find(List.of("str2"), innerNode, false));
        assertThrows(KeyNotFoundException.class, () -> find(List.of("str3"), innerNode, false));
        assertThrows(KeyNotFoundException.class, () -> find(List.of("subCfg1"), innerNode, false));

        assertThrows(KeyNotFoundException.class, () -> find(List.of("subCfg", "str01"), innerNode, false));
        assertThrows(KeyNotFoundException.class, () -> find(List.of("subCfg", "str02"), innerNode, false));

        // Check that internal configuration will be found.

        assertNull(find(List.of("str2"), innerNode, true));
        assertEquals("foo", find(List.of("str3"), innerNode, true));
        assertNotNull(find(List.of("subCfg1"), innerNode, true));

        assertEquals("foo", find(List.of("subCfg", "str01"), innerNode, true));
        assertEquals("foo", find(List.of("subCfg", "str02"), innerNode, true));
    }

    /** */
    @Test
    void testGetInternalConfigs() {
        Map<Class<?>, Set<Class<?>>> extensions = internalSchemaExtensions(List.of(
            InternalFirstSimpleRootConfigurationSchema.class,
            InternalSecondSimpleRootConfigurationSchema.class,
            InternalFirstSimpleConfigurationSchema.class,
            InternalSecondSimpleConfigurationSchema.class
        ));

        ConfigurationAsmGenerator generator = new ConfigurationAsmGenerator();
        generator.compileRootSchema(SimpleRootConfigurationSchema.class, extensions);

        InnerNode innerNode = generator.instantiateNode(SimpleRootConfigurationSchema.class);

        addDefaults(innerNode);

        Map<String, Object> config = (Map<String, Object>)innerNode.accept(null, new ConfigurationVisitorImpl(false));

        // Check that no internal configuration will be received.

        assertEquals(4, config.size());
        assertNull(config.get("str0"));
        assertEquals("foo", config.get("str1"));
        assertNotNull(config.get("subCfg"));
        assertNotNull(config.get("namedCfg"));

        Map<String, Object> subConfig = (Map<String, Object>)config.get("subCfg");

        assertEquals(1, subConfig.size());
        assertEquals("foo", subConfig.get("str00"));

        // Check that no internal configuration will be received.

        config = (Map<String, Object>)innerNode.accept(null, new ConfigurationVisitorImpl(true));

        assertEquals(7, config.size());
        assertNull(config.get("str0"));
        assertNull(config.get("str2"));
        assertEquals("foo", config.get("str1"));
        assertEquals("foo", config.get("str3"));
        assertNotNull(config.get("subCfg"));
        assertNotNull(config.get("subCfg1"));
        assertNotNull(config.get("namedCfg"));

        subConfig = (Map<String, Object>)config.get("subCfg");

        assertEquals(3, subConfig.size());
        assertEquals("foo", subConfig.get("str00"));
        assertEquals("foo", subConfig.get("str01"));
        assertEquals("foo", subConfig.get("str02"));

        subConfig = (Map<String, Object>)config.get("subCfg1");

        assertEquals(3, subConfig.size());
        assertEquals("foo", subConfig.get("str00"));
        assertEquals("foo", subConfig.get("str01"));
        assertEquals("foo", subConfig.get("str02"));
    }

    /** */
    @Test
    void testSuperRootWithInternalConfig() {
        ConfigurationAsmGenerator generator = new ConfigurationAsmGenerator();

        Class<?> schemaClass = InternalWithoutSuperclassConfigurationSchema.class;
        RootKey<?, ?> schemaKey = InternalWithoutSuperclassConfiguration.KEY;

        generator.compileRootSchema(schemaClass, Map.of());

        SuperRoot superRoot = new SuperRoot(
            s -> new IgniteBiTuple<>(schemaKey, generator.instantiateNode(schemaClass))
        );

        assertThrows(NoSuchElementException.class, () -> superRoot.construct(schemaKey.key(), null, false));

        superRoot.construct(schemaKey.key(), null, true);

        superRoot.addRoot(schemaKey, generator.instantiateNode(schemaClass));

        assertThrows(KeyNotFoundException.class, () -> find(List.of(schemaKey.key()), superRoot, false));

        assertNotNull(find(List.of(schemaKey.key()), superRoot, true));

        Map<String, Object> config =
            (Map<String, Object>)superRoot.accept(schemaKey.key(), new ConfigurationVisitorImpl(false));

        assertTrue(config.isEmpty());

        config = (Map<String, Object>)superRoot.accept(schemaKey.key(), new ConfigurationVisitorImpl(true));

        assertEquals(1, config.size());
        assertNotNull(config.get(schemaKey.key()));
    }

    /** */
    @Test
    void testCollectShemas() {
        assertTrue(collectShemas(List.of()).isEmpty());

        assertThrows(IllegalArgumentException.class, () -> collectShemas(List.of(Object.class)));

        assertEquals(
            Set.of(LocalFirstConfigurationSchema.class, SimpleConfigurationSchema.class),
            collectShemas(List.of(LocalFirstConfigurationSchema.class, SimpleConfigurationSchema.class))
        );

        assertEquals(
            Set.of(SimpleRootConfigurationSchema.class, SimpleConfigurationSchema.class),
            collectShemas(List.of(SimpleRootConfigurationSchema.class))
        );
    }

    /**
     * Patches super root and returns flat representation of the changes. Passed {@code superRoot} object will contain
     * patched tree when method execution is completed.
     *
     * @param superRoot Super root to patch.
     * @param patch Closure to cnahge parent node.
     * @return Flat map with all changes from the patch.
     */
    @NotNull private Map<String, Serializable> flattenedMap(SuperRoot superRoot, Consumer<ParentChange> patch) {
        // Preserve a copy of the super root to use it as a golden source of data.
        SuperRoot originalSuperRoot = superRoot.copy();

        // Make a copy of the root insode of the superRoot. This copy will be used for further patching.
        superRoot.construct(ParentConfiguration.KEY.key(), new ConfigurationSource() {}, true);

        // Patch root node.
        patch.accept((ParentChange)superRoot.getRoot(ParentConfiguration.KEY));

        // Create flat diff between two super trees.
        return createFlattenedUpdatesMap(originalSuperRoot, superRoot);
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
    @ConfigurationRoot(rootName = "testRootSimple")
    public static class SimpleRootConfigurationSchema {
        /** String value without default. */
        @Value
        public String str0;

        /** String value with default. */
        @Value(hasDefault = true)
        public String str1 = "foo";

        /** Sub configuration schema. */
        @ConfigValue
        public SimpleConfigurationSchema subCfg;

        /** Named configuration schema. */
        @NamedConfigValue
        public SimpleConfigurationSchema namedCfg;
    }

    /**
     * Simple configuration schema.
     */
    @Config
    public static class SimpleConfigurationSchema {
        /** String value with default. */
        @Value(hasDefault = true)
        public String str00 = "foo";
    }

    /**
     * Internal schema extension without superclass.
     */
    @InternalConfiguration
    @ConfigurationRoot(rootName = "testRootInternal")
    public static class InternalWithoutSuperclassConfigurationSchema {
    }

    /**
     * Internal schema extension with an internal superclass.
     */
    @InternalConfiguration
    public static class InternalSuperclassConfigurationSchema extends InternalWithoutSuperclassConfigurationSchema {
    }

    /**
     * First simple internal schema extension.
     */
    @InternalConfiguration
    public static class InternalFirstSimpleConfigurationSchema extends SimpleConfigurationSchema {
        /** String value with default. */
        @Value(hasDefault = true)
        public String str01 = "foo";
    }

    /**
     * Second simple internal schema extension.
     */
    @InternalConfiguration
    public static class InternalSecondSimpleConfigurationSchema extends SimpleConfigurationSchema {
        /** String value with default. */
        @Value(hasDefault = true)
        public String str02 = "foo";
    }

    /**
     * First root simple internal schema extension.
     */
    @InternalConfiguration
    public static class InternalFirstSimpleRootConfigurationSchema extends SimpleRootConfigurationSchema {
        /** String value without default. */
        @Value
        public String str0;

        /** String value with default. */
        @Value(hasDefault = true)
        public String str1 = "foo";

        /** Sub configuration schema. */
        @ConfigValue
        public SimpleConfigurationSchema subCfg;

        /** Named configuration schema. */
        @NamedConfigValue
        public SimpleConfigurationSchema namedCfg;

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
    @InternalConfiguration
    public static class InternalSecondSimpleRootConfigurationSchema extends SimpleRootConfigurationSchema {
        /** String value without default. */
        @Value
        public String str0;

        /** String value with default. */
        @Value(hasDefault = true)
        public String str1 = "foo";

        /** Sub configuration schema. */
        @ConfigValue
        public SimpleConfigurationSchema subCfg;

        /** Named configuration schema. */
        @NamedConfigValue
        public SimpleConfigurationSchema namedCfg;

        /** Second string value without default. */
        @Value
        public String str2;

        /** Second sub configuration schema. */
        @ConfigValue
        public SimpleConfigurationSchema subCfg1;
    }

    /**
     * Root config extension with error: {@link #str0} changed {@link Value#hasDefault} -> {@code true}.
     */
    @InternalConfiguration
    public static class InternalError0SimpleRootConfigurationSchema extends SimpleRootConfigurationSchema {
        /** String value with default. */
        @Value(hasDefault = true)
        public String str0 = "foo";
    }

    /**
     * Root config extension with error: {@link #str1} changed default value.
     */
    @InternalConfiguration
    public static class InternalError1SimpleRootConfigurationSchema extends SimpleRootConfigurationSchema {
        /** String value with default. */
        @Value(hasDefault = true)
        public String str1 = "bar";
    }

    /**
     * Root config extension with error: {@link #namedCfg} changed {@link NamedConfigValue#syntheticKeyName}.
     */
    @InternalConfiguration
    public static class InternalError2SimpleRootConfigurationSchema extends SimpleRootConfigurationSchema {
        /** Sub configuration schema. */
        @NamedConfigValue(syntheticKeyName = "foo")
        public SimpleConfigurationSchema namedCfg;
    }
}
