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

package org.apache.ignite.internal.configuration.hocon;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.hocon.HoconConverter.hoconSource;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.Name;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.manager.ComponentContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Tests for the {@link HoconConverter}.
 */
public class HoconConverterTest {
    /**
     * Hocon root configuration schema.
     */
    @ConfigurationRoot(rootName = "root", type = LOCAL)
    public static class HoconRootConfigurationSchema {
        @NamedConfigValue(syntheticKeyName = "a")
        public HoconArraysConfigurationSchema arraysList;

        @NamedConfigValue(syntheticKeyName = "p")
        public HoconPrimitivesConfigurationSchema primitivesList;

        @NamedConfigValue(syntheticKeyName = "poly")
        public HoconPolymorphicConfigurationSchema polymorphicCfg;
    }

    /**
     * Configuration schema for testing the support of arrays of primitives.
     */
    @Config
    public static class HoconArraysConfigurationSchema {
        @Value(hasDefault = true)
        public boolean[] booleans = {false};

        @Value(hasDefault = true)
        public byte[] bytes = {0};

        @Value(hasDefault = true)
        public short[] shorts = {0};

        @Value(hasDefault = true)
        public int[] ints = {0};

        @Value(hasDefault = true)
        public long[] longs = {0L};

        @Value(hasDefault = true)
        public char[] chars = {0};

        @Value(hasDefault = true)
        public float[] floats = {0};

        @Value(hasDefault = true)
        public double[] doubles = {0};

        @Value(hasDefault = true)
        public String[] strings = {""};

        @Value(hasDefault = true)
        public UUID[] uuids = {new UUID(1111, 2222)};
    }

    /**
     * Configuration schema for testing the support of primitives.
     */
    @Config
    public static class HoconPrimitivesConfigurationSchema {
        @Value(hasDefault = true)
        public boolean booleanVal = false;

        @Value(hasDefault = true)
        public byte byteVal = 0;

        @Value(hasDefault = true)
        public short shortVal = 0;

        @Value(hasDefault = true)
        public int intVal = 0;

        @Value(hasDefault = true)
        public long longVal = 0L;

        @Value(hasDefault = true)
        public char charVal = 0;

        @Value(hasDefault = true)
        public float floatVal = 0;

        @Value(hasDefault = true)
        public double doubleVal = 0;

        @Value(hasDefault = true)
        public String stringVal = "";

        @Value(hasDefault = true)
        public UUID uuidVal = new UUID(100, 200);
    }

    /**
     * Configuration schema for testing the support of polymorphic configuration.
     */
    @PolymorphicConfig
    public static class HoconPolymorphicConfigurationSchema {
        @PolymorphicId
        public String typeId;
    }

    /**
     * Configuration schema for testing the support of polymorphic configuration.
     */
    @PolymorphicConfigInstance("first")
    public static class HoconFirstPolymorphicInstanceConfigurationSchema extends HoconPolymorphicConfigurationSchema {
        @Value(hasDefault = true)
        public int longVal = 0;
    }

    /**
     * Configuration schema for testing the support of polymorphic configuration.
     */
    @PolymorphicConfigInstance("second")
    public static class HoconSecondPolymorphicInstanceConfigurationSchema extends HoconPolymorphicConfigurationSchema {
        @Value(hasDefault = true)
        public int intVal = 0;
    }

    /**
     * Hocon root configuration schema for testing the support of {@link InjectedName}.
     */
    @ConfigurationRoot(rootName = "rootInjectedName", type = LOCAL)
    public static class HoconInjectedNameRootConfigurationSchema {
        @Name("testDefault")
        @org.apache.ignite.configuration.annotation.ConfigValue
        public HoconInjectedNameConfigurationSchema nested;

        @NamedConfigValue(syntheticKeyName = "superName")
        public HoconInjectedNameConfigurationSchema nestedNamed;
    }

    /**
     * Configuration schema for testing the support of {@link InjectedName}.
     */
    @Config
    public static class HoconInjectedNameConfigurationSchema {
        @InjectedName
        public String someName;
    }

    private static ConfigurationRegistry registry;

    private static HoconRootConfiguration configuration;

    private static HoconInjectedNameRootConfiguration injectedNameRootConfig;

    /**
     * Before all.
     */
    @BeforeAll
    public static void beforeAll() {
        registry = new ConfigurationRegistry(
                List.of(HoconRootConfiguration.KEY, HoconInjectedNameRootConfiguration.KEY),
                new TestConfigurationStorage(LOCAL),
                new ConfigurationTreeGenerator(
                        List.of(HoconRootConfiguration.KEY, HoconInjectedNameRootConfiguration.KEY),
                        List.of(),
                        List.of(
                                HoconFirstPolymorphicInstanceConfigurationSchema.class,
                                HoconSecondPolymorphicInstanceConfigurationSchema.class
                        )
                ),
                new TestConfigurationValidator()
        );

        assertThat(registry.startAsync(new ComponentContext()), willCompleteSuccessfully());

        configuration = registry.getConfiguration(HoconRootConfiguration.KEY);
        injectedNameRootConfig = registry.getConfiguration(HoconInjectedNameRootConfiguration.KEY);
    }

    /**
     * After all.
     */
    @AfterAll
    public static void after() {
        assertThat(registry.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        registry = null;

        configuration = null;
        injectedNameRootConfig = null;
    }

    /**
     * Before each.
     */
    @BeforeEach
    public void before() throws Exception {
        configuration.change(cfg -> cfg
                .changePrimitivesList(list -> list.namedListKeys().forEach(list::delete))
                .changeArraysList(list -> list.namedListKeys().forEach(list::delete))
                .changePolymorphicCfg(c -> {})
        ).get(1, SECONDS);

        injectedNameRootConfig.change(c -> c
                .changeNested(c0 -> {})
                .changeNestedNamed(list -> list.namedListKeys().forEach(list::delete))
        ).get(1, SECONDS);
    }

    @Test
    public void toHoconBasic() {
        assertEquals(
                "root{arraysList=[],polymorphicCfg=[],primitivesList=[]},rootInjectedName{nested{},nestedNamed=[]}",
                asHoconStr(List.of())
        );

        assertEquals("arraysList=[],polymorphicCfg=[],primitivesList=[]", asHoconStr(List.of("root")));

        assertEquals("[]", asHoconStr(List.of("root", "arraysList")));

        assertThrowsIllegalArgException(
                () -> HoconConverter.represent(registry.superRoot(), List.of("doot")),
                "Configuration value 'doot' has not been found"
        );

        assertThrowsIllegalArgException(
                () -> HoconConverter.represent(registry.superRoot(), List.of("root", "x")),
                "Configuration value 'root.x' has not been found"
        );

        assertEquals("null", asHoconStr(List.of("root", "primitivesList", "foo")));
    }

    /**
     * Tests that the {@code HoconConverter} supports serialization of {@link String}, {@link UUID} and primitives.
     */
    @Test
    public void testHoconPrimitivesSerialization() throws Exception {
        configuration.change(cfg -> cfg
                .changePrimitivesList(primitivesList -> primitivesList
                        .create("name", primitives -> {
                        })
                )
        ).get(1, SECONDS);

        var basePath = List.of("root", "primitivesList", "name");

        UUID uuid = new UUID(100, 200);

        assertEquals(
                "booleanVal=false,byteVal=0,charVal=\"\\u0000\",doubleVal=0.0,floatVal=0,intVal=0,longVal=0,shortVal=0,stringVal=\"\""
                        + ",uuidVal=\"" + uuid + "\"",
                asHoconStr(basePath)
        );

        assertEquals("false", asHoconStr(basePath, "booleanVal"));
        assertEquals("0", asHoconStr(basePath, "byteVal"));
        assertEquals("0", asHoconStr(basePath, "shortVal"));
        assertEquals("0", asHoconStr(basePath, "intVal"));
        assertEquals("0", asHoconStr(basePath, "longVal"));
        assertEquals("\"\\u0000\"", asHoconStr(basePath, "charVal"));
        assertEquals("0", asHoconStr(basePath, "floatVal"));
        assertEquals("0.0", asHoconStr(basePath, "doubleVal"));
        assertEquals("\"\"", asHoconStr(basePath, "stringVal"));
        assertEquals("\"" + uuid + "\"", asHoconStr(basePath, "uuidVal"));
    }

    /**
     * Tests that the {@code HoconConverter} supports serialization of arrays of {@link String}, {@link UUID} and primitives.
     */
    @Test
    public void testHoconArraysSerialization() throws Exception {
        configuration.change(cfg -> cfg
                .changeArraysList(arraysList -> arraysList
                        .create("name", arrays -> {
                        })
                )
        ).get(1, SECONDS);

        var basePath = List.of("root", "arraysList", "name");

        UUID uuid = new UUID(1111, 2222);

        assertEquals(
                "booleans=[false],bytes=[0],chars=[\"\\u0000\"],doubles=[0.0],floats=[0],ints=[0],longs=[0],shorts=[0],strings=[\"\"]"
                        + ",uuids=[\"" + uuid + "\"]",
                asHoconStr(basePath)
        );

        assertEquals("[false]", asHoconStr(basePath, "booleans"));
        assertEquals("[0]", asHoconStr(basePath, "bytes"));
        assertEquals("[0]", asHoconStr(basePath, "shorts"));
        assertEquals("[0]", asHoconStr(basePath, "ints"));
        assertEquals("[0]", asHoconStr(basePath, "longs"));
        assertEquals("[\"\\u0000\"]", asHoconStr(basePath, "chars"));
        assertEquals("[0]", asHoconStr(basePath, "floats"));
        assertEquals("[0.0]", asHoconStr(basePath, "doubles"));
        assertEquals("[\"\"]", asHoconStr(basePath, "strings"));
        assertEquals("[\"" + uuid + "\"]", asHoconStr(basePath, "uuids"));
    }

    /**
     * Retrieves the HOCON configuration located at the given path.
     */
    private static String asHoconStr(List<String> basePath, String... path) {
        List<String> fullPath = Stream.concat(basePath.stream(), Arrays.stream(path)).collect(Collectors.toList());

        ConfigValue hoconCfg = HoconConverter.represent(registry.superRoot(), fullPath);

        return hoconCfg.render(ConfigRenderOptions.concise().setJson(false));
    }

    @Test
    public void fromHoconBasic() {
        // Wrong names:
        assertThrowsIllegalArgException(
                () -> change("doot : {}"),
                "'doot' configuration root doesn't exist"
        );

        assertThrowsIllegalArgException(
                () -> change("root.foo : {}"),
                "'root' configuration doesn't have the 'foo' sub-configuration"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList.name.x = 1"),
                "'root.arraysList.name' configuration doesn't have the 'x' sub-configuration"
        );

        // Wrong node types:
        assertThrowsIllegalArgException(
                () -> change("root = foo"),
                "'root' is expected to be a composite configuration node, not a single value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList = foo"),
                "'root.arraysList' is expected to be a composite configuration node, not a single value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList.name = foo"),
                "'root.arraysList.name' is expected to be a composite configuration node, not a single value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList.name.ints = {}"),
                "'int[]' is expected as a type for the 'root.arraysList.name.ints' configuration value"
        );

        // Wrong ordered named list syntax:
        assertThrowsIllegalArgException(
                () -> change("root.arraysList = [1]"),
                "'root.arraysList[0]' is expected to be a composite configuration node, not a single value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList = [{}]"),
                "'root.arraysList[0].a' configuration value is mandatory and must be a String"
        );
    }

    /**
     * Tests that the {@code HoconConverter} supports deserialization of {@link String}, {@link UUID} and primitives.
     */
    @Test
    public void testHoconPrimitivesDeserialization() throws Throwable {
        change("root.primitivesList = [{p = name}]");

        HoconPrimitivesConfiguration primitives = configuration.primitivesList().get("name");
        assertNotNull(primitives);

        change("root.primitivesList.name.booleanVal = true");
        assertThat(primitives.booleanVal().value(), is(true));

        change("root.primitivesList.name.byteVal = 123");
        assertThat(primitives.byteVal().value(), is((byte) 123));

        change("root.primitivesList.name.shortVal = 12345");
        assertThat(primitives.shortVal().value(), is((short) 12345));

        change("root.primitivesList.name.intVal = 12345");
        assertThat(primitives.intVal().value(), is(12345));

        change("root.primitivesList.name.longVal = 12345678900");
        assertThat(primitives.longVal().value(), is(12345678900L));

        change("root.primitivesList.name.charVal = p");
        assertThat(primitives.charVal().value(), is('p'));

        change("root.primitivesList.name.floatVal = 2.5");
        assertThat(primitives.floatVal().value(), is(2.5f));

        change("root.primitivesList.name.doubleVal = 2.5");
        assertThat(primitives.doubleVal().value(), is(2.5d));

        change("root.primitivesList.name.stringVal = foo");
        assertThat(primitives.stringVal().value(), is("foo"));

        UUID newUuid0 = UUID.randomUUID();
        UUID newUuid1 = UUID.randomUUID();

        change("root.primitivesList.name.uuidVal = " + newUuid0);
        assertThat(primitives.uuidVal().value(), is(newUuid0));

        change("root.primitivesList.name.uuidVal = \"" + newUuid1 + "\"");
        assertThat(primitives.uuidVal().value(), is(newUuid1));
    }

    /**
     * Tests deserialization errors that can happen during the deserialization of primitives.
     */
    @Test
    public void testInvalidHoconPrimitivesDeserialization() {
        assertThrowsIllegalArgException(
                () -> change("root.primitivesList.name.booleanVal = \"true\""),
                "'boolean' is expected as a type for the 'root.primitivesList.name.booleanVal' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.primitivesList.name.byteVal = 290"),
                "Value '290' of 'root.primitivesList.name.byteVal' is out of its declared bounds"
        );

        assertThrowsIllegalArgException(
                () -> change("root.primitivesList.name.byteVal = false"),
                "'byte' is expected as a type for the 'root.primitivesList.name.byteVal' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.primitivesList.name.shortVal = 12345678900"),
                "Value '12345678900' of 'root.primitivesList.name.shortVal' is out of its declared bounds"
        );

        assertThrowsIllegalArgException(
                () -> change("root.primitivesList.name.shortVal = false"),
                "'short' is expected as a type for the 'root.primitivesList.name.shortVal' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.primitivesList.name.intVal = 12345678900"),
                "Value '12345678900' of 'root.primitivesList.name.intVal' is out of its declared bounds"
        );

        assertThrowsIllegalArgException(
                () -> change("root.primitivesList.name.intVal = false"),
                "'int' is expected as a type for the 'root.primitivesList.name.intVal' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.primitivesList.name.longVal = false"),
                "'long' is expected as a type for the 'root.primitivesList.name.longVal' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.primitivesList.name.charVal = 10"),
                "'char' is expected as a type for the 'root.primitivesList.name.charVal' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.primitivesList.name.floatVal = false"),
                "'float' is expected as a type for the 'root.primitivesList.name.floatVal' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.primitivesList.name.doubleVal = []"),
                "'double' is expected as a type for the 'root.primitivesList.name.doubleVal' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.primitivesList.name.stringVal = 10"),
                "'String' is expected as a type for the 'root.primitivesList.name.stringVal' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.primitivesList.name.uuidVal = 123"),
                "'UUID' is expected as a type for the 'root.primitivesList.name.uuidVal' configuration value"
        );
    }

    /**
     * Tests that the {@code HoconConverter} supports deserialization of arrays of {@link String}, {@link UUID} and primitives.
     */
    @Test
    public void testHoconArraysDeserialization() throws Throwable {
        change("root.arraysList = [{a = name}]");

        HoconArraysConfiguration arrays = configuration.arraysList().get("name");
        assertNotNull(arrays);

        change("root.arraysList.name.booleans = [true]");
        assertThat(arrays.booleans().value(), is(new boolean[]{true}));

        change("root.arraysList.name.bytes = [123]");
        assertThat(arrays.bytes().value(), is(new byte[]{123}));

        change("root.arraysList.name.shorts = [123]");
        assertThat(arrays.shorts().value(), is(new short[]{123}));

        change("root.arraysList.name.ints = [12345]");
        assertThat(arrays.ints().value(), is(new int[]{12345}));

        change("root.arraysList.name.longs = [12345678900]");
        assertThat(arrays.longs().value(), is(new long[]{12345678900L}));

        change("root.arraysList.name.chars = [p]");
        assertThat(arrays.chars().value(), is(new char[]{'p'}));

        change("root.arraysList.name.floats = [2.5]");
        assertThat(arrays.floats().value(), is(new float[]{2.5f}));

        change("root.arraysList.name.doubles = [2.5]");
        assertThat(arrays.doubles().value(), is(new double[]{2.5d}));

        change("root.arraysList.name.strings = [foo]");
        assertThat(arrays.strings().value(), is(new String[]{"foo"}));

        UUID newUuid0 = UUID.randomUUID();
        UUID newUuid1 = UUID.randomUUID();

        change("root.arraysList.name.uuids = [" + newUuid0 + "]");
        assertThat(arrays.uuids().value(), is(new UUID[]{newUuid0}));

        change("root.arraysList.name.uuids = [\"" + newUuid1 + "\"]");
        assertThat(arrays.uuids().value(), is(new UUID[]{newUuid1}));
    }

    /**
     * Tests deserialization errors that can happen during the deserialization of arrays of primitives.
     */
    @Test
    public void testInvalidHoconArraysDeserialization() {
        assertThrowsIllegalArgException(
                () -> change("root.arraysList.name.booleans = true"),
                "'boolean[]' is expected as a type for the 'root.arraysList.name.booleans' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList.name.booleans = [{}]"),
                "'boolean' is expected as a type for the 'root.arraysList.name.booleans[0]' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList.name.bytes = [123, 290]"),
                "Value '290' of 'root.arraysList.name.bytes[1]' is out of its declared bounds"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList.name.bytes = false"),
                "'byte[]' is expected as a type for the 'root.arraysList.name.bytes' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList.name.shorts = [12345678900]"),
                "Value '12345678900' of 'root.arraysList.name.shorts[0]' is out of its declared bounds"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList.name.shorts = [123, false]"),
                "'short' is expected as a type for the 'root.arraysList.name.shorts[1]' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList.name.ints = [5, 12345678900]"),
                "Value '12345678900' of 'root.arraysList.name.ints[1]' is out of its declared bounds"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList.name.ints = false"),
                "'int[]' is expected as a type for the 'root.arraysList.name.ints' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList.name.longs = [foo]"),
                "'long' is expected as a type for the 'root.arraysList.name.longs[0]' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList.name.chars = 10"),
                "'char[]' is expected as a type for the 'root.arraysList.name.chars' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList.name.chars = [abc]"),
                "'char' is expected as a type for the 'root.arraysList.name.chars[0]' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList.name.floats = [1.2, foo]"),
                "'float' is expected as a type for the 'root.arraysList.name.floats[1]' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList.name.doubles = foo"),
                "'double[]' is expected as a type for the 'root.arraysList.name.doubles' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList.name.strings = 10"),
                "'String[]' is expected as a type for the 'root.arraysList.name.strings' configuration value"
        );

        assertThrowsIllegalArgException(
                () -> change("root.arraysList.name.uuids = uuids"),
                "'UUID[]' is expected as a type for the 'root.arraysList.name.uuids' configuration value"
        );
    }

    @Test
    void testPolymorphicConfig() throws Throwable {
        // Check defaults.
        assertEquals("arraysList=[],polymorphicCfg=[],primitivesList=[]", asHoconStr(List.of("root")));

        // Check change type.
        change("root.polymorphicCfg = [{poly = name, typeId = second}]");

        assertInstanceOf(HoconSecondPolymorphicInstanceConfiguration.class, configuration.polymorphicCfg().get("name"));
        assertEquals("arraysList=[],polymorphicCfg=[{intVal=0,poly=name,typeId=second}],primitivesList=[]", asHoconStr(List.of("root")));

        // Check change field.
        change("root.polymorphicCfg.name.intVal = 10");
        assertEquals("arraysList=[],polymorphicCfg=[{intVal=10,poly=name,typeId=second}],primitivesList=[]", asHoconStr(List.of("root")));

        // Check error: unknown typeId.
        assertThrowsIllegalArgException(
                () -> change("root.polymorphicCfg.name.typeId = ERROR"),
                "Polymorphic configuration type is not correct: ERROR"
        );

        // Check error: try update field from typeId = first.
        assertThrowsIllegalArgException(
                () -> change("root.polymorphicCfg.name.longVal = 10"),
                "'root.polymorphicCfg.name' configuration doesn't have the 'longVal' sub-configuration"
        );
    }

    @Test
    void testInjectedName() throws Throwable {
        // Check defaults.
        assertEquals("nested{},nestedNamed=[]", asHoconStr(List.of("rootInjectedName")));

        // Checks get/set field with @InjectedName for nested config.
        assertThrowsIllegalArgException(
                () -> change("rootInjectedName.nested.someName = testName"),
                "rootInjectedName.nested' configuration doesn't have the 'someName' sub-configuration"
        );

        assertThrowsIllegalArgException(
                () -> asHoconStr(List.of("rootInjectedName"), "nested", "someName"),
                "Configuration value 'rootInjectedName.nested.someName' has not been found"
        );

        // Checks get/set field with @InjectedName for nested named config.
        change("rootInjectedName.nestedNamed = [{someName = foo}]");

        assertEquals("nested{},nestedNamed=[{someName=foo}]", asHoconStr(List.of("rootInjectedName")));
        assertEquals("[someName=foo]", asHoconStr(List.of("rootInjectedName", "nestedNamed")));
        assertEquals("{}", asHoconStr(List.of("rootInjectedName", "nestedNamed", "foo")));

        // Let's check that the NamedConfigValue#syntheticKeyName key will not work.
        assertThrowsIllegalArgException(
                () -> change("rootInjectedName.nestedNamed = [{superName = foo}]"),
                "'rootInjectedName.nestedNamed[0].someName' configuration value is mandatory and must be a String"
        );
    }

    /**
     * Updates the configuration using the provided HOCON string.
     */
    private static void change(String hocon) throws Throwable {
        try {
            registry.change(hoconSource(ConfigFactory.parseString(hocon).root())).get(1, SECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();

            if (cause instanceof ConfigurationChangeException) {
                throw cause.getCause();
            } else {
                throw cause;
            }
        }
    }

    private static void assertThrowsIllegalArgException(Executable executable, String msg) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, executable);

        assertThat(e.getMessage(), containsString(msg));
    }
}
