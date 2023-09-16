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

package org.apache.ignite.internal.schema.marshaller;

import static org.apache.ignite.internal.schema.DefaultValueProvider.constantProvider;
import static org.apache.ignite.internal.schema.NativeTypes.BOOLEAN;
import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.DATE;
import static org.apache.ignite.internal.schema.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.schema.NativeTypes.FLOAT;
import static org.apache.ignite.internal.schema.NativeTypes.INT16;
import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.INT64;
import static org.apache.ignite.internal.schema.NativeTypes.INT8;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.apache.ignite.internal.schema.NativeTypes.UUID;
import static org.apache.ignite.internal.schema.NativeTypes.datetime;
import static org.apache.ignite.internal.schema.NativeTypes.time;
import static org.apache.ignite.internal.schema.NativeTypes.timestamp;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.facebook.presto.bytecode.Access;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.ClassGenerator;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.processing.Generated;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.internal.marshaller.SerializingConverter;
import org.apache.ignite.internal.marshaller.testobjects.TestObjectWithAllTypes;
import org.apache.ignite.internal.marshaller.testobjects.TestObjectWithNoDefaultConstructor;
import org.apache.ignite.internal.marshaller.testobjects.TestObjectWithPrivateConstructor;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.schema.marshaller.asm.AsmMarshallerGenerator;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.testobjects.TestSimpleObjectKey;
import org.apache.ignite.internal.schema.testobjects.TestSimpleObjectVal;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.ObjectFactory;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * KvMarshaller test.
 */
public class KvMarshallerTest {
    /**
     * Return list of marshaller factories for test.
     */
    private static List<MarshallerFactory> marshallerFactoryProvider() {
        return List.of(
                new ReflectionMarshallerFactory(),
                new AsmMarshallerGenerator()
        );
    }

    /** Random. */
    private Random rnd;

    /**
     * Init test.
     */
    @BeforeEach
    public void initRandom() {
        long seed = System.currentTimeMillis();

        System.out.println("Using seed: " + seed + "L;");

        rnd = new Random(seed);
    }

    @TestFactory
    public Stream<DynamicNode> basicTypes() {
        NativeType[] types = {BOOLEAN, INT8, INT16, INT32, INT64, FLOAT, DOUBLE, UUID, STRING, BYTES,
                NativeTypes.bitmaskOf(5), NativeTypes.numberOf(42), NativeTypes.decimalOf(12, 3)};

        return marshallerFactoryProvider().stream().map(factory ->
                dynamicContainer(
                        factory.getClass().getSimpleName(),
                        Stream.concat(
                                // Test pure types.
                                Stream.of(types).map(type ->
                                        dynamicTest("testBasicTypes(" + type.spec().name() + ')', () -> checkBasicType(factory, type, type))
                                ),

                                // Test pairs of mixed types.
                                Stream.of(
                                        dynamicTest("testMixTypes 1", () -> checkBasicType(factory, INT64, INT32)),
                                        dynamicTest("testMixTypes 1", () -> checkBasicType(factory, FLOAT, DOUBLE)),
                                        dynamicTest("testMixTypes 1", () -> checkBasicType(factory, INT32, BYTES)),
                                        dynamicTest("testMixTypes 1", () -> checkBasicType(factory, STRING, INT64)),
                                        dynamicTest("testMixTypes 1", () -> checkBasicType(factory, NativeTypes.bitmaskOf(9), BYTES)),
                                        dynamicTest("testMixTypes 1", () -> checkBasicType(factory, NativeTypes.numberOf(12), BYTES)),
                                        dynamicTest("testMixTypes 1", () -> checkBasicType(factory, NativeTypes.decimalOf(12, 3), BYTES))
                                )
                        )
                ));
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void pojoWithFieldsOfAllTypes(MarshallerFactory factory) throws MarshallerException {
        SchemaDescriptor schema = new SchemaDescriptor(1, columnsAllTypes(false), columnsAllTypes(true));

        final TestObjectWithAllTypes key = TestObjectWithAllTypes.randomKey(rnd);
        final TestObjectWithAllTypes val = TestObjectWithAllTypes.randomObject(rnd);

        KvMarshaller<TestObjectWithAllTypes, TestObjectWithAllTypes> marshaller =
                factory.create(schema, TestObjectWithAllTypes.class, TestObjectWithAllTypes.class);

        Row row = Row.wrapBinaryRow(schema, marshaller.marshal(key, val));

        TestObjectWithAllTypes restoredVal = marshaller.unmarshalValue(row);
        TestObjectWithAllTypes restoredKey = marshaller.unmarshalKey(row);

        assertTrue(key.getClass().isInstance(restoredKey));
        assertTrue(val.getClass().isInstance(restoredVal));

        assertEquals(key, restoredKey);
        assertEquals(val, restoredVal);
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void narrowType(MarshallerFactory factory) {
        Assumptions.assumeFalse(factory instanceof AsmMarshallerGenerator, "Generated marshaller doesn't support truncated values, yet.");

        Column[] cols = {
                new Column("primitiveIntCol".toUpperCase(), INT32, false),
                new Column("primitiveLongCol".toUpperCase(), INT64, false),
                new Column("primitiveFloatCol".toUpperCase(), FLOAT, false),
                new Column("primitiveDoubleCol".toUpperCase(), DOUBLE, false),
                new Column("stringCol".toUpperCase(), STRING, false),
                new Column("uuidCol".toUpperCase(), UUID, false),
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, cols, columnsAllTypes(true));

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> factory.create(schema, TestTruncatedObject.class, TestTruncatedObject.class));

        assertEquals("No field found for column BOOLEANCOL", ex.getMessage());
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void wideType(MarshallerFactory factory) {
        Column[] cols = {
                new Column("primitiveLongCol".toUpperCase(), INT64, false),
                new Column("primitiveDoubleCol".toUpperCase(), DOUBLE, false),
                new Column("stringCol".toUpperCase(), STRING, false),
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, cols, cols);

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> factory.create(schema, TestObjectWithAllTypes.class, TestObjectWithAllTypes.class)
        );

        assertEquals(
                "Fields [bitmaskCol, booleanCol, byteCol, bytesCol, dateCol, dateTimeCol, decimalCol, doubleCol, floatCol, "
                        + "intCol, longCol, nullBytesCol, nullLongCol, numberCol, primitiveBooleanCol, primitiveByteCol, "
                        + "primitiveFloatCol, primitiveIntCol, primitiveShortCol, shortCol, timeCol, timestampCol, uuidCol] "
                        + "of type org.apache.ignite.internal.marshaller.testobjects.TestObjectWithAllTypes are not mapped to columns.",
                ex.getMessage());
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void columnNameMapping(MarshallerFactory factory) throws MarshallerException {
        Assumptions.assumeFalse(factory instanceof AsmMarshallerGenerator, "Generated marshaller doesn't support column mapping, yet.");

        SchemaDescriptor schema = new SchemaDescriptor(1,
                new Column[]{new Column("key".toUpperCase(), INT64, false)},
                new Column[]{
                        new Column("col1".toUpperCase(), INT64, false),
                        new Column("col3".toUpperCase(), STRING, false)
                });

        Mapper<TestKeyObject> keyMapper = Mapper.builder(TestKeyObject.class)
                .map("id", "key")
                .build();

        Mapper<TestObject> valMapper = Mapper.builder(TestObject.class)
                .map("longCol", "col1")
                .map("stringCol", "col3")
                .build();

        KvMarshaller<TestKeyObject, TestObject> marshaller = factory.create(schema, keyMapper, valMapper);

        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestObject val = TestObject.randomObject(rnd);

        Row row = Row.wrapBinaryRow(schema, marshaller.marshal(key, val));

        Object restoredVal = marshaller.unmarshalValue(row);
        Object restoredKey = marshaller.unmarshalKey(row);

        assertTrue(key.getClass().isInstance(restoredKey));
        assertTrue(val.getClass().isInstance(restoredVal));

        val.longCol2 = null;

        assertEquals(key, restoredKey);
        assertEquals(val, restoredVal);
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classWithWrongFieldType(MarshallerFactory factory) {
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{
                        new Column("longCol".toUpperCase(), NativeTypes.bitmaskOf(42), false),
                        new Column("intCol".toUpperCase(), UUID, false)
                },
                new Column[]{
                        new Column("bytesCol".toUpperCase(), NativeTypes.bitmaskOf(42), true),
                        new Column("stringCol".toUpperCase(), UUID, true)
                }
        );

        Throwable ex = assertThrows(
                ClassCastException.class,
                () -> factory.create(schema, TestSimpleObjectKey.class, TestSimpleObjectVal.class));

        assertThat(
                ex.getMessage(),
                containsString("Column's type mismatch [column=LONGCOL, expectedType=BITSET, actualType=class java.lang.Long]")
        );
    }

    /**
     * Try to create marshaller for class without field for key column.
     */
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classWithoutKeyField(MarshallerFactory factory) {
        Column[] keyCols = {
                new Column("id".toUpperCase(), INT64, false),
                new Column("id2".toUpperCase(), INT64, false),
        };

        Column[] valCols = {
                new Column("primitiveDoubleCol", DOUBLE, false)
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, keyCols, valCols);

        assertThrows(IllegalArgumentException.class, () -> factory.create(schema, TestKeyObject.class, TestObjectWithAllTypes.class));
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classWithIncorrectBitmaskSize(MarshallerFactory factory) {
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{ new Column("key".toUpperCase(), INT32, false) },
                new Column[]{ new Column("bitmaskCol".toUpperCase(), NativeTypes.bitmaskOf(9), true) }
        );

        KvMarshaller<Integer, BitSet> marshaller =
                factory.create(schema, Integer.class, BitSet.class);

        Throwable ex = ExceptionUtils.unwrapRootCause(assertThrows(
                MarshallerException.class,
                () -> marshaller.marshal(1, IgniteTestUtils.randomBitSet(rnd, 42))));

        assertThat(ex.getMessage(), startsWith("Failed to set bitmask for column 'BITMASKCOL' (mask size exceeds allocated size)"));
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classWithPrivateConstructor(MarshallerFactory factory) throws MarshallerException {
        Column[] cols = {
                new Column("primLongCol".toUpperCase(), INT64, false),
                new Column("primIntCol".toUpperCase(), INT32, false),
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, cols, cols);

        KvMarshaller<TestObjectWithPrivateConstructor, TestObjectWithPrivateConstructor> marshaller =
                factory.create(schema, TestObjectWithPrivateConstructor.class, TestObjectWithPrivateConstructor.class);

        final TestObjectWithPrivateConstructor key = TestObjectWithPrivateConstructor.randomObject(rnd);
        final TestObjectWithPrivateConstructor val = TestObjectWithPrivateConstructor.randomObject(rnd);

        Row row = Row.wrapBinaryRow(schema, marshaller.marshal(key, val));

        Object key1 = marshaller.unmarshalKey(row);
        Object val1 = marshaller.unmarshalValue(row);

        assertTrue(key.getClass().isInstance(key1));
        assertTrue(val.getClass().isInstance(val1));

        assertEquals(key, key);
        assertEquals(val, val1);
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classWithNoDefaultConstructor(MarshallerFactory factory) {
        Column[] cols = {
                new Column("primLongCol", INT64, false),
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, cols, cols);

        final Object key = TestObjectWithNoDefaultConstructor.randomObject(rnd);
        final Object val = TestObjectWithNoDefaultConstructor.randomObject(rnd);

        assertThrows(IllegalArgumentException.class, () -> factory.create(schema, key.getClass(), val.getClass()));
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void privateClass(MarshallerFactory factory) throws MarshallerException {
        Column[] cols = {
                new Column("primLongCol".toUpperCase(), INT64, false),
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, cols, cols);

        final ObjectFactory<PrivateTestObject> objFactory = new ObjectFactory<>(PrivateTestObject.class);
        final KvMarshaller<PrivateTestObject, PrivateTestObject> marshaller =
                factory.create(schema, PrivateTestObject.class, PrivateTestObject.class);

        final PrivateTestObject key = PrivateTestObject.randomObject(rnd);
        final PrivateTestObject val = PrivateTestObject.randomObject(rnd);

        Row row = Row.wrapBinaryRow(schema, marshaller.marshal(key, objFactory.create()));

        Object key1 = marshaller.unmarshalKey(row);
        Object val1 = marshaller.unmarshalValue(row);

        assertTrue(key.getClass().isInstance(key1));
        assertTrue(val.getClass().isInstance(val1));
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classLoader(MarshallerFactory factory) throws MarshallerException {
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(new DynamicClassLoader(getClass().getClassLoader()));

            Column[] keyCols = {
                    new Column("key".toUpperCase(), INT64, false)
            };

            Column[] valCols = {
                    new Column("col0".toUpperCase(), INT64, false),
                    new Column("col1".toUpperCase(), INT64, false),
                    new Column("col2".toUpperCase(), INT64, false),
            };

            SchemaDescriptor schema = new SchemaDescriptor(1, keyCols, valCols);

            final Class<?> valClass = createGeneratedObjectClass();
            final ObjectFactory<?> objFactory = new ObjectFactory<>(valClass);

            KvMarshaller<Long, Object> marshaller = factory.create(schema, Long.class, (Class<Object>) valClass);

            final Long key = rnd.nextLong();

            Row row = Row.wrapBinaryRow(schema, marshaller.marshal(key, objFactory.create()));

            Long key1 = marshaller.unmarshalKey(row);
            Object val1 = marshaller.unmarshalValue(row);

            assertTrue(valClass.isInstance(val1));

            assertEquals(key, key1);
        } finally {
            Thread.currentThread().setContextClassLoader(loader);
        }
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void pojoMapping(MarshallerFactory factory) throws MarshallerException, IOException {
        Assumptions.assumeFalse(factory instanceof AsmMarshallerGenerator, "Generated marshaller doesn't support column mapping.");

        final SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{new Column("key", INT64, false)},
                new Column[]{new Column("val", BYTES, true),
                });

        final TestPojo pojo = new TestPojo(42);
        final byte[] serializedPojo = serializeObject(pojo);

        final KvMarshaller<Long, TestPojo> marshaller1 = factory.create(schema,
                Mapper.of(Long.class, "\"key\""),
                Mapper.of(TestPojo.class, "\"val\"", new SerializingConverter<>()));

        final KvMarshaller<Long, byte[]> marshaller2 = factory.create(schema,
                Mapper.of(Long.class, "\"key\""),
                Mapper.of(byte[].class, "\"val\""));

        final KvMarshaller<Long, TestPojoWrapper> marshaller3 = factory.create(schema,
                Mapper.of(Long.class, "\"key\""),
                Mapper.builder(TestPojoWrapper.class).map("pojoField", "\"val\"", new SerializingConverter<>()).build());

        final KvMarshaller<Long, TestPojoWrapper> marshaller4 = factory.create(schema,
                Mapper.of(Long.class, "\"key\""),
                Mapper.builder(TestPojoWrapper.class).map("rawField", "\"val\"").build());

        BinaryRow binaryRow = marshaller1.marshal(1L, pojo);
        BinaryRow binaryRow2 = marshaller2.marshal(1L, serializedPojo);
        BinaryRow binaryRow3 = marshaller3.marshal(1L, new TestPojoWrapper(pojo));
        BinaryRow binaryRow4 = marshaller4.marshal(1L, new TestPojoWrapper(serializedPojo));

        // Verify all rows are equivalent.
        assertEquals(binaryRow, binaryRow2);
        assertEquals(binaryRow, binaryRow3);
        assertEquals(binaryRow, binaryRow4);

        Row row = Row.wrapBinaryRow(schema, binaryRow);

        // Check key.
        assertEquals(1L, marshaller1.unmarshalKey(row));
        assertEquals(1L, marshaller2.unmarshalKey(row));
        assertEquals(1L, marshaller3.unmarshalKey(row));
        assertEquals(1L, marshaller4.unmarshalKey(row));

        // Check values.
        assertEquals(pojo, marshaller1.unmarshalValue(row));
        assertArrayEquals(serializedPojo, marshaller2.unmarshalValue(row));
        assertEquals(new TestPojoWrapper(pojo), marshaller3.unmarshalValue(row));
        assertEquals(new TestPojoWrapper(serializedPojo), marshaller4.unmarshalValue(row));
    }

    private byte[] serializeObject(TestPojo obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(512);

        try (ObjectOutputStream dos = new ObjectOutputStream(baos)) {
            dos.writeObject(obj);
        }

        return baos.toByteArray();
    }

    /**
     * Generate random key-value pair of given types and check serialization and deserialization works fine.
     *
     * @param factory KvMarshaller factory.
     * @param keyType Key type.
     * @param valType Value type.
     * @throws MarshallerException If (de)serialization failed.
     */
    private void checkBasicType(MarshallerFactory factory, NativeType keyType,
            NativeType valType) throws MarshallerException {
        final Object key = generateRandomValue(keyType);
        final Object val = generateRandomValue(valType);

        Column[] keyCols = {new Column("key", keyType, false)};
        Column[] valCols = {new Column("val", valType, false)};

        SchemaDescriptor schema = new SchemaDescriptor(1, keyCols, valCols);

        KvMarshaller<Object, Object> marshaller = factory.create(schema,
                Mapper.of((Class<Object>) key.getClass(), "\"key\""),
                Mapper.of((Class<Object>) val.getClass(), "\"val\""));
        Row row = Row.wrapBinaryRow(schema, marshaller.marshal(key, val));

        Object key1 = marshaller.unmarshalKey(row);
        Object val1 = marshaller.unmarshalValue(row);

        assertTrue(key.getClass().isInstance(key1));
        assertTrue(val.getClass().isInstance(val1));

        compareObjects(keyType, key, key);
        compareObjects(valType, val, val1);
    }

    /**
     * Compare object regarding NativeType.
     *
     * @param type Native type.
     * @param exp  Expected value.
     * @param act  Actual value.
     */
    private void compareObjects(NativeType type, Object exp, Object act) {
        if (type.spec() == NativeTypeSpec.BYTES) {
            assertArrayEquals((byte[]) exp, (byte[]) act);
        } else {
            assertEquals(exp, act);
        }
    }

    /**
     * Generates random value of given type.
     *
     * @param type Type.
     */
    private Object generateRandomValue(NativeType type) {
        return SchemaTestUtils.generateRandomValue(rnd, type);
    }

    /**
     * Generate class for test objects.
     *
     * @return Generated test object class.
     */
    private Class<?> createGeneratedObjectClass() {
        final String packageName = getClass().getPackageName();
        final String className = "GeneratedTestObject";

        final ClassDefinition classDef = new ClassDefinition(
                EnumSet.of(Access.PUBLIC),
                packageName.replace('.', '/') + '/' + className,
                ParameterizedType.type(Object.class)
        );
        classDef.declareAnnotation(Generated.class).setValue("value", getClass().getCanonicalName());

        for (int i = 0; i < 3; i++) {
            classDef.declareField(EnumSet.of(Access.PRIVATE), "col" + i, ParameterizedType.type(long.class));
        }

        // Build constructor.
        final MethodDefinition methodDef = classDef.declareConstructor(EnumSet.of(Access.PUBLIC));
        final Variable rnd = methodDef.getScope().declareVariable(Random.class, "rnd");

        BytecodeBlock body = methodDef.getBody()
                .append(methodDef.getThis())
                .invokeConstructor(classDef.getSuperClass())
                .append(rnd.set(BytecodeExpressions.newInstance(Random.class)));

        for (int i = 0; i < 3; i++) {
            body.append(methodDef.getThis().setField("col" + i, rnd.invoke("nextLong", long.class).cast(long.class)));
        }

        body.ret();

        return ClassGenerator.classGenerator(Thread.currentThread().getContextClassLoader())
                .fakeLineNumbers(true)
                .runAsmVerifier(true)
                .dumpRawBytecode(true)
                .defineClass(classDef, Object.class);
    }

    private Column[] columnsAllTypes(boolean nullable) {
        Column[] cols = {
                new Column("primitiveBooleanCol".toUpperCase(), BOOLEAN, false, constantProvider(true)),
                new Column("primitiveByteCol".toUpperCase(), INT8, false, constantProvider((byte) 0x42)),
                new Column("primitiveShortCol".toUpperCase(), INT16, false, constantProvider((short) 0x4242)),
                new Column("primitiveIntCol".toUpperCase(), INT32, false, constantProvider(0x42424242)),
                new Column("primitiveLongCol".toUpperCase(), INT64, false),
                new Column("primitiveFloatCol".toUpperCase(), FLOAT, false),
                new Column("primitiveDoubleCol".toUpperCase(), DOUBLE, false),

                new Column("booleanCol".toUpperCase(), BOOLEAN, nullable),
                new Column("byteCol".toUpperCase(), INT8, nullable),
                new Column("shortCol".toUpperCase(), INT16, nullable),
                new Column("intCol".toUpperCase(), INT32, nullable),
                new Column("longCol".toUpperCase(), INT64, nullable),
                new Column("nullLongCol".toUpperCase(), INT64, nullable),
                new Column("floatCol".toUpperCase(), FLOAT, nullable),
                new Column("doubleCol".toUpperCase(), DOUBLE, nullable),

                new Column("dateCol".toUpperCase(), DATE, nullable),
                new Column("timeCol".toUpperCase(), time(), nullable),
                new Column("dateTimeCol".toUpperCase(), datetime(), nullable),
                new Column("timestampCol".toUpperCase(), timestamp(), nullable),

                new Column("uuidCol".toUpperCase(), UUID, nullable),
                new Column("bitmaskCol".toUpperCase(), NativeTypes.bitmaskOf(42), nullable),
                new Column("stringCol".toUpperCase(), STRING, nullable),
                new Column("nullBytesCol".toUpperCase(), BYTES, nullable),
                new Column("bytesCol".toUpperCase(), BYTES, nullable),
                new Column("numberCol".toUpperCase(), NativeTypes.numberOf(12), nullable),
                new Column("decimalCol".toUpperCase(), NativeTypes.decimalOf(19, 3), nullable),
        };
        // Validate all types are tested.
        Set<NativeTypeSpec> testedTypes = Arrays.stream(cols).map(c -> c.type().spec())
                .collect(Collectors.toSet());
        Set<NativeTypeSpec> missedTypes = Arrays.stream(NativeTypeSpec.values())
                .filter(t -> !testedTypes.contains(t)).collect(Collectors.toSet());

        assertEquals(Collections.emptySet(), missedTypes);
        return cols;
    }

    /**
     * Test object.
     */
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    public static class TestKeyObject {
        static TestKeyObject randomObject(Random rnd) {
            final TestKeyObject obj = new TestKeyObject();

            obj.id = rnd.nextLong();

            return obj;
        }

        private long id;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestKeyObject that = (TestKeyObject) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }


    /**
     * Test object.
     */
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    public static class TestObject {
        static TestObject randomObject(Random rnd) {
            final TestObject obj = new TestObject();

            obj.longCol = rnd.nextLong();
            obj.longCol2 = rnd.nextLong();
            obj.stringCol = IgniteTestUtils.randomString(rnd, 100);

            return obj;
        }

        private long longCol;

        private Long longCol2;

        private String stringCol;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestObject that = (TestObject) o;

            return longCol == that.longCol
                    && Objects.equals(longCol2, that.longCol2)
                    && Objects.equals(stringCol, that.stringCol);
        }

        @Override
        public int hashCode() {
            return Objects.hash(longCol, longCol2, stringCol);
        }
    }

    /**
     * Test object.
     */
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    public static class TestTruncatedObject {
        static TestTruncatedObject randomObject(Random rnd) {
            final TestTruncatedObject obj = new TestTruncatedObject();

            obj.primitiveIntCol = rnd.nextInt();
            obj.primitiveLongCol = rnd.nextLong();
            obj.primitiveDoubleCol = rnd.nextDouble();

            obj.uuidCol = java.util.UUID.randomUUID();
            obj.stringCol = IgniteTestUtils.randomString(rnd, 100);

            return obj;
        }

        // Primitive typed
        private int primitiveIntCol;

        private long primitiveLongCol;

        private float primitiveFloatCol;

        private double primitiveDoubleCol;

        private String stringCol;

        private java.util.UUID uuidCol;

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestTruncatedObject object = (TestTruncatedObject) o;

            return primitiveIntCol == object.primitiveIntCol
                    && primitiveLongCol == object.primitiveLongCol
                    && Float.compare(object.primitiveFloatCol, primitiveFloatCol) == 0
                    && Double.compare(object.primitiveDoubleCol, primitiveDoubleCol) == 0
                    && Objects.equals(stringCol, ((TestTruncatedObject) o).stringCol)
                    && Objects.equals(uuidCol, ((TestTruncatedObject) o).uuidCol);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return 42;
        }
    }

    /**
     * Test object without default constructor.
     */
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    private static class PrivateTestObject {
        /**
         * Get random TestObject.
         */
        static PrivateTestObject randomObject(Random rnd) {
            return new PrivateTestObject(rnd.nextInt());
        }

        /** Value. */
        private long primLongCol;

        /** Constructor. */
        PrivateTestObject() {
        }

        /**
         * Private constructor.
         */
        PrivateTestObject(long val) {
            primLongCol = val;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PrivateTestObject object = (PrivateTestObject) o;

            return primLongCol == object.primLongCol;
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Objects.hash(primLongCol);
        }
    }

    /**
     * Test object represents a user object of arbitrary type.
     */
    static class TestPojo implements Serializable {
        private static final long serialVersionUID = -1L;

        int intField;

        public TestPojo() {
        }

        public TestPojo(int intVal) {
            this.intField = intVal;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestPojo testPojo = (TestPojo) o;
            return intField == testPojo.intField;
        }

        @Override
        public int hashCode() {
            return Objects.hash(intField);
        }
    }

    /**
     * Wrapper for the {@link TestPojo}.
     */
    static class TestPojoWrapper {
        TestPojo pojoField;

        byte[] rawField;

        public TestPojoWrapper() {
        }

        public TestPojoWrapper(TestPojo pojoField) {
            this.pojoField = pojoField;
        }

        public TestPojoWrapper(byte[] rawField) {
            this.rawField = rawField;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestPojoWrapper that = (TestPojoWrapper) o;
            return Objects.equals(pojoField, that.pojoField)
                    && Arrays.equals(rawField, that.rawField);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(pojoField);
            result = 31 * result + Arrays.hashCode(rawField);
            return result;
        }
    }
}
