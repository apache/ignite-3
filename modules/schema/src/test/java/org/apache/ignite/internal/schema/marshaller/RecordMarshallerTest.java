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
import static org.apache.ignite.internal.type.NativeTypes.BOOLEAN;
import static org.apache.ignite.internal.type.NativeTypes.BYTES;
import static org.apache.ignite.internal.type.NativeTypes.DATE;
import static org.apache.ignite.internal.type.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.type.NativeTypes.FLOAT;
import static org.apache.ignite.internal.type.NativeTypes.INT16;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.INT64;
import static org.apache.ignite.internal.type.NativeTypes.INT8;
import static org.apache.ignite.internal.type.NativeTypes.STRING;
import static org.apache.ignite.internal.type.NativeTypes.UUID;
import static org.apache.ignite.internal.type.NativeTypes.datetime;
import static org.apache.ignite.internal.type.NativeTypes.time;
import static org.apache.ignite.internal.type.NativeTypes.timestamp;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.presto.bytecode.Access;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.ClassGenerator;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import javax.annotation.processing.Generated;
import org.apache.ignite.internal.marshaller.testobjects.TestObjectWithAllTypes;
import org.apache.ignite.internal.marshaller.testobjects.TestObjectWithNoDefaultConstructor;
import org.apache.ignite.internal.marshaller.testobjects.TestObjectWithPrivateConstructor;
import org.apache.ignite.internal.marshaller.testobjects.TestSimpleObject;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.ObjectFactory;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.mapper.TypeConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * RecordMarshaller test.
 */
public class RecordMarshallerTest {
    /**
     * Returns list of marshaller factories for the test.
     */
    private static List<MarshallerFactory> marshallerFactoryProvider() {
        return List.of(new ReflectionMarshallerFactory());
    }

    /** Schema version. */
    private static final AtomicInteger schemaVersion = new AtomicInteger();

    /** Random. */
    private Random rnd;

    /**
     * Init random.
     */
    @BeforeEach
    public void initRandom() {
        long seed = System.currentTimeMillis();

        System.out.println("Using seed: " + seed + "L;");

        rnd = new Random(seed);
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void complexType(MarshallerFactory factory) throws MarshallerException {
        SchemaDescriptor schema = new SchemaDescriptor(schemaVersion.incrementAndGet(), keyColumns(), valueColumnsAllTypes());

        final TestObjectWithAllTypes rec = TestObjectWithAllTypes.randomObject(rnd);

        RecordMarshaller<TestObjectWithAllTypes> marshaller = factory.create(schema, TestObjectWithAllTypes.class);

        BinaryRow row = marshaller.marshal(rec);

        TestObjectWithAllTypes restoredRec = marshaller.unmarshal(Row.wrapBinaryRow(schema, row));

        assertTrue(rec.getClass().isInstance(restoredRec));

        assertEquals(rec, restoredRec);
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void truncatedType(MarshallerFactory factory) throws MarshallerException {
        SchemaDescriptor schema = new SchemaDescriptor(schemaVersion.incrementAndGet(), keyColumns(), valueColumnsAllTypes());

        RecordMarshaller<TestTruncatedObject> marshaller = factory.create(schema, TestTruncatedObject.class);

        final TestTruncatedObject rec = TestTruncatedObject.randomObject(rnd);

        BinaryRow row = marshaller.marshal(rec);

        Object restoredRec = marshaller.unmarshal(Row.wrapBinaryRow(schema, row));

        assertTrue(rec.getClass().isInstance(restoredRec));

        assertEquals(rec, restoredRec);
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void widerType(MarshallerFactory factory) {
        SchemaDescriptor schema = new SchemaDescriptor(
                schemaVersion.incrementAndGet(),
                keyColumns(),
                new Column[]{
                        new Column("primitiveDoubleCol".toUpperCase(), DOUBLE, false),
                        new Column("stringCol".toUpperCase(), STRING, true),
                }
        );

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> factory.create(schema, TestObjectWithAllTypes.class));

        assertEquals(
                "Fields [booleanCol, byteCol, bytesCol, dateCol, dateTimeCol, decimalCol, doubleCol, floatCol, "
                        + "longCol, nullBytesCol, nullLongCol, primitiveBooleanCol, primitiveByteCol, primitiveFloatCol, "
                        + "primitiveIntCol, primitiveShortCol, shortCol, timeCol, timestampCol, uuidCol] of type "
                        + "org.apache.ignite.internal.marshaller.testobjects.TestObjectWithAllTypes are not mapped to columns",
                ex.getMessage());
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void mapping(MarshallerFactory factory) throws MarshallerException {
        SchemaDescriptor schema = new SchemaDescriptor(schemaVersion.incrementAndGet(),
                new Column[]{new Column("key".toUpperCase(), INT64, false)},
                new Column[]{
                        new Column("col1".toUpperCase(), INT32, false),
                        new Column("col2".toUpperCase(), INT64, true),
                        new Column("col3".toUpperCase(), STRING, false)
                });

        Mapper<TestObject> mapper = Mapper.builder(TestObject.class)
                .map("id", "key")
                .map("intCol", "col1")
                .map("dateCol", "col3", new TestTypeConverter())
                .build();

        RecordMarshaller<TestObject> marshaller = factory.create(schema, mapper);

        final TestObject rec = TestObject.randomObject(rnd);

        BinaryRow row = marshaller.marshal(rec);

        Object restoredRec = marshaller.unmarshal(Row.wrapBinaryRow(schema, row));

        assertTrue(rec.getClass().isInstance(restoredRec));

        rec.longCol2 = null; // Nullify non-mapped field.

        assertEquals(rec, restoredRec);
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classWithWrongFieldType(MarshallerFactory factory) {
        SchemaDescriptor schema = new SchemaDescriptor(
                schemaVersion.incrementAndGet(),
                new Column[]{
                        new Column("longCol".toUpperCase(), INT32, false),
                        new Column("intCol".toUpperCase(), UUID, false)
                },
                new Column[]{
                        new Column("bytesCol".toUpperCase(), NativeTypes.blobOf(42), true),
                        new Column("stringCol".toUpperCase(), UUID, true)
                }
        );

        Throwable ex = assertThrows(ClassCastException.class, () -> factory.create(schema, TestSimpleObject.class));

        assertThat(
                ex.getMessage(),
                containsString("Column's type mismatch [column=LONGCOL, expectedType=INT, actualType=class java.lang.Long]"));
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classWithPrivateConstructor(MarshallerFactory factory) throws MarshallerException, IllegalAccessException {
        SchemaDescriptor schema = new SchemaDescriptor(
                schemaVersion.incrementAndGet(),
                new Column[]{new Column("primLongCol".toUpperCase(), INT64, false)},
                new Column[]{new Column("primIntCol".toUpperCase(), INT32, false)}
        );

        RecordMarshaller<TestObjectWithPrivateConstructor> marshaller = factory.create(schema, TestObjectWithPrivateConstructor.class);

        final TestObjectWithPrivateConstructor rec = TestObjectWithPrivateConstructor.randomObject(rnd);

        BinaryRow row = marshaller.marshal(rec);

        TestObjectWithPrivateConstructor restoredRec = marshaller.unmarshal(Row.wrapBinaryRow(schema, row));

        assertDeepEquals(TestObjectWithPrivateConstructor.class, rec, restoredRec);
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classWithNoDefaultConstructor(MarshallerFactory factory) {
        SchemaDescriptor schema = new SchemaDescriptor(
                schemaVersion.incrementAndGet(),
                new Column[]{new Column("primLongCol".toUpperCase(), INT64, false)},
                new Column[]{new Column("primIntCol".toUpperCase(), INT32, false)}
        );

        final Object rec = TestObjectWithNoDefaultConstructor.randomObject(rnd);

        assertThrows(IllegalArgumentException.class, () -> factory.create(schema, rec.getClass()));
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void privateClass(MarshallerFactory factory) throws MarshallerException {
        SchemaDescriptor schema = new SchemaDescriptor(
                schemaVersion.incrementAndGet(),
                new Column[]{new Column("primLongCol".toUpperCase(), INT64, false)},
                new Column[]{new Column("primIntCol".toUpperCase(), INT32, false)}
        );

        final ObjectFactory<PrivateTestObject> objFactory = new ObjectFactory<>(PrivateTestObject.class);
        final RecordMarshaller<PrivateTestObject> marshaller = factory
                .create(schema, PrivateTestObject.class);

        final PrivateTestObject rec = PrivateTestObject.randomObject(rnd);

        BinaryRow row = marshaller.marshal(objFactory.create());

        Object restoredRec = marshaller.unmarshal(Row.wrapBinaryRow(schema, row));

        assertTrue(rec.getClass().isInstance(restoredRec));
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classLoader(MarshallerFactory factory) throws MarshallerException, IllegalAccessException {
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(new DynamicClassLoader(getClass().getClassLoader()));

            Column[] keyCols = new Column[]{
                    new Column("key".toUpperCase(), INT64, false)
            };

            Column[] valCols = new Column[]{
                    new Column("col0".toUpperCase(), INT64, false),
                    new Column("col1".toUpperCase(), INT64, false),
                    new Column("col2".toUpperCase(), INT64, false),
            };

            SchemaDescriptor schema = new SchemaDescriptor(schemaVersion.incrementAndGet(), keyCols, valCols);

            final Class<Object> recClass = (Class<Object>) createGeneratedObjectClass();
            final ObjectFactory<Object> objFactory = new ObjectFactory<>(recClass);

            RecordMarshaller<Object> marshaller = factory.create(schema, recClass);

            Object rec = objFactory.create();

            BinaryRow row = marshaller.marshal(rec);

            Object restoredRec = marshaller.unmarshal(Row.wrapBinaryRow(schema, row));

            assertDeepEquals(recClass, rec, restoredRec);
        } finally {
            Thread.currentThread().setContextClassLoader(loader);
        }
    }

    /**
     * Validate all types are tested.
     */
    @Test
    public void ensureAllTypesChecked() {
        SchemaTestUtils.ensureAllTypesChecked(Stream.concat(Arrays.stream(keyColumns()), Arrays.stream(valueColumnsAllTypes())));
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

        classDef.declareField(EnumSet.of(Access.PRIVATE), "key", ParameterizedType.type(long.class));

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

        body.append(methodDef.getThis().setField("key", rnd.invoke("nextLong", long.class).cast(long.class)));

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

    private <T> void assertDeepEquals(Class<T> recClass, T rec, T restoredRec) throws IllegalAccessException {
        assertTrue(recClass.isInstance(restoredRec));

        for (Field fld : recClass.getDeclaredFields()) {
            fld.setAccessible(true);
            assertEquals(fld.get(rec), fld.get(restoredRec), fld.getName());
        }
    }

    private Column[] keyColumns() {
        return new Column[]{
                new Column("primitiveLongCol".toUpperCase(), INT64, false),
                new Column("intCol".toUpperCase(), INT32, false)
        };
    }

    private Column[] valueColumnsAllTypes() {
        return new Column[]{
                new Column("primitiveBooleanCol".toUpperCase(), BOOLEAN, false, constantProvider(true)),
                new Column("primitiveByteCol".toUpperCase(), INT8, false, constantProvider((byte) 0x42)),
                new Column("primitiveShortCol".toUpperCase(), INT16, false, constantProvider((short) 0x4242)),
                new Column("primitiveIntCol".toUpperCase(), INT32, false, constantProvider(0x42424242)),
                new Column("primitiveFloatCol".toUpperCase(), FLOAT, false),
                new Column("primitiveDoubleCol".toUpperCase(), DOUBLE, false),

                new Column("booleanCol".toUpperCase(), BOOLEAN, true),
                new Column("byteCol".toUpperCase(), INT8, true),
                new Column("shortCol".toUpperCase(), INT16, true),
                new Column("longCol".toUpperCase(), INT64, true),
                new Column("nullLongCol".toUpperCase(), INT64, true),
                new Column("floatCol".toUpperCase(), FLOAT, true),
                new Column("doubleCol".toUpperCase(), DOUBLE, true),

                new Column("dateCol".toUpperCase(), DATE, true),
                new Column("timeCol".toUpperCase(), time(0), true),
                new Column("dateTimeCol".toUpperCase(), datetime(6), true),
                new Column("timestampCol".toUpperCase(), timestamp(6), true),

                new Column("uuidCol".toUpperCase(), UUID, true),
                new Column("stringCol".toUpperCase(), STRING, true),
                new Column("nullBytesCol".toUpperCase(), BYTES, true),
                new Column("bytesCol".toUpperCase(), BYTES, true),
                new Column("decimalCol".toUpperCase(), NativeTypes.decimalOf(19, 3), true),
        };
    }

    /**
     * Test object.
     */
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    public static class TestObject {
        private long id;

        private int intCol;

        private Long longCol2;

        private LocalDate dateCol;

        static TestObject randomObject(Random rnd) {
            final TestObject obj = new TestObject();

            obj.id = rnd.nextLong();
            obj.intCol = rnd.nextInt();
            obj.longCol2 = rnd.nextLong();
            obj.dateCol = LocalDate.now();

            return obj;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestObject that = (TestObject) o;

            return id == that.id
                    && intCol == that.intCol
                    && Objects.equals(longCol2, that.longCol2)
                    && Objects.equals(dateCol, that.dateCol);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    /**
     * Test object with less amount of fields.
     */
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    public static class TestTruncatedObject {
        private Integer intCol;

        // Primitive typed
        private int primitiveIntCol;

        private long primitiveLongCol;

        private float primitiveFloatCol;

        private double primitiveDoubleCol;

        private String stringCol;

        private java.util.UUID uuidCol;

        static TestTruncatedObject randomObject(Random rnd) {
            final TestTruncatedObject obj = new TestTruncatedObject();

            obj.intCol = rnd.nextInt();

            obj.primitiveIntCol = rnd.nextInt();
            obj.primitiveLongCol = rnd.nextLong();
            obj.primitiveFloatCol = rnd.nextFloat();
            obj.primitiveDoubleCol = rnd.nextDouble();

            obj.uuidCol = java.util.UUID.randomUUID();
            obj.stringCol = IgniteTestUtils.randomString(rnd, 100);

            return obj;
        }

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
                    && Objects.equals(stringCol, object.stringCol)
                    && Objects.equals(uuidCol, object.uuidCol)
                    && Objects.equals(intCol, object.intCol);
        }

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
        static PrivateTestObject randomObject(Random rnd) {
            return new PrivateTestObject(rnd.nextLong(), rnd.nextInt());
        }

        private long primLongCol;

        private int primIntCol;

        PrivateTestObject() {
        }

        PrivateTestObject(long longVal, int intVal) {
            primLongCol = longVal;
            primIntCol = intVal;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PrivateTestObject object = (PrivateTestObject) o;

            return primLongCol == object.primLongCol && primIntCol == object.primIntCol;
        }

        @Override
        public int hashCode() {
            return Objects.hash(primLongCol);
        }
    }

    private static class TestTypeConverter implements TypeConverter<LocalDate, String> {
        @Override
        public String toColumnType(LocalDate val) {
            return val.toString();
        }

        @Override
        public LocalDate toObjectType(String val) {
            return LocalDate.parse(val);
        }
    }
}
