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

package org.apache.ignite.internal.schema.marshaller;

import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;
import javax.lang.model.element.Modifier;
import org.apache.ignite.internal.schema.Bitmask;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.TestUtils;
import org.apache.ignite.internal.schema.marshaller.generator.SerializerGenerator;
import org.apache.ignite.internal.schema.marshaller.reflection.JavaSerializerFactory;
import org.apache.ignite.internal.util.ObjectFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.ignite.internal.schema.NativeType.BYTE;
import static org.apache.ignite.internal.schema.NativeType.BYTES;
import static org.apache.ignite.internal.schema.NativeType.DOUBLE;
import static org.apache.ignite.internal.schema.NativeType.FLOAT;
import static org.apache.ignite.internal.schema.NativeType.INTEGER;
import static org.apache.ignite.internal.schema.NativeType.LONG;
import static org.apache.ignite.internal.schema.NativeType.SHORT;
import static org.apache.ignite.internal.schema.NativeType.STRING;
import static org.apache.ignite.internal.schema.NativeType.UUID;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

/**
 * Serializer test.
 */
public class JavaSerializerTest {
    /**
     * @return List of serializers for test.
     */
    private static List<SerializerFactory> serializerFactoryProvider() {
        return Arrays.asList(
            new SerializerGenerator(),
            new JavaSerializerFactory()
        );
    }

    /** Random. */
    private Random rnd;

    /**
     *
     */
    @BeforeEach
    public void initRandom() {
        long seed = System.currentTimeMillis();

        System.out.println("Using seed: " + seed + "L;");

        rnd = new Random(seed);
    }

    /**
     *
     */
    @TestFactory
    public Stream<DynamicNode> testBasicTypes() {
        NativeType[] types = new NativeType[] {BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, UUID, STRING, BYTES, Bitmask.of(5)};

        return serializerFactoryProvider().stream().map(factory ->
            dynamicContainer(
                factory.getClass().getSimpleName(),
                Stream.concat(
                    // Test pure types.
                    Stream.of(types).map(type ->
                        dynamicTest("testBasicTypes(" + type.spec().name() + ')', () -> checkBasicType(factory, type, type))
                    ),

                    // Test pairs of mixed types.
                    Stream.of(
                        dynamicTest("testMixTypes 1", () -> checkBasicType(factory, LONG, INTEGER)),
                        dynamicTest("testMixTypes 1", () -> checkBasicType(factory, FLOAT, DOUBLE)),
                        dynamicTest("testMixTypes 1", () -> checkBasicType(factory, INTEGER, BYTES)),
                        dynamicTest("testMixTypes 1", () -> checkBasicType(factory, STRING, LONG)),
                        dynamicTest("testMixTypes 1", () -> checkBasicType(factory, Bitmask.of(9), BYTES))
                    )
                )
            ));
    }

    /**
     * @throws SerializationException If serialization failed.
     */
    @ParameterizedTest
    @MethodSource("serializerFactoryProvider")
    public void testComplexType(SerializerFactory factory) throws SerializationException {
        Column[] cols = new Column[] {
            new Column("pByteCol", BYTE, false),
            new Column("pShortCol", SHORT, false),
            new Column("pIntCol", INTEGER, false),
            new Column("pLongCol", LONG, false),
            new Column("pFloatCol", FLOAT, false),
            new Column("pDoubleCol", DOUBLE, false),

            new Column("byteCol", BYTE, true),
            new Column("shortCol", SHORT, true),
            new Column("intCol", INTEGER, true),
            new Column("longCol", LONG, true),
            new Column("nullLongCol", LONG, true),
            new Column("floatCol", FLOAT, true),
            new Column("doubleCol", DOUBLE, true),

            new Column("uuidCol", UUID, true),
            new Column("bitmaskCol", Bitmask.of(42), true),
            new Column("stringCol", STRING, true),
            new Column("nullBytesCol", BYTES, true),
            new Column("bytesCol", BYTES, true),
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, new Columns(cols), new Columns(cols.clone()));

        final Object key = TestObject.randomObject(rnd);
        final Object val = TestObject.randomObject(rnd);

        Serializer serializer = factory.create(schema, key.getClass(), val.getClass());

        byte[] bytes = serializer.serialize(key, val);

        // Try different order.
        Object restoredVal = serializer.deserializeValue(bytes);
        Object restoredKey = serializer.deserializeKey(bytes);

        assertTrue(key.getClass().isInstance(restoredKey));
        assertTrue(val.getClass().isInstance(restoredVal));

        assertEquals(key, restoredKey);
        assertEquals(val, restoredVal);
    }

    /**
     *
     */
    @ParameterizedTest
    @MethodSource("serializerFactoryProvider")
    public void testClassWithIncorrectBitmaskSize(SerializerFactory factory) {
        Column[] cols = new Column[] {
            new Column("pLongCol", LONG, false),
            new Column("bitmaskCol", Bitmask.of(9), true),
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, new Columns(cols), new Columns(cols.clone()));

        final Object key = TestObject.randomObject(rnd);
        final Object val = TestObject.randomObject(rnd);

        Serializer serializer = factory.create(schema, key.getClass(), val.getClass());

        assertThrows(
            SerializationException.class,
            () -> serializer.serialize(key, val),
            "Failed to write field [name=bitmaskCol]"
        );
    }

    /**
     *
     */
    @ParameterizedTest
    @MethodSource("serializerFactoryProvider")
    public void testClassWithWrongFieldType(SerializerFactory factory) {
        Column[] cols = new Column[] {
            new Column("bitmaskCol", Bitmask.of(42), true),
            new Column("shortCol", UUID, true)
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, new Columns(cols), new Columns(cols.clone()));

        final Object key = TestObject.randomObject(rnd);
        final Object val = TestObject.randomObject(rnd);

        Serializer serializer = factory.create(schema, key.getClass(), val.getClass());

        assertThrows(
            SerializationException.class,
            () -> serializer.serialize(key, val),
            "Failed to write field [name=shortCol]"
        );
    }

    /**
     *
     */
    @ParameterizedTest
    @MethodSource("serializerFactoryProvider")
    public void testClassWithPrivateConstructor(SerializerFactory factory) throws SerializationException {
        Column[] cols = new Column[] {
            new Column("pLongCol", LONG, false),
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, new Columns(cols), new Columns(cols.clone()));

        final Object key = PrivateTestObject.randomObject(rnd);
        final Object val = PrivateTestObject.randomObject(rnd);

        Serializer serializer = factory.create(schema, key.getClass(), val.getClass());

        byte[] bytes = serializer.serialize(key, val);

        Object key1 = serializer.deserializeKey(bytes);
        Object val1 = serializer.deserializeValue(bytes);

        assertTrue(key.getClass().isInstance(key1));
        assertTrue(val.getClass().isInstance(val1));

        assertEquals(key, key);
        assertEquals(val, val1);
    }

    /**
     *
     */
    @ParameterizedTest
    @MethodSource("serializerFactoryProvider")
    public void testClassWithNoDefaultConstructor(SerializerFactory factory) {
        Column[] cols = new Column[] {
            new Column("pLongCol", LONG, false),
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, new Columns(cols), new Columns(cols.clone()));

        final Object key = WrongTestObject.randomObject(rnd);
        final Object val = WrongTestObject.randomObject(rnd);

        assertThrows(IllegalStateException.class,
            () -> factory.create(schema, key.getClass(), val.getClass()),
            "Class has no default constructor: class=org.apache.ignite.internal.schema.marshaller.JavaSerializerTest$WrongTestObject"
        );
    }

    /**
     *
     */
    @ParameterizedTest
    @MethodSource("serializerFactoryProvider")
    public void testClassLoader(SerializerFactory factory) throws SerializationException {
        final ClassLoader prevClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(CompilerUtils.dynamicClassLoader());

            Column[] keyCols = new Column[] {
                new Column("key", LONG, false)
            };

            Column[] valCols = new Column[] {
                new Column("col0", LONG, false),
                new Column("col1", LONG, false),
                new Column("col2", LONG, false),
            };

            SchemaDescriptor schema = new SchemaDescriptor(1, new Columns(keyCols), new Columns(valCols));

            final Class<?> valClass = createGeneratedObjectClass(Long.class);
            final ObjectFactory<?> objFactory = new ObjectFactory<>(valClass);

            final Long key = rnd.nextLong();

            Serializer serializer = factory.create(schema, key.getClass(), valClass);

            byte[] bytes = serializer.serialize(key, objFactory.create());

            Object key1 = serializer.deserializeKey(bytes);
            Object val1 = serializer.deserializeValue(bytes);

            assertTrue(key.getClass().isInstance(key1));
            assertTrue(valClass.isInstance(val1));
        }
        finally {
            Thread.currentThread().setContextClassLoader(prevClassLoader);
        }
    }

    /**
     * Generate random key-value pair of given types and
     * check serialization and deserialization works fine.
     *
     * @param factory Serializer factory.
     * @param keyType Key type.
     * @param valType Value type.
     * @throws SerializationException If (de)serialization failed.
     */
    private void checkBasicType(SerializerFactory factory, NativeType keyType,
        NativeType valType) throws SerializationException {
        final Object key = generateRandomValue(keyType);
        final Object val = generateRandomValue(valType);

        Column[] keyCols = new Column[] {new Column("key", keyType, false)};
        Column[] valCols = new Column[] {new Column("val", valType, false)};

        SchemaDescriptor schema = new SchemaDescriptor(1, new Columns(keyCols), new Columns(valCols));

        Serializer serializer = factory.create(schema, key.getClass(), val.getClass());

        byte[] bytes = serializer.serialize(key, val);

        Object key1 = serializer.deserializeKey(bytes);
        Object val1 = serializer.deserializeValue(bytes);

        assertTrue(key.getClass().isInstance(key1));
        assertTrue(val.getClass().isInstance(val1));

        compareObjects(keyType, key, key);
        compareObjects(valType, val, val1);
    }

    /**
     * Compare object regarding NativeType.
     *
     * @param type Native type.
     * @param exp Expected value.
     * @param act Actual value.
     */
    private void compareObjects(NativeType type, Object exp, Object act) {
        if (type.spec() == NativeTypeSpec.BYTES)
            assertArrayEquals((byte[])exp, (byte[])act);
        else
            assertEquals(exp, act);
    }

    /**
     * Generates randon value of given type.
     *
     * @param type Type.
     */
    private Object generateRandomValue(NativeType type) {
        return TestUtils.generateRandomValue(rnd, type);
    }

    /**
     * Generate class for test objects.
     *
     * @param fieldType Field type.
     * @return Generated test object class.
     */
    private Class<?> createGeneratedObjectClass(Class<?> fieldType) {
        final String packageName = getClass().getPackageName();
        final String className = "GeneratedTestObject";

        final TypeSpec.Builder classBuilder = TypeSpec.classBuilder(className).addModifiers(Modifier.PUBLIC);

        for (int i = 0; i < 3; i++)
            classBuilder.addField(fieldType, "col" + i, Modifier.PRIVATE);

        { // Build constructor.
            final MethodSpec.Builder builder = MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addStatement("$T rnd = new $T()", Random.class, Random.class);

            for (int i = 0; i < 3; i++)
                builder.addStatement("col$L = rnd.nextLong()", i);

            classBuilder.addMethod(builder.build());
        }

        final JavaFile javaFile = JavaFile.builder(packageName, classBuilder.build()).build();

        try {
            return CompilerUtils.compileCode(javaFile).loadClass(packageName + '.' + className);
        }
        catch (Exception ex) {
            throw new IllegalStateException("Failed to compile/instantiate generated Serializer.", ex);
        }
    }

    /**
     * Test object.
     */
    public static class TestObject {
        /**
         * @return Random TestObject.
         */
        public static TestObject randomObject(Random rnd) {
            final TestObject obj = new TestObject();

            obj.pByteCol = (byte)rnd.nextInt(255);
            obj.pShortCol = (short)rnd.nextInt(65535);
            obj.pIntCol = rnd.nextInt();
            obj.pLongCol = rnd.nextLong();
            obj.pFloatCol = rnd.nextFloat();
            obj.pDoubleCol = rnd.nextDouble();

            obj.byteCol = (byte)rnd.nextInt(255);
            obj.shortCol = (short)rnd.nextInt(65535);
            obj.intCol = rnd.nextInt();
            obj.longCol = rnd.nextLong();
            obj.floatCol = rnd.nextFloat();
            obj.doubleCol = rnd.nextDouble();
            obj.nullLongCol = null;

            obj.nullBytesCol = null;
            obj.uuidCol = new UUID(rnd.nextLong(), rnd.nextLong());
            obj.bitmaskCol = TestUtils.randomBitSet(rnd, 42);
            obj.stringCol = TestUtils.randomString(rnd, rnd.nextInt(255));
            obj.bytesCol = TestUtils.randomBytes(rnd, rnd.nextInt(255));

            return obj;
        }

        // Primitive typed
        private byte pByteCol;
        private short pShortCol;
        private int pIntCol;
        private long pLongCol;
        private float pFloatCol;
        private double pDoubleCol;

        // Reference typed
        private Byte byteCol;
        private Short shortCol;
        private Integer intCol;
        private Long longCol;
        private Long nullLongCol;
        private Float floatCol;
        private Double doubleCol;

        private UUID uuidCol;
        private BitSet bitmaskCol;
        private String stringCol;
        private byte[] bytesCol;
        private byte[] nullBytesCol;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestObject object = (TestObject)o;

            return pByteCol == object.pByteCol &&
                pShortCol == object.pShortCol &&
                pIntCol == object.pIntCol &&
                pLongCol == object.pLongCol &&
                Float.compare(object.pFloatCol, pFloatCol) == 0 &&
                Double.compare(object.pDoubleCol, pDoubleCol) == 0 &&
                Objects.equals(byteCol, object.byteCol) &&
                Objects.equals(shortCol, object.shortCol) &&
                Objects.equals(intCol, object.intCol) &&
                Objects.equals(longCol, object.longCol) &&
                Objects.equals(nullLongCol, object.nullLongCol) &&
                Objects.equals(floatCol, object.floatCol) &&
                Objects.equals(doubleCol, object.doubleCol) &&
                Objects.equals(uuidCol, object.uuidCol) &&
                Objects.equals(bitmaskCol, object.bitmaskCol) &&
                Objects.equals(stringCol, object.stringCol) &&
                Arrays.equals(bytesCol, object.bytesCol) &&
                Arrays.equals(nullBytesCol, object.nullBytesCol);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 73;
        }
    }

    /**
     * Test object with private constructor.
     */
    public static class PrivateTestObject {
        /**
         * @return Random TestObject.
         */
        static PrivateTestObject randomObject(Random rnd) {
            final PrivateTestObject obj = new PrivateTestObject();

            obj.pLongCol = rnd.nextLong();

            return obj;
        }

        /** Value. */
        private long pLongCol;

        /**
         * Private constructor.
         */
        private PrivateTestObject() {
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            PrivateTestObject object = (PrivateTestObject)o;

            return pLongCol == object.pLongCol;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(pLongCol);
        }
    }

    /**
     * Test object without default constructor.
     */
    private static class WrongTestObject {
        /**
         * @return Random TestObject.
         */
        static WrongTestObject randomObject(Random rnd) {
            return new WrongTestObject(rnd.nextLong());
        }

        /** Value. */
        private final long pLongCol;

        /**
         * Private constructor.
         */
        private WrongTestObject(long val) {
            pLongCol = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            WrongTestObject object = (WrongTestObject)o;

            return pLongCol == object.pLongCol;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(pLongCol);
        }
    }
}
