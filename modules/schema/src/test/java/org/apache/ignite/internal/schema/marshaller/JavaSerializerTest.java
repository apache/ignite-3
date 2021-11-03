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

import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.schema.NativeTypes.FLOAT;
import static org.apache.ignite.internal.schema.NativeTypes.INT16;
import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.INT64;
import static org.apache.ignite.internal.schema.NativeTypes.INT8;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.apache.ignite.internal.schema.NativeTypes.UUID;
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import javax.annotation.processing.Generated;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.schema.marshaller.asm.AsmSerializerGenerator;
import org.apache.ignite.internal.schema.marshaller.reflection.JavaSerializerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.testobjects.TestObjectWithAllTypes;
import org.apache.ignite.internal.schema.testobjects.TestObjectWithNoDefaultConstructor;
import org.apache.ignite.internal.schema.testobjects.TestObjectWithPrivateConstructor;
import org.apache.ignite.internal.util.ObjectFactory;
import org.apache.ignite.lang.IgniteInternalException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Serializer test.
 */
public class JavaSerializerTest {
    /**
     * @return List of serializers for test.
     */
    private static List<SerializerFactory> serializerFactoryProvider() {
        return List.of(new JavaSerializerFactory(), new AsmSerializerGenerator());
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
        NativeType[] types = new NativeType[]{INT8, INT16, INT32, INT64, FLOAT, DOUBLE, UUID, STRING, BYTES,
                NativeTypes.bitmaskOf(5), NativeTypes.numberOf(42), NativeTypes.decimalOf(12, 3)};
        
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
    
    /**
     * @throws MarshallerException If serialization failed.
     */
    @ParameterizedTest
    @MethodSource("serializerFactoryProvider")
    public void complexType(SerializerFactory factory) throws MarshallerException {
        Column[] cols = new Column[]{
                new Column("primByteCol", INT8, false),
                new Column("primShortCol", INT16, false),
                new Column("primIntCol", INT32, false),
                new Column("primLongCol", INT64, false),
                new Column("primFloatCol", FLOAT, false),
                new Column("primDoubleCol", DOUBLE, false),
                
                new Column("byteCol", INT8, true),
                new Column("shortCol", INT16, true),
                new Column("intCol", INT32, true),
                new Column("longCol", INT64, true),
                new Column("nullLongCol", INT64, true),
                new Column("floatCol", FLOAT, true),
                new Column("doubleCol", DOUBLE, true),
                
                new Column("uuidCol", UUID, true),
                new Column("bitmaskCol", NativeTypes.bitmaskOf(42), true),
                new Column("stringCol", STRING, true),
                new Column("nullBytesCol", BYTES, true),
                new Column("bytesCol", BYTES, true),
                new Column("numberCol", NativeTypes.numberOf(12), true),
                new Column("decimalCol", NativeTypes.decimalOf(19, 3), true),
        };
        
        SchemaDescriptor schema = new SchemaDescriptor(1, cols, cols);
        
        final Object key = TestObjectWithAllTypes.randomObject(rnd);
        final Object val = TestObjectWithAllTypes.randomObject(rnd);
        
        Serializer serializer = factory.create(schema, key.getClass(), val.getClass());
        
        BinaryRow row = serializer.serialize(key, val);
        
        // Try different order.
        Object restoredVal = serializer.deserializeValue(new Row(schema, row));
        Object restoredKey = serializer.deserializeKey(new Row(schema, row));
        
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
    public void classWithWrongFieldType(SerializerFactory factory) {
        Column[] cols = new Column[]{
                new Column("bitmaskCol", NativeTypes.bitmaskOf(42), true),
                new Column("shortCol", UUID, true)
        };
        
        SchemaDescriptor schema = new SchemaDescriptor(1, cols, cols);
        
        final Object key = TestObjectWithAllTypes.randomObject(rnd);
        final Object val = TestObjectWithAllTypes.randomObject(rnd);
        
        Serializer serializer = factory.create(schema, key.getClass(), val.getClass());
        
        assertThrows(
                MarshallerException.class,
                () -> serializer.serialize(key, val),
                "Failed to write field [name=shortCol]"
        );
    }
    
    /**
     *
     */
    @ParameterizedTest
    @MethodSource("serializerFactoryProvider")
    public void classWithIncorrectBitmaskSize(SerializerFactory factory) {
        Column[] cols = new Column[]{
                new Column("primLongCol", INT64, false),
                new Column("bitmaskCol", NativeTypes.bitmaskOf(9), true),
        };
        
        SchemaDescriptor schema = new SchemaDescriptor(1, cols, cols);
        
        final Object key = TestObjectWithAllTypes.randomObject(rnd);
        final Object val = TestObjectWithAllTypes.randomObject(rnd);
        
        Serializer serializer = factory.create(schema, key.getClass(), val.getClass());
        
        assertThrows(
                MarshallerException.class,
                () -> serializer.serialize(key, val),
                "Failed to write field [name=bitmaskCol]"
        );
    }
    
    /**
     *
     */
    @ParameterizedTest
    @MethodSource("serializerFactoryProvider")
    public void classWithPrivateConstructor(SerializerFactory factory) throws MarshallerException {
        Column[] cols = new Column[]{
                new Column("primLongCol", INT64, false),
        };
        
        SchemaDescriptor schema = new SchemaDescriptor(1, cols, cols);
        
        final Object key = TestObjectWithPrivateConstructor.randomObject(rnd);
        final Object val = TestObjectWithPrivateConstructor.randomObject(rnd);
        
        Serializer serializer = factory.create(schema, key.getClass(), val.getClass());
        
        BinaryRow row = serializer.serialize(key, val);
        
        Object key1 = serializer.deserializeKey(new Row(schema, row));
        Object val1 = serializer.deserializeValue(new Row(schema, row));
        
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
    public void classWithNoDefaultConstructor(SerializerFactory factory) {
        Column[] cols = new Column[]{
                new Column("primLongCol", INT64, false),
        };
        
        SchemaDescriptor schema = new SchemaDescriptor(1, cols, cols);
        
        final Object key = TestObjectWithNoDefaultConstructor.randomObject(rnd);
        final Object val = TestObjectWithNoDefaultConstructor.randomObject(rnd);
        
        assertThrows(IgniteInternalException.class, () -> factory.create(schema, key.getClass(), val.getClass()));
    }
    
    /**
     *
     */
    @ParameterizedTest
    @MethodSource("serializerFactoryProvider")
    public void privateClass(SerializerFactory factory) throws MarshallerException {
        Column[] cols = new Column[]{
                new Column("primLongCol", INT64, false),
        };
        
        SchemaDescriptor schema = new SchemaDescriptor(1, cols, cols);
        
        final Object key = TestObjectWithPrivateConstructor.randomObject(rnd);
        final Object val = TestObjectWithPrivateConstructor.randomObject(rnd);
        
        final ObjectFactory<?> objFactory = new ObjectFactory<>(TestObjectWithPrivateConstructor.class);
        final Serializer serializer = factory.create(schema, key.getClass(), val.getClass());
        
        BinaryRow row = serializer.serialize(key, objFactory.create());
        
        Object key1 = serializer.deserializeKey(new Row(schema, row));
        Object val1 = serializer.deserializeValue(new Row(schema, row));
        
        assertTrue(key.getClass().isInstance(key1));
        assertTrue(val.getClass().isInstance(val1));
    }
    
    /**
     *
     */
    @ParameterizedTest
    @MethodSource("serializerFactoryProvider")
    public void classLoader(SerializerFactory factory) throws MarshallerException {
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(new DynamicClassLoader(getClass().getClassLoader()));
            
            Column[] keyCols = new Column[]{
                    new Column("key", INT64, false)
            };
            
            Column[] valCols = new Column[]{
                    new Column("col0", INT64, false),
                    new Column("col1", INT64, false),
                    new Column("col2", INT64, false),
            };
            
            SchemaDescriptor schema = new SchemaDescriptor(1, keyCols, valCols);
            
            final Class<?> valClass = createGeneratedObjectClass(long.class);
            final ObjectFactory<?> objFactory = new ObjectFactory<>(valClass);
            
            final Long key = rnd.nextLong();
            
            Serializer serializer = factory.create(schema, key.getClass(), valClass);
            
            BinaryRow row = serializer.serialize(key, objFactory.create());
            
            Object key1 = serializer.deserializeKey(new Row(schema, row));
            Object val1 = serializer.deserializeValue(new Row(schema, row));
            
            assertTrue(key.getClass().isInstance(key1));
            assertTrue(valClass.isInstance(val1));
        } finally {
            Thread.currentThread().setContextClassLoader(loader);
        }
    }
    
    /**
     * Generate random key-value pair of given types and check serialization and deserialization works fine.
     *
     * @param factory Serializer factory.
     * @param keyType Key type.
     * @param valType Value type.
     * @throws MarshallerException If (de)serialization failed.
     */
    private void checkBasicType(SerializerFactory factory, NativeType keyType,
                                NativeType valType) throws MarshallerException {
        final Object key = generateRandomValue(keyType);
        final Object val = generateRandomValue(valType);
        
        Column[] keyCols = new Column[]{new Column("key", keyType, false)};
        Column[] valCols = new Column[]{new Column("val", valType, false)};
        
        SchemaDescriptor schema = new SchemaDescriptor(1, keyCols, valCols);
        
        Serializer serializer = factory.create(schema, key.getClass(), val.getClass());
        
        BinaryRow row = serializer.serialize(key, val);
        
        Object key1 = serializer.deserializeKey(new Row(schema, row));
        Object val1 = serializer.deserializeValue(new Row(schema, row));
        
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
     * @param fieldType Field type.
     * @return Generated test object class.
     */
    private Class<?> createGeneratedObjectClass(Class<?> fieldType) {
        final String packageName = getClass().getPackageName();
        final String className = "GeneratedTestObject";
        
        final ClassDefinition classDef = new ClassDefinition(
                EnumSet.of(Access.PUBLIC),
                packageName.replace('.', '/') + '/' + className,
                ParameterizedType.type(Object.class)
        );
        classDef.declareAnnotation(Generated.class).setValue("value", getClass().getCanonicalName());
    
        for (int i = 0; i < 3; i++) {
            classDef.declareField(EnumSet.of(Access.PRIVATE), "col" + i, ParameterizedType.type(fieldType));
        }
    
        // Build constructor.
        final MethodDefinition methodDef = classDef.declareConstructor(EnumSet.of(Access.PUBLIC));
        final Variable rnd = methodDef.getScope().declareVariable(Random.class, "rnd");
    
        BytecodeBlock body = methodDef.getBody()
                .append(methodDef.getThis())
                .invokeConstructor(classDef.getSuperClass())
                .append(rnd.set(BytecodeExpressions.newInstance(Random.class)));
    
        for (int i = 0; i < 3; i++) {
            body.append(methodDef.getThis().setField("col" + i, rnd.invoke("nextLong", long.class).cast(fieldType)));
        }
    
        body.ret();
        
        return ClassGenerator.classGenerator(Thread.currentThread().getContextClassLoader())
                .fakeLineNumbers(true)
                .runAsmVerifier(true)
                .dumpRawBytecode(true)
                .defineClass(classDef, Object.class);
    }
    
    /**
     * Test object.
     */
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    public static class TestObject {
        /**
         * @return Random TestObject.
         */
        public static TestObject randomObject(Random rnd) {
            final TestObject obj = new TestObject();
            
            obj.primByteCol = (byte) rnd.nextInt(255);
            obj.primShortCol = (short) rnd.nextInt(65535);
            obj.primIntCol = rnd.nextInt();
            obj.primLongCol = rnd.nextLong();
            obj.primFloatCol = rnd.nextFloat();
            obj.primDoubleCol = rnd.nextDouble();
            
            obj.byteCol = (byte) rnd.nextInt(255);
            obj.shortCol = (short) rnd.nextInt(65535);
            obj.intCol = rnd.nextInt();
            obj.longCol = rnd.nextLong();
            obj.floatCol = rnd.nextFloat();
            obj.doubleCol = rnd.nextDouble();
            obj.nullLongCol = null;
            
            obj.nullBytesCol = null;
            obj.uuidCol = new UUID(rnd.nextLong(), rnd.nextLong());
            obj.bitmaskCol = IgniteTestUtils.randomBitSet(rnd, 42);
            obj.stringCol = IgniteTestUtils.randomString(rnd, rnd.nextInt(255));
            obj.bytesCol = IgniteTestUtils.randomBytes(rnd, rnd.nextInt(255));
            obj.numberCol = (BigInteger) TestUtils.generateRandomValue(rnd, NativeTypes.numberOf(12));
            obj.decimalCol = (BigDecimal) TestUtils.generateRandomValue(rnd, NativeTypes.decimalOf(19, 3));
            
            return obj;
        }
        
        // Primitive typed
        private byte primByteCol;
        
        private short primShortCol;
        
        private int primIntCol;
        
        private long primLongCol;
        
        private float primFloatCol;
        
        private double primDoubleCol;
        
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
        
        private BigInteger numberCol;
        
        private BigDecimal decimalCol;
        
        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
    
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            
            TestObject object = (TestObject) o;
            
            return primByteCol == object.primByteCol
                    && primShortCol == object.primShortCol
                    && primIntCol == object.primIntCol
                    && primLongCol == object.primLongCol
                    && Float.compare(object.primFloatCol, primFloatCol) == 0
                    && Double.compare(object.primDoubleCol, primDoubleCol) == 0
                    && Objects.equals(byteCol, object.byteCol)
                    && Objects.equals(shortCol, object.shortCol)
                    && Objects.equals(intCol, object.intCol)
                    && Objects.equals(longCol, object.longCol)
                    && Objects.equals(nullLongCol, object.nullLongCol)
                    && Objects.equals(floatCol, object.floatCol)
                    && Objects.equals(doubleCol, object.doubleCol)
                    && Objects.equals(uuidCol, object.uuidCol)
                    && Objects.equals(bitmaskCol, object.bitmaskCol)
                    && Objects.equals(stringCol, object.stringCol)
                    && Arrays.equals(bytesCol, object.bytesCol)
                    && Objects.equals(numberCol, object.numberCol)
                    && Objects.equals(decimalCol, object.decimalCol);
        }
        
        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return 73;
        }
    }
    
    /**
     * Test object with private constructor.
     */
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    public static class TestObjectWithPrivateConstructor {
        /**
         * @return Random TestObject.
         */
        static TestObjectWithPrivateConstructor randomObject(Random rnd) {
            final TestObjectWithPrivateConstructor obj = new TestObjectWithPrivateConstructor();
            
            obj.primLongCol = rnd.nextLong();
            
            return obj;
        }
        
        /** Value. */
        private long primLongCol;
        
        /**
         * Private constructor.
         */
        private TestObjectWithPrivateConstructor() {
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
            
            TestObjectWithPrivateConstructor object = (TestObjectWithPrivateConstructor) o;
            
            return primLongCol == object.primLongCol;
        }
        
        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Objects.hash(primLongCol);
        }
    }
    
    /**
     * Test object without default constructor.
     */
    public static class WrongTestObject {
        /**
         * @return Random TestObject.
         */
        static WrongTestObject randomObject(Random rnd) {
            return new WrongTestObject(rnd.nextLong());
        }
        
        /** Value. */
        private final long primLongCol;
        
        /**
         * Private constructor.
         */
        public WrongTestObject(long val) {
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
            
            WrongTestObject object = (WrongTestObject) o;
            
            return primLongCol == object.primLongCol;
        }
        
        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Objects.hash(primLongCol);
        }
    }
    
    /**
     * Test object without default constructor.
     */
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    private static class PrivateTestObject {
        /**
         * @return Random TestObject.
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
            
            WrongTestObject object = (WrongTestObject) o;
            
            return primLongCol == object.primLongCol;
        }
        
        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Objects.hash(primLongCol);
        }
    }
}
