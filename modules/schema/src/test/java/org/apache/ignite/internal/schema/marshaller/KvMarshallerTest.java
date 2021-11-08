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
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.processing.Generated;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.testobjects.TestObjectWithAllTypes;
import org.apache.ignite.internal.schema.testobjects.TestObjectWithNoDefaultConstructor;
import org.apache.ignite.internal.schema.testobjects.TestObjectWithPrivateConstructor;
import org.apache.ignite.internal.schema.testobjects.TestSimpleObject;
import org.apache.ignite.internal.util.ObjectFactory;
import org.apache.ignite.lang.IgniteInternalException;
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
     * @return List of marshallers for test.
     */
    private static List<MarshallerFactory> marshallerFactoryProvider() {
        return List.of(new ReflectionMarshallerFactory());
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
    
    /**
     * @throws MarshallerException If serialization failed.
     */
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void complexType(MarshallerFactory factory) throws MarshallerException {
        Column[] cols = columnsAllTypes();
    
        SchemaDescriptor schema = new SchemaDescriptor(1, cols, cols);
        
        final Object key = TestObjectWithAllTypes.randomObject(rnd);
        final Object val = TestObjectWithAllTypes.randomObject(rnd);
        
        KvMarshaller marshaller = factory.create(schema, key.getClass(), val.getClass());
        
        BinaryRow row = marshaller.marshal(key, val);
        
        // Try different order.
        Object restoredVal = marshaller.unmarshalValue(new Row(schema, row));
        Object restoredKey = marshaller.unmarshalKey(new Row(schema, row));
        
        assertTrue(key.getClass().isInstance(restoredKey));
        assertTrue(val.getClass().isInstance(restoredVal));
        
        assertEquals(key, restoredKey);
        assertEquals(val, restoredVal);
    }
    
    /**
     * @throws MarshallerException If serialization failed.
     */
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void truncatedType(MarshallerFactory factory) throws MarshallerException {
        Column[] cols = columnsAllTypes();
    
        SchemaDescriptor schema = new SchemaDescriptor(1, cols, cols);
        
        final Object key = TestTruncatedObject.randomObject(rnd);
        final Object val = TestTruncatedObject.randomObject(rnd);
        
        KvMarshaller marshaller = factory.create(schema, key.getClass(), val.getClass());
        
        BinaryRow row = marshaller.marshal(key, val);
        
        // Try different order.
        Object restoredVal = marshaller.unmarshalValue(new Row(schema, row));
        Object restoredKey = marshaller.unmarshalKey(new Row(schema, row));
        
        assertTrue(key.getClass().isInstance(restoredKey));
        assertTrue(val.getClass().isInstance(restoredVal));
        
        assertEquals(key, restoredKey);
        assertEquals(val, restoredVal);
    }
    
    /**
     *
     */
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classWithWrongFieldType(MarshallerFactory factory) {
        Column[] cols = new Column[]{
                new Column("bitmaskCol", NativeTypes.bitmaskOf(42), true),
                new Column("shortCol", UUID, true)
        };
        
        SchemaDescriptor schema = new SchemaDescriptor(1, cols, cols);
        
        final Object key = TestObjectWithAllTypes.randomObject(rnd);
        final Object val = TestObjectWithAllTypes.randomObject(rnd);
        
        KvMarshaller marshaller = factory.create(schema, key.getClass(), val.getClass());
        
        assertThrows(
                MarshallerException.class,
                () -> marshaller.marshal(key, val),
                "Failed to write field [name=shortCol]"
        );
    }
    
    /**
     *
     */
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classWithIncorrectBitmaskSize(MarshallerFactory factory) {
        Column[] cols = new Column[]{
                new Column("primitiveLongCol", INT64, false),
                new Column("bitmaskCol", NativeTypes.bitmaskOf(9), true),
        };
        
        SchemaDescriptor schema = new SchemaDescriptor(1, cols, cols);
        
        final Object key = TestObjectWithAllTypes.randomObject(rnd);
        final Object val = TestObjectWithAllTypes.randomObject(rnd);
        
        KvMarshaller marshaller = factory.create(schema, key.getClass(), val.getClass());
        
        assertThrows(
                MarshallerException.class,
                () -> marshaller.marshal(key, val),
                "Failed to write field [name=bitmaskCol]"
        );
    }
    
    /**
     *
     */
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classWithPrivateConstructor(MarshallerFactory factory) throws MarshallerException {
        Column[] cols = new Column[]{
                new Column("primLongCol", INT64, false),
        };
        
        SchemaDescriptor schema = new SchemaDescriptor(1, cols, cols);
        
        final Object key = TestObjectWithPrivateConstructor.randomObject(rnd);
        final Object val = TestObjectWithPrivateConstructor.randomObject(rnd);
        
        KvMarshaller marshaller = factory.create(schema, key.getClass(), val.getClass());
        
        BinaryRow row = marshaller.marshal(key, val);
        
        Object key1 = marshaller.unmarshalKey(new Row(schema, row));
        Object val1 = marshaller.unmarshalValue(new Row(schema, row));
        
        assertTrue(key.getClass().isInstance(key1));
        assertTrue(val.getClass().isInstance(val1));
        
        assertEquals(key, key);
        assertEquals(val, val1);
    }
    
    /**
     *
     */
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classWithNoDefaultConstructor(MarshallerFactory factory) {
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
    @MethodSource("marshallerFactoryProvider")
    public void privateClass(MarshallerFactory factory) throws MarshallerException {
        Column[] cols = new Column[]{
                new Column("primLongCol", INT64, false),
        };
        
        SchemaDescriptor schema = new SchemaDescriptor(1, cols, cols);
        
        final Object key = TestObjectWithPrivateConstructor.randomObject(rnd);
        final Object val = TestObjectWithPrivateConstructor.randomObject(rnd);
        
        final ObjectFactory<TestObjectWithPrivateConstructor> objFactory = new ObjectFactory<>(TestObjectWithPrivateConstructor.class);
        final KvMarshaller marshaller = factory.create(schema, key.getClass(), val.getClass());
        
        BinaryRow row = marshaller.marshal(key, objFactory.create());
        
        Object key1 = marshaller.unmarshalKey(new Row(schema, row));
        Object val1 = marshaller.unmarshalValue(new Row(schema, row));
        
        assertTrue(key.getClass().isInstance(key1));
        assertTrue(val.getClass().isInstance(val1));
    }
    
    /**
     *
     */
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classLoader(MarshallerFactory factory) throws MarshallerException {
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
            
            KvMarshaller marshaller = factory.create(schema, key.getClass(), valClass);
            
            BinaryRow row = marshaller.marshal(key, objFactory.create());
            
            Object key1 = marshaller.unmarshalKey(new Row(schema, row));
            Object val1 = marshaller.unmarshalValue(new Row(schema, row));
            
            assertTrue(key.getClass().isInstance(key1));
            assertTrue(valClass.isInstance(val1));
        } finally {
            Thread.currentThread().setContextClassLoader(loader);
        }
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
        
        Column[] keyCols = new Column[]{new Column("key", keyType, false)};
        Column[] valCols = new Column[]{new Column("val", valType, false)};
        
        SchemaDescriptor schema = new SchemaDescriptor(1, keyCols, valCols);
        
        KvMarshaller marshaller = factory.create(schema, key.getClass(), val.getClass());
        
        BinaryRow row = marshaller.marshal(key, val);
        
        Object key1 = marshaller.unmarshalKey(new Row(schema, row));
        Object val1 = marshaller.unmarshalValue(new Row(schema, row));
        
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
    public static class TestTruncatedObject {
        /**
         * @return Random TestObject.
         */
        public static TestTruncatedObject randomObject(Random rnd) {
            final TestTruncatedObject obj = new TestTruncatedObject();
    
            obj.primitiveByteCol = (byte) rnd.nextInt(255);
            obj.primitiveShortCol = (short) rnd.nextInt(65535);
            obj.primitiveIntCol = rnd.nextInt();
            obj.primitiveLongCol = rnd.nextLong();
            obj.primitiveFloatCol = rnd.nextFloat();
            obj.primitiveDoubleCol = rnd.nextDouble();
            
            return obj;
        }
        
        // Primitive typed
        private byte primitiveByteCol;
    
        private short primitiveShortCol;
    
        private int primitiveIntCol;
    
        private long primitiveLongCol;
    
        private float primitiveFloatCol;
    
        private double primitiveDoubleCol;
        
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
    
            return primitiveByteCol == object.primitiveByteCol
                    && primitiveShortCol == object.primitiveShortCol
                    && primitiveIntCol == object.primitiveIntCol
                    && primitiveLongCol == object.primitiveLongCol
                    && Float.compare(object.primitiveFloatCol, primitiveFloatCol) == 0
                    && Double.compare(object.primitiveDoubleCol, primitiveDoubleCol) == 0;
        }
        
        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return 42;
        }
    }
    
    private Column[] columnsAllTypes() {
        Column[] cols = new Column[]{
                new Column("primitiveByteCol", INT8, false),
                new Column("primitiveShortCol", INT16, false),
                new Column("primitiveIntCol", INT32, false),
                new Column("primitiveLongCol", INT64, false),
                new Column("primitiveFloatCol", FLOAT, false),
                new Column("primitiveDoubleCol", DOUBLE, false),
                
                new Column("byteCol", INT8, true),
                new Column("shortCol", INT16, true),
                new Column("intCol", INT32, true),
                new Column("longCol", INT64, true),
                new Column("nullLongCol", INT64, true),
                new Column("floatCol", FLOAT, true),
                new Column("doubleCol", DOUBLE, true),
                
                new Column("dateCol", DATE, true),
                new Column("timeCol", time(), true),
                new Column("dateTimeCol", datetime(), true),
                new Column("timestampCol", timestamp(), true),
                
                new Column("uuidCol", UUID, true),
                new Column("bitmaskCol", NativeTypes.bitmaskOf(42), true),
                new Column("stringCol", STRING, true),
                new Column("nullBytesCol", BYTES, true),
                new Column("bytesCol", BYTES, true),
                new Column("numberCol", NativeTypes.numberOf(12), true),
                new Column("decimalCol", NativeTypes.decimalOf(19, 3), true),
        };
        // Validate all types are tested.
        Set<NativeTypeSpec> testedTypes = Arrays.stream(cols).map(c -> c.type().spec())
                .collect(Collectors.toSet());
        Set<NativeTypeSpec> missedTypes = Arrays.stream(NativeTypeSpec.values())
                .filter(t -> !testedTypes.contains(t)).collect(Collectors.toSet());
        
        assertEquals(Collections.emptySet(), missedTypes);
        return cols;
    }
}
