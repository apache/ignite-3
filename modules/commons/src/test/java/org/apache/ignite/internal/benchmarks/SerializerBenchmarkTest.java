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

package org.apache.ignite.internal.benchmarks;

import java.lang.reflect.Field;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.Serializer;
import org.apache.ignite.internal.schema.marshaller.SerializerFactory;
import org.apache.ignite.internal.util.Factory;
import org.apache.ignite.internal.util.ObjectFactory;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static org.apache.ignite.internal.schema.NativeType.LONG;

/**
 * Serializer benchmark.
 */
@State(Scope.Benchmark)
@Warmup(time = 10, iterations = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(time = 10, iterations = 5, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
public class SerializerBenchmarkTest {
    /** Random. */
    private Random rnd = new Random();

    /** Reflection-based Serializer. */
    private Serializer serializer;

    /** Test object factory. */
    private Factory<?> objectFactory;

    /** Object fields count. */
    @Param({"10", "100"})
    public int fieldsCount;

    /** Serializer. */
    @Param({"Janino", "Java"})
    public String serializerName;

    /**
     * Runner.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(SerializerBenchmarkTest.class.getSimpleName())
            .build();

        new Runner(opt).run();
    }

    /**
     * @throws Exception If failed.
     */
    @Setup
    public void init() throws Exception {
        long seed = System.currentTimeMillis();

        rnd = new Random(seed);

        final Class<?> valClass = createGeneratedObjectClass(fieldsCount, Long.TYPE);
        objectFactory = new ObjectFactory<>(valClass);

        Columns keyCols = new Columns(new Column("key", LONG, true));
        Columns valCols = mapFieldsToColumns(valClass);
        final SchemaDescriptor schema = new SchemaDescriptor(1, keyCols, valCols);

        if ("Java".equals(serializerName))
            serializer = SerializerFactory.createJavaSerializerFactory().create(schema, Long.class, valClass);
        else
            serializer = SerializerFactory.createJaninoSerializerFactory().create(schema, Long.class, valClass);
    }

    /**
     * Measure serialization-deserialization operation cost.
     *
     * @param bh Black hole.
     * @throws Exception If failed.
     */
    @Benchmark
    public void measureSerializeDeserializeCost(Blackhole bh) throws Exception {
        Long key = rnd.nextLong();

        Object val = objectFactory.create();
        byte[] bytes = serializer.serialize(key, val);

        Object restoredKey = serializer.deserializeKey(bytes);
        Object restoredVal = serializer.deserializeValue(bytes);

        bh.consume(restoredVal);
        bh.consume(restoredKey);
    }

    /**
     * Map fields to columns.
     *
     * @param aClass Object class.
     * @return Columns for schema
     */
    private Columns mapFieldsToColumns(Class<?> aClass) {
        final Field[] fields = aClass.getDeclaredFields();
        final Column[] cols = new Column[fields.length];

        for (int i = 0; i < fields.length; i++) {
            assert fields[i].getType() == Long.TYPE : "Only 'long' field type is supported.";

            cols[i] = new Column("col" + i, LONG, false);
        }

        return new Columns(cols);
    }

    /**
     * Generate class for test objects.
     *
     * @param maxFields Max class member fields.
     * @param fieldType Field type.
     * @return Generated test object class.
     * @throws Exception If failed.
     */
    private Class<?> createGeneratedObjectClass(int maxFields, Class<?> fieldType) throws Exception {
        final IClassBodyEvaluator ce = CompilerFactoryFactory.getDefaultCompilerFactory().newClassBodyEvaluator();

        ce.setClassName("TestObject");
        ce.setDefaultImports("java.util.Random");

        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < maxFields; i++)
            sb.append(fieldType.getName()).append(" col").append(i).append(";\n");

        // Constructor.
        sb.append("public TestObject() {\n");
        sb.append("    Random rnd = new Random();\n");
        for (int i = 0; i < maxFields; i++)
            sb.append("    col").append(i).append(" = rnd.nextLong();\n");
        sb.append("}\n");

        try {
            ce.setParentClassLoader(getClass().getClassLoader());
            ce.cook(sb.toString());

            return ce.getClazz();
        }
        catch (Exception ex) {
            throw new IllegalStateException("Failed to compile/instantiate generated Serializer.", ex);
        }
    }
}
