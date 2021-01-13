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

import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import java.lang.reflect.Field;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import javax.lang.model.element.Modifier;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.CompilerUtils;
import org.apache.ignite.internal.schema.marshaller.Serializer;
import org.apache.ignite.internal.schema.marshaller.SerializerFactory;
import org.apache.ignite.internal.util.Factory;
import org.apache.ignite.internal.util.ObjectFactory;
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
@Warmup(time = 30, timeUnit = TimeUnit.SECONDS)
@Measurement(time = 60, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(jvmArgs = "-Djava.lang.invoke.stringConcat=BC_SB" /* Workaround for Java 9+ */, value = 1)
public class SerializerBenchmarkTest {
    /** Random. */
    private Random rnd = new Random();

    /** Reflection-based Serializer. */
    private Serializer serializer;

    /** Test object factory. */
    private Factory<?> objectFactory;

    /** Object fields count. */
    @Param({"0", "1", "10", "100"})
    public int fieldsCount;

    /** Serializer. */
    @Param({"ASM", "Java"})
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
     *
     */
    @Setup
    public void init() {
        Thread.currentThread().setContextClassLoader(CompilerUtils.dynamicClassLoader());

        long seed = System.currentTimeMillis();

        rnd = new Random(seed);

        final Class<?> valClass;

        if (fieldsCount == 0) {
            valClass = Long.class;
            objectFactory = (Factory<Object>)rnd::nextLong;
        }
        else {
            valClass = createGeneratedObjectClass(fieldsCount, long.class);
            objectFactory = new ObjectFactory<>(valClass);
        }

        Columns keyCols = new Columns(new Column("key", LONG, true));
        Columns valCols = mapFieldsToColumns(valClass);
        final SchemaDescriptor schema = new SchemaDescriptor(1, keyCols, valCols);

        if ("Java".equals(serializerName))
            serializer = SerializerFactory.createJavaSerializerFactory().create(schema, Long.class, valClass);
        else
            serializer = SerializerFactory.createGeneratedSerializerFactory().create(schema, Long.class, valClass);
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
        if (aClass == Long.class)
            return new Columns(new Column("col0", LONG, true));

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
     */
    private Class<?> createGeneratedObjectClass(int maxFields, Class<?> fieldType) {
        final String packageName = "org.apache.ignite.internal.benchmarks";
        final String className = "TestObject";

        final TypeSpec.Builder classBuilder = TypeSpec.classBuilder(className).addModifiers(Modifier.PUBLIC);

        for (int i = 0; i < maxFields; i++)
            classBuilder.addField(fieldType, "col" + i, Modifier.PRIVATE);

        { // Build constructor.
            final MethodSpec.Builder builder = MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addStatement("$T rnd = new $T()", Random.class, Random.class);

            for (int i = 0; i < maxFields; i++)
                builder.addStatement("col$L = rnd.nextLong()", i);

            classBuilder.addMethod(builder.build());
        }

        final JavaFile javaFile = JavaFile.builder(packageName, classBuilder.build()).build();

        final ClassLoader loader = CompilerUtils.compileCode(javaFile);

        try {
            return loader.loadClass(packageName + '.' + className);
        }
        catch (Exception ex) {
            throw new IllegalStateException("Failed to compile/instantiate generated Serializer.", ex);
        }
    }
}
