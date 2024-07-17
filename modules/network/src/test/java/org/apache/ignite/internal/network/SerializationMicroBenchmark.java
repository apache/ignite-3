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

package org.apache.ignite.internal.network;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.network.messages.AllTypesMessage;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.marshal.DefaultUserObjectMarshaller;
import org.apache.ignite.internal.network.serialization.marshal.MarshalException;
import org.apache.ignite.internal.network.serialization.marshal.MarshalledObject;
import org.apache.ignite.internal.network.serialization.marshal.UnmarshalException;
import org.apache.ignite.internal.network.serialization.marshal.UserObjectMarshaller;
import org.jetbrains.annotations.Nullable;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * A micro-benchmark of {@link DefaultUserObjectMarshaller}.
 */
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class SerializationMicroBenchmark {

    static TestClass smallObject;
    static AllTypesMessage mediumObject;
    static AllTypesMessage largeObject;

    static ClassDescriptorRegistry registry;

    static UserObjectMarshaller userObjectMarshaller;

    static Kryo kryo;

    static byte[] smallSerializedWithUos;
    static byte[] smallSerializedWithJava;
    static byte[] smallSerializedWithKryo;

    static byte[] mediumSerializedWithUos;
    static byte[] mediumSerializedWithJava;
    static byte[] mediumSerializedWithKryo;

    static byte[] largeSerializedWithUos;
    static byte[] largeSerializedWithJava;
    static byte[] largeSerializedWithKryo;

    static {
        registry = new ClassDescriptorRegistry();
        var factory = new ClassDescriptorFactory(registry);
        userObjectMarshaller = new DefaultUserObjectMarshaller(registry, factory);

        smallObject = new TestClass(1000, false, 3000);
        mediumObject = AllTypesMessageGenerator.generate(10, true, false);
        largeObject = AllTypesMessageGenerator.generate(10, true, true);

        factory.create(smallObject.getClass());
        factory.create(largeObject.getClass());

        kryo = new Kryo();
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());

        try {
            smallSerializedWithUos = userObjectMarshaller.marshal(smallObject).bytes();
            mediumSerializedWithUos = userObjectMarshaller.marshal(mediumObject).bytes();
            largeSerializedWithUos = userObjectMarshaller.marshal(largeObject).bytes();
        } catch (MarshalException e) {
            throw new RuntimeException(e);
        }

        smallSerializedWithJava = serializeWithJdk(smallObject);
        mediumSerializedWithJava = serializeWithJdk(mediumObject);
        largeSerializedWithJava = serializeWithJdk(largeObject);

        smallSerializedWithKryo = serializeWithKryo(smallObject);
        mediumSerializedWithKryo = serializeWithKryo(mediumObject);
        largeSerializedWithKryo = serializeWithKryo(largeObject);

        System.out.println("Small Java: " + smallSerializedWithJava.length);
        System.out.println("Small UOS : " + smallSerializedWithUos.length);
        System.out.println("Small Kryo: " + smallSerializedWithKryo.length);

        System.out.println("Medium Java: " + mediumSerializedWithJava.length);
        System.out.println("Medium UOS : " + mediumSerializedWithUos.length);
        System.out.println("Medium Kryo: " + mediumSerializedWithKryo.length);

        System.out.println("Large Java: " + largeSerializedWithJava.length);
        System.out.println("Large UOS : " + largeSerializedWithUos.length);
        System.out.println("Large Kryo: " + largeSerializedWithKryo.length);
    }

    /**
     * Runs the benchmark.
     *
     * @param args args
     * @throws Exception if something goes wrong
     */
    public static void main(String[] args) throws Exception {
        Options build = new OptionsBuilder()
                // .addProfiler("gc")
                .include(SerializationMicroBenchmark.class.getName() + ".*").build();

        new Runner(build).run();
    }

    @Benchmark
    public byte[] serialization_01_small_jdk() {
        return serializeWithJdk(smallObject);
    }

    @Benchmark
    public byte[] serialization_11_medium_jdk() {
        return serializeWithJdk(mediumObject);
    }

    @Benchmark
    public byte[] serialization_21_large_jdk() {
        return serializeWithJdk(largeObject);
    }

    @Benchmark
    public void serialization_02_small_uos(Blackhole blackhole) {
        benchmarkUosSerialization(smallObject, blackhole);
    }

    @Benchmark
    public void serialization_12_medium_uos(Blackhole blackhole) {
        benchmarkUosSerialization(mediumObject, blackhole);
    }

    @Benchmark
    public void serialization_22_large_uos(Blackhole blackhole) {
        benchmarkUosSerialization(largeObject, blackhole);
    }

    private void benchmarkUosSerialization(Object obj, Blackhole blackhole) {
        try {
            MarshalledObject marshal = userObjectMarshaller.marshal(obj);

            blackhole.consume(marshal);
            blackhole.consume(marshal.bytes());
        } catch (MarshalException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public byte[] serialization_03_small_kryo() {
        return serializeWithKryo(smallObject);
    }

    @Benchmark
    public byte[] serialization_13_medium_kryo() {
        return serializeWithKryo(mediumObject);
    }

    @Benchmark
    public byte[] serialization_23_large_kryo() {
        return serializeWithKryo(largeObject);
    }

    @Benchmark
    public Object deserialization_01_small_jdk() {
        return deserializeWithJava(SerializationMicroBenchmark.smallSerializedWithJava);
    }

    @Benchmark
    public Object deserialization_11_medium_jdk() {
        return deserializeWithJava(SerializationMicroBenchmark.mediumSerializedWithJava);
    }

    @Benchmark
    public Object deserialization_21_large_jdk() {
        return deserializeWithJava(SerializationMicroBenchmark.largeSerializedWithJava);
    }

    private Object deserializeWithJava(byte[] serialized) {
        try (var bis = new ByteArrayInputStream(serialized); var ois = new ObjectInputStream(bis)) {
            return ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public Object deserialization_02_small_uos() {
        return deserializeWithUos(smallSerializedWithUos);
    }

    @Benchmark
    public Object deserialization_12_medium_uos() {
        return deserializeWithUos(mediumSerializedWithUos);
    }

    @Benchmark
    public Object deserialization_22_large_uos() {
        return deserializeWithUos(largeSerializedWithUos);
    }

    @Nullable
    private Object deserializeWithUos(byte[] serialized) {
        try {
            return userObjectMarshaller.unmarshal(serialized, registry);
        } catch (UnmarshalException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public Object deserialization_03_small_kryo() {
        return deserializeWithKryo(smallSerializedWithKryo);
    }

    @Benchmark
    public Object deserialization_13_medium_kryo() {
        return deserializeWithKryo(mediumSerializedWithKryo);
    }

    @Benchmark
    public Object deserialization_23_large_kryo() {
        return deserializeWithKryo(largeSerializedWithKryo);
    }

    private Object deserializeWithKryo(byte[] serialized) {
        ByteArrayInputStream stream = new ByteArrayInputStream(serialized);
        try (Input input = new Input(stream)) {
            return kryo.readClassAndObject(input);
        }
    }

    private static byte[] serializeWithJdk(Object obj) {
        try (var bos = new ByteArrayOutputStream(); var oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);

            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] serializeWithKryo(Object obj) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try (Output output = new Output(stream)) {
            kryo.writeClassAndObject(output, obj);
            output.close();

            return stream.toByteArray();
        }
    }

    static class TestClass implements Serializable {

        private int foo;

        private boolean bar;

        private double foobar;

        public TestClass() {
        }

        public TestClass(int foo, boolean bar, double foobar) {
            this.foo = foo;
            this.bar = bar;
            this.foobar = foobar;
        }
    }

}
