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

package org.apache.ignite.benchmarks;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import org.apache.ignite.internal.binarytuple.inlineschema.TupleMarshalling;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Tuple marshalling benchmark.
 */
@State(Scope.Benchmark)
public class TupleMarshallingBenchmark {
    Tuple tuple;
    Kryo kryo;
    ByteArrayOutputStream baus;
    Output output;
    Input input;

    /**
     * Initialize.
     */
    @Setup
    public void init() {
        tuple = Tuple.create()
                .set("col1", null)
                .set("col2", true)
                .set("col3", (byte) 1)
                .set("col4", (short) 2)
                .set("col5", 3)
                .set("col6", 4L)
                .set("col7", 5.0f)
                .set("col8", 6.0)
                .set("col9", new BigDecimal("7.0"))
                .set("col10", LocalDate.of(2024, 1, 1))
                .set("col11", LocalTime.of(12, 0))
                .set("col12", LocalDate.of(2024, 1, 1).atTime(LocalTime.of(12, 0)))
                .set("col14", "string")
                .set("col15", new byte[]{1, 2, 3})
                .set("col16", Period.ofDays(10))
                .set("col17", Duration.ofDays(10));

        kryo = new Kryo();
    }

    /**
     * Tear down.
     *
     * @throws Exception If failed.
     */
    @TearDown
    public void tearDown() throws Exception {
        tuple = null;
        kryo = null;
        baus = null;
        output = null;
        input = null;
    }

    /**
     * Round trip with inline schema.
     */
    @Benchmark
    public void roundWithInlineSchema() {
        TupleMarshalling.unmarshal(TupleMarshalling.marshal(tuple));
    }

    /**
     * Round trip with Kryo.
     *
     * @param blackhole Blackhole.
     * @throws IOException If failed.
     */
    @Benchmark
    public void roundWithKryo(Blackhole blackhole) throws IOException {
        baus = new ByteArrayOutputStream();
        output = new Output(baus);
        kryo.writeObject(output, tuple);
        output.flush();
        input = new Input(new ByteArrayInputStream(baus.toByteArray()));
        Tuple theObject = kryo.readObject(input, TupleImpl.class);
        blackhole.consume(theObject);
    }

    /**
     * Round trip with Java serialization.
     *
     * @throws IOException If failed.
     * @throws ClassNotFoundException If failed.
     */
    @Benchmark
    public void roundWithJavaSerialization() throws IOException, ClassNotFoundException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(tuple);
            out.flush();
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        try (ObjectInputStream in = new ObjectInputStream(bis)) {
            in.readObject();
        }
    }
}
