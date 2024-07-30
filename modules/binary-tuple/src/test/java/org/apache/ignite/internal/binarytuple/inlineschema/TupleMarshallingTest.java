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

package org.apache.ignite.internal.binarytuple.inlineschema;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TupleMarshallingTest {
    static Stream<Arguments> oneFieldTuple() {
        return Stream.of(
                Tuple.create().set("col", 1)
        ).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("oneFieldTuple")
    void serDeOneFieldTuple(Tuple tuple) {
        byte[] marshalled = TupleMarshalling.marshal(tuple);
        assertEquals(tuple, TupleMarshalling.unmarshal(marshalled));
    }

    @Test
    void allFieldsTupleTest() {
        Tuple tuple = Tuple.create()
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
                .set("col13", UUID.fromString("123e4567-e89b-12d3-a456-426614174000"))
                .set("col14", "string")
                .set("col15", new byte[]{1, 2, 3})
                .set("col16", Period.ofDays(10))
                .set("col17", Duration.ofDays(10));

        byte[] marshalled = TupleMarshalling.marshal(tuple);
        assertEquals(tuple, TupleMarshalling.unmarshal(marshalled));
    }

    @Test
    void jacksonTest() throws IOException {
        Tuple tuple = Tuple.create()
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
//                .set("col13", UUID.fromString("123e4567-e89b-12d3-a456-426614174000"))
                .set("col14", "string")
                .set("col15", new byte[]{1, 2, 3})
                .set("col16", Period.ofDays(10))
                .set("col17", Duration.ofDays(10));

        var kryo = new Kryo();
        var baus = new ByteArrayOutputStream();
        var output = new Output(baus);
        kryo.writeObject(output, tuple);
        output.flush();

        var input = new Input(new ByteArrayInputStream(baus.toByteArray()));


        Tuple theObject = kryo.readObject(input, TupleImpl.class);
        input.close();

        assertEquals(tuple, theObject);
    }
}
