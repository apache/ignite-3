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

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TupleMarshallingTest {
    private static Stream<Arguments> oneFieldTuple() {
        return Stream.of(
                Tuple.create().set("col", 1),
                Tuple.create().set("col2", true),
                Tuple.create().set("col3", (byte) 1),
                Tuple.create().set("col4", (short) 2),
                Tuple.create().set("col5", 3),
                Tuple.create().set("col6", 4L),
                Tuple.create().set("col7", 5.0f),
                Tuple.create().set("col8", 6.0),
                Tuple.create().set("col9", new BigDecimal("7.1")),
                Tuple.create().set("col10", LocalDate.of(2024, 1, 1)),
                Tuple.create().set("col11", LocalTime.of(12, 0)),
                Tuple.create().set("col12", LocalDate.of(2024, 1, 1).atTime(LocalTime.of(12, 0))),
                Tuple.create().set("col13", UUID.fromString("123e4567-e89b-12d3-a456-426614174000")),
                Tuple.create().set("col14", "string"),
                Tuple.create().set("col15", new byte[]{1, 2, 3}),
                Tuple.create().set("col16", Period.ofDays(10)),
                Tuple.create().set("col17", Duration.ofDays(10))
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
                .set("col9", new BigDecimal("7.11"))
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
}
