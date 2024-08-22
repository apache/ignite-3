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

package org.apache.ignite.marshalling;


import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.BitSet;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ByteArrayMarshallerTest {

    private static Stream<Arguments> primitiveTypes() {
        return Stream.of(
                Byte.valueOf("1"),
                Short.valueOf("1"),
                Integer.valueOf(1),
                Long.valueOf(1),
                Float.valueOf(1),
                Double.valueOf(1),
                Boolean.TRUE,
                new BigDecimal(11),
                new BigDecimal("11.1"),
                UUID.randomUUID(),
                "Hi, I am string",
                new BitSet(1),
                LocalDate.now(),
                LocalDateTime.now(),
                Timestamp.valueOf(LocalDateTime.now()),
                Duration.ofDays(1),
                Period.ofMonths(1)
        ).map(Arguments::of);
    }

    private static Stream<Arguments> cornerValues() {
        return Stream.of(
               "", ".*456+576+$%^&*()_+{}|:<>?`~", "1", "0",
                -1, 0
        ).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("primitiveTypes")
    void marshalPrimitiveTypes(Object obj) {
        ByteArrayMarshaller<Object> marshaller = ByteArrayMarshaller.create();

        assertEquals(obj, marshaller.unmarshal(marshaller.marshal(obj)));
    }

    @Test
    void nullValue() {
        ByteArrayMarshaller<Object> marshaller = ByteArrayMarshaller.create();
        assertNull(marshaller.unmarshal(marshaller.marshal(null)));
    }

    @ParameterizedTest
    @MethodSource("cornerValues")
    void cornerValuesMarshalling(Object obj) {
        ByteArrayMarshaller<Object> marshaller = ByteArrayMarshaller.create();

        assertEquals(obj, marshaller.unmarshal(marshaller.marshal(obj)));
    }


    @Test
    void byteArray() {
        ByteArrayMarshaller<byte[]> marshaller = ByteArrayMarshaller.create();

        byte[] bytes = {1, 2, 3, 4, 5};

        assertArrayEquals(bytes, marshaller.unmarshal(marshaller.marshal(bytes)));
    }

    @Test
    void javaInt() {
        ByteArrayMarshaller<Integer> intMarshaller = ByteArrayMarshaller.create();
        assertEquals(2, intMarshaller.unmarshal(intMarshaller.marshal(2)));
    }

    @Test
    void javaString() {
        ByteArrayMarshaller<String> stringMarshaller = ByteArrayMarshaller.create();
        assertEquals("Hello", stringMarshaller.unmarshal(stringMarshaller.marshal("Hello")));
    }

    @Test
    void javaBoolean() {
        ByteArrayMarshaller<Boolean> booleanMarshaller = ByteArrayMarshaller.create();
        assertEquals(true, booleanMarshaller.unmarshal(booleanMarshaller.marshal(true)));
    }

    @Test
    void javaLong() {
        ByteArrayMarshaller<Long> longMarshaller = ByteArrayMarshaller.create();
        assertEquals(2L, longMarshaller.unmarshal(longMarshaller.marshal(2L)));
    }

    @Test
    void javaShort() {
        ByteArrayMarshaller<Short> shortMarshaller = ByteArrayMarshaller.create();
        assertEquals((short) 2, shortMarshaller.unmarshal(shortMarshaller.marshal((short) 2)));
    }

    @Test
    void javaByte() {
        ByteArrayMarshaller<Byte> byteMarshaller = ByteArrayMarshaller.create();
        assertEquals((byte) 2, byteMarshaller.unmarshal(byteMarshaller.marshal((byte) 2)));
    }

    @Test
    void javaFloat() {
        ByteArrayMarshaller<Float> floatMarshaller = ByteArrayMarshaller.create();
        assertEquals(2.0f, floatMarshaller.unmarshal(floatMarshaller.marshal(2.0f)));
    }

    @Test
    void notSerializable() {
        ByteArrayMarshaller<NotSerializable> notSerializableMarshaller = ByteArrayMarshaller.create();

        NotSerializable notSerializable = new NotSerializable(2);

        assertThrows(
                UnsupportedObjectTypeMarshallingException.class,
                () -> notSerializableMarshaller.marshal(notSerializable)
        );
    }

    private static class NotSerializable {
        private final int value;

        private NotSerializable(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }
}
