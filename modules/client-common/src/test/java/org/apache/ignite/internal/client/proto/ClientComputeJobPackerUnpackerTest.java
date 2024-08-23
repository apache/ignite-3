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

package org.apache.ignite.internal.client.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.marshalling.UnmarshallingException;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ClientComputeJobPackerUnpackerTest {
    private static Stream<Arguments> nativeTypes() {
        return Stream.of(
                (byte) 4, (short) 8, 15, 16L, 23.0f, 42.0d, "TEST_STRING", null, UUID.randomUUID(),
                LocalTime.now(), LocalDate.now(), LocalDateTime.now(), Instant.now(), Period.of(1, 2, 3),
                Duration.of(1, ChronoUnit.DAYS)
        ).map(Arguments::of);
    }

    private static Stream<Arguments> tuples() {
        return Stream.of(
                null,
                Tuple.create(),
                Tuple.create().set("key", 1),
                Tuple.create().set("key", "value"),
                Tuple.create().set("col1", null).set("col2", true).set("col3", (byte) 1).set("col4", (short) 2).set("col5", 3)
                        .set("col6", 4L).set("col7", 5.0f).set("col8", 6.0).set("col9", new BigDecimal("7.11"))
                        .set("col10", LocalDate.of(2024, 1, 1)).set("col11", LocalTime.of(12, 0))
                        .set("col12", LocalDate.of(2024, 1, 1).atTime(LocalTime.of(12, 0)))
                        .set("col13", UUID.fromString("123e4567-e89b-12d3-a456-426614174000")).set("col14", "string")
                        .set("col15", new byte[]{1, 2, 3}).set("col16", Period.ofDays(10)).set("col17", Duration.ofDays(10))
        ).map(Arguments::of);
    }

    private ClientMessagePacker messagePacker;

    @BeforeEach
    void setUp() {
        messagePacker = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer());
    }

    @AfterEach
    void tearDown() {
        messagePacker.close();
    }

    @MethodSource({"tuples", "nativeTypes"})
    @ParameterizedTest
    void packUnpackNoMarshalling_jobArgument(Object arg) {
        // When pack job argument without marshaller.
        ClientComputeJobPacker.packJobArgument(arg, null, messagePacker);
        byte[] data = ByteBufUtil.getBytes(messagePacker.getBuffer());

        // And unpack job argument without marshaller.
        try (var messageUnpacker = messageUnpacker(data)) {
            var res = ClientComputeJobUnpacker.unpackJobArgument(null, messageUnpacker);

            // Then.
            assertEquals(arg, res);
        }
    }

    private ClientMessageUnpacker messageUnpacker(byte[] data) {
        return new ClientMessageUnpacker(Unpooled.wrappedBuffer(data, 4, data.length - 4));
    }

    @MethodSource({"tuples", "nativeTypes"})
    @ParameterizedTest
    void packUnpackNoMarshalling_jobResult(Object arg) {
        // When pack job result without marshaller.
        ClientComputeJobPacker.packJobResult(arg, null, messagePacker);
        byte[] data = ByteBufUtil.getBytes(messagePacker.getBuffer());

        // And unpack job result without marshaller.
        try (var messageUnpacker = messageUnpacker(data)) {
            var res = ClientComputeJobUnpacker.unpackJobResult(null, messageUnpacker);

            // Then.
            assertEquals(arg, res);
        }
    }

    @Test
    void marshallingPackUnpack_jobResult() {
        // Given.
        Marshaller<String, byte[]> marshaller = new TestStringMarshaller();
        var str = "Hi, marshal me!";

        // When pack job result with marshaller.
        ClientComputeJobPacker.packJobResult(str, marshaller, messagePacker);
        byte[] data = ByteBufUtil.getBytes(messagePacker.getBuffer());

        // And unpack job result with marshaller.
        try (var messageUnpacker = messageUnpacker(data)) {
            Object res = ClientComputeJobUnpacker.unpackJobResult(marshaller, messageUnpacker);

            // Then.
            assertEquals(str, res);
        }
    }

    @Test
    void marshallingPackUnpack_jobArgument() {
        // Given.
        Marshaller<String, byte[]> marshaller = new TestStringMarshaller();
        var str = "Hi, marshal me!";

        // When pack job argument with marshaller.
        ClientComputeJobPacker.packJobArgument(str, marshaller, messagePacker);
        byte[] data = ByteBufUtil.getBytes(messagePacker.getBuffer());

        // And unpack job argument with marshaller.
        try (var messageUnpacker = messageUnpacker(data)) {
            Object res = ClientComputeJobUnpacker.unpackJobArgument(marshaller, messageUnpacker);

            // Then.
            assertEquals(str, res);
        }
    }

    @Test
    void packWithMarshallerUnpackWithout_jobResult() {
        // Given.
        Marshaller<String, byte[]> marshaller = new TestStringMarshaller();
        var str = "Hi, marshal me!";

        // When pack job result with marshaller.
        ClientComputeJobPacker.packJobResult(str, marshaller, messagePacker);
        byte[] data = ByteBufUtil.getBytes(messagePacker.getBuffer());

        // And unpack job result without marshaller.
        try (var messageUnpacker = messageUnpacker(data)) {
            // Then the exception is thrown because it is not allowed unpack the marshalled object without marshaller.
            assertThrows(
                    UnmarshallingException.class,
                    () -> ClientComputeJobUnpacker.unpackJobResult(null, messageUnpacker)
            );
        }
    }

    @Test
    void packWithMarshallerUnpackWithout_jobArgument() {
        // Given.
        Marshaller<String, byte[]> marshaller = new TestStringMarshaller();
        var str = "Hi, marshal me!";

        // When pack job argument with marshaller.
        ClientComputeJobPacker.packJobArgument(str, marshaller, messagePacker);
        byte[] data = ByteBufUtil.getBytes(messagePacker.getBuffer());

        // And unpack job argument without marshaller.
        try (var messageUnpacker = messageUnpacker(data)) {
            // Then the exception is thrown because it is not allowed unpack the marshalled object without marshaller.
            assertThrows(
                    UnmarshallingException.class,
                    () -> ClientComputeJobUnpacker.unpackJobArgument(null, messageUnpacker)
            );
        }
    }

    @Test
    void packByteArrayUnpackStringWithMarshaller_jobResult() {
        // Given.
        var str = "Hi, marshal me!";
        var bytes = str.getBytes();

        // When pack job result without marshaller.
        ClientComputeJobPacker.packJobResult(bytes, null, messagePacker);
        byte[] data = ByteBufUtil.getBytes(messagePacker.getBuffer());

        // And unpack job result with marshaller.
        try (var messageUnpacker = messageUnpacker(data)) {
            // Then the exception is thrown because it is not allowed to define the marshaller only for the result.
            assertThrows(
                    UnmarshallingException.class,
                    () -> ClientComputeJobUnpacker.unpackJobResult(new TestStringMarshaller(), messageUnpacker)
            );
        }
    }

    @Test
    void packByteArrayUnpackStringWithMarshaller_jobArgument() {
        // Given.
        var str = "Hi, marshal me!";
        byte[] bytes = str.getBytes();

        // When pack job argument without marshaller.
        ClientComputeJobPacker.packJobArgument(bytes, null, messagePacker);
        byte[] data = ByteBufUtil.getBytes(messagePacker.getBuffer());

        // And unpack job argument with marshaller.
        try (var messageUnpacker = messageUnpacker(data)) {
            // Then the exception is thrown because it is not allowed to define the marshaller only for the result.
            assertThrows(
                    UnmarshallingException.class,
                    () -> ClientComputeJobUnpacker.unpackJobArgument(new TestStringMarshaller(), messageUnpacker)
            );
        }
    }

    private static class TestStringMarshaller implements Marshaller<String, byte[]> {
        @Override
        public byte[] marshal(String obj) {
            return obj.getBytes();
        }

        @Override
        public String unmarshal(byte[] bytes) {
            return new String(bytes);
        }
    }
}
