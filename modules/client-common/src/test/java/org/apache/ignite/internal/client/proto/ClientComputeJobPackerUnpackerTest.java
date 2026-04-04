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

import static org.apache.ignite.internal.client.proto.ClientComputeJobPacker.packJobArgument;
import static org.apache.ignite.internal.client.proto.ClientComputeJobPacker.packJobResult;
import static org.apache.ignite.internal.client.proto.ClientComputeJobUnpacker.unpackJobArgumentWithoutMarshaller;
import static org.apache.ignite.internal.client.proto.ClientComputeJobUnpacker.unpackJobResult;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.internal.client.proto.pojo.Pojo;
import org.apache.ignite.internal.client.proto.pojo.StaticFieldPojo;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;
import org.apache.ignite.internal.compute.ComputeJobDataType;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.marshalling.MarshallingException;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings("ThrowableNotThrown")
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

    private static Stream<Arguments> tupleCollections() {
        return Stream.of(
                List.of(),
                Collections.singletonList(Tuple.create()),
                Collections.singletonList(null),
                List.of(Tuple.create(), Tuple.create().set("key", 1), Tuple.create().set("key", "value1")),
                Set.of(Tuple.create().set("key", 2), Tuple.create().set("key", "value2"))
        ).map(Arguments::of);
    }

    private static List<Object> pojo() {
        return List.of(Pojo.generateTestPojo());
    }

    private static List<Arguments> notMarshalled() {
        return List.of(
                arguments(Tuple.create(), ComputeJobDataType.TUPLE),
                arguments(Pojo.generateTestPojo(), ComputeJobDataType.POJO)
        );
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

    private static ClientMessageUnpacker messageUnpacker(byte[] data) {
        return new ClientMessageUnpacker(Unpooled.wrappedBuffer(data, 4, data.length - 4));
    }

    @MethodSource({"tuples", "nativeTypes", "tupleCollections"})
    @ParameterizedTest
    void packUnpackNoMarshalling(Object arg) {
        // When pack job result without marshaller.
        packJobResult(arg, null, messagePacker);
        byte[] data = ByteBufUtil.getBytes(messagePacker.getBuffer());

        // And unpack job result without marshaller.
        try (var messageUnpacker = messageUnpacker(data)) {
            var res = unpackJobResult(messageUnpacker, null, null);

            // Then.
            if (arg instanceof Collection<?>) {
                assertIterableEquals((Collection<?>) arg, (Collection<?>) res);
            } else {
                assertEquals(arg, res);
            }
        }
    }

    @MethodSource("notMarshalled")
    @ParameterizedTest
    void notMarshalledArgument(Object arg, ComputeJobDataType type) {
        // When pack job argument without marshaller.
        packJobArgument(arg, null, messagePacker, null);
        byte[] data = ByteBufUtil.getBytes(messagePacker.getBuffer());

        // And unpack without marshaller.
        try (var messageUnpacker = messageUnpacker(data)) {
            var res = unpackJobArgumentWithoutMarshaller(messageUnpacker, false);

            // Then argument is unpacked but not unmarshalled.
            ComputeJobDataHolder argument = assertInstanceOf(ComputeJobDataHolder.class, res);
            assertEquals(type, argument.type());
        }
    }

    @MethodSource("pojo")
    @ParameterizedTest
    void packUnpackPojo(Object arg) {
        // When pack job result without marshaller.
        packJobResult(arg, null, messagePacker);
        byte[] data = ByteBufUtil.getBytes(messagePacker.getBuffer());

        // And unpack job result without marshaller.
        try (var messageUnpacker = messageUnpacker(data)) {
            var res = unpackJobResult(messageUnpacker, null, arg.getClass());

            // Then.
            assertEquals(arg, res);
        }
    }

    @Test
    void marshallingPackUnpack() {
        // Given.
        Marshaller<String, byte[]> marshaller = new TestStringMarshaller();
        var str = "Hi, marshal me!";

        // When pack job result with marshaller.
        packJobResult(str, marshaller, messagePacker);
        byte[] data = ByteBufUtil.getBytes(messagePacker.getBuffer());

        // And unpack job result with marshaller.
        try (var messageUnpacker = messageUnpacker(data)) {
            Object res = unpackJobResult(messageUnpacker, marshaller, null);

            // Then.
            assertEquals(str, res);
        }
    }

    @Test
    void packWithMarshallerUnpackWithout() {
        // Given.
        Marshaller<String, byte[]> marshaller = new TestStringMarshaller();
        var str = "Hi, marshal me!";

        // When pack job result with marshaller.
        packJobResult(str, marshaller, messagePacker);
        byte[] data = ByteBufUtil.getBytes(messagePacker.getBuffer());

        // And unpack job result without marshaller.
        try (var messageUnpacker = messageUnpacker(data)) {
            // Then the exception is thrown because it is not allowed unpack the marshalled object without marshaller.
            assertThrows(
                    ComputeException.class,
                    () -> unpackJobResult(messageUnpacker, null, null),
                    "ComputeJob.resultMarshaller is defined, but the JobDescriptor.resultMarshaller is not defined."
            );
        }
    }

    @Test
    void packByteArrayUnpackStringWithMarshaller() {
        // Given.
        var str = "Hi, marshal me!";
        var bytes = str.getBytes();

        // When pack job result without marshaller.
        packJobResult(bytes, null, messagePacker);
        byte[] data = ByteBufUtil.getBytes(messagePacker.getBuffer());

        // And unpack job result with marshaller.
        try (var messageUnpacker = messageUnpacker(data)) {
            // Then the exception is thrown because it is not allowed to define the marshaller only for the result.
            assertThrows(
                    ComputeException.class,
                    () -> unpackJobResult(messageUnpacker, new TestStringMarshaller(), null),
                    "JobDescriptor.resultMarshaller is defined, but the ComputeJob.resultMarshaller is not defined."
            );
        }
    }

    @Test
    void packInvalidPojoStatic() {
        assertThrows(
                MarshallingException.class,
                () -> packJobResult(new StaticFieldPojo(), null, messagePacker),
                "Can't pack object"
        );
    }

    @Test
    void packInvalidPojoEmpty() {
        assertThrows(
                MarshallingException.class,
                () -> packJobResult(new Object(), null, messagePacker),
                "Can't pack object"
        );
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
