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

package org.apache.ignite.internal.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.nio.ByteBuffer;
import java.util.stream.Stream;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataOutput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class VarIntUtilsTest {
    @ParameterizedTest
    @MethodSource("sampleLongs")
    void readingAndWritingVarintsInMemoryIsCompatible(long longVal) {
        byte[] array = new byte[10];

        VarIntUtils.putVarIntToBytes(longVal, array, 1);

        ByteBuffer buf = ByteBuffer.wrap(array);
        buf.position(1);

        assertThat(VarIntUtils.readVarInt(buf), is(longVal));
    }

    @ParameterizedTest
    @MethodSource("sampleLongs")
    void readingAndWritingVarintsInIoIsCompatible(long longVal) throws Exception {
        IgniteUnsafeDataOutput out = new IgniteUnsafeDataOutput(100);

        VarIntUtils.writeVarInt(longVal, out);

        IgniteUnsafeDataInput in = new IgniteUnsafeDataInput(out.array());

        assertThat(VarIntUtils.readVarInt(in), is(longVal));
    }

    @ParameterizedTest
    @MethodSource("sampleLongs")
    void writingVarIntInMemoryReturnsNumberOfBytesInItsRepresentation(long longVal) {
        byte[] array = new byte[10];

        int len = VarIntUtils.putVarIntToBytes(longVal, array, 0);

        assertThat(VarIntUtils.varIntLength(longVal), is(len));
    }

    @ParameterizedTest
    @MethodSource("sampleLongs")
    void writingVarIntInIoReturnsNumberOfBytesInItsRepresentation(long longVal) throws Exception {
        IgniteUnsafeDataOutput out = new IgniteUnsafeDataOutput(100);

        int len = VarIntUtils.writeVarInt(longVal, out);

        assertThat(VarIntUtils.varIntLength(longVal), is(len));
    }

    @ParameterizedTest
    @MethodSource("sampleLongs")
    void readingVarIntFromMemoryConsumesExactlyItsBytes(long longVal) {
        byte[] array = new byte[10];

        int len = VarIntUtils.putVarIntToBytes(longVal, array, 0);

        ByteBuffer buf = ByteBuffer.wrap(array);

        VarIntUtils.readVarInt(buf);

        assertThat(buf.position(), is(len));
    }

    @ParameterizedTest
    @MethodSource("sampleLongs")
    void readingVarIntFromIoConsumesExactlyItsBytes(long longVal) throws Exception {
        byte[] array = new byte[10];

        int len = VarIntUtils.putVarIntToBytes(longVal, array, 0);

        IgniteUnsafeDataInput in = new IgniteUnsafeDataInput(array);

        VarIntUtils.readVarInt(in);

        assertThat(in.available(), is(array.length - len));
    }

    private static Stream<Arguments> sampleLongs() {
        return Stream.of(
                -1L, 0L, 1L,
                128L - 2, 128L - 1, 255L, 256L,
                128L * 128 - 2, 128L * 128 - 1,
                65535L, 65536L,
                128L * 128 * 128 - 2, 128L * 128 * 128 - 1,
                16777215L, 16777216L,
                128L * 128 * 128 * 128 - 2, 128L * 128 * 128 * 128 - 1,
                2147483647L
        ).map(Arguments::of);
    }

    @Test
    void testVarIntLength() {
        assertThat(VarIntUtils.varIntLength(-1), is(1));
        assertThat(VarIntUtils.varIntLength(0), is(1));
        assertThat(VarIntUtils.varIntLength(128 - 2), is(1));
        assertThat(VarIntUtils.varIntLength(128 - 1), is(2));
        assertThat(VarIntUtils.varIntLength(128 * 128 - 2), is(2));
        assertThat(VarIntUtils.varIntLength(128 * 128 - 1), is(3));
        assertThat(VarIntUtils.varIntLength(128 * 128 * 128 - 2), is(3));
        assertThat(VarIntUtils.varIntLength(128 * 128 * 128 - 1), is(4));
        assertThat(VarIntUtils.varIntLength(128 * 128 * 128 * 128 - 2), is(4));
        assertThat(VarIntUtils.varIntLength(128 * 128 * 128 * 128 - 1), is(5));
    }
}
