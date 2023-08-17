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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class VarIntUtilsTest {
    private final byte[] array = new byte[10];

    @ParameterizedTest
    @MethodSource("sampleInts")
    void readingAndWritingVarintsIsCompatible(int intVal) {
        VarIntUtils.putVarIntToBytes(intVal, array, 1);

        ByteBuffer buf = ByteBuffer.wrap(array);
        buf.position(1);

        assertThat(VarIntUtils.readVarInt(buf), is(intVal));
    }

    @ParameterizedTest
    @MethodSource("sampleInts")
    void writingVarIntReturnsNumberOfBytesInItsRepresentation(int intVal) {
        int len = VarIntUtils.putVarIntToBytes(intVal, array, 0);

        assertThat(VarIntUtils.varIntLength(intVal), is(len));
    }

    @ParameterizedTest
    @MethodSource("sampleInts")
    void readingVarIntConsumesExactlyItsBytes(int intVal) {
        int len = VarIntUtils.putVarIntToBytes(intVal, array, 0);

        ByteBuffer buf = ByteBuffer.wrap(array);

        VarIntUtils.readVarInt(buf);

        assertThat(buf.position(), is(len));
    }

    private static Stream<Arguments> sampleInts() {
        return Stream.of(
                -1, 0, 1,
                127, 128, 255, 256,
                32767, 32768, 65535, 65536,
                8388607, 8388608, 16777215, 16777216,
                2147483647
        ).map(Arguments::of);
    }
}
