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

package org.apache.ignite.internal.raft.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.GridUnsafe;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

class VarlenEncoderTest {
    @SuppressWarnings("unused")
    private static final long[] TEST_VALUES = {
            0L,
            1L,
            127L,            // 1 byte boundary
            128L,            // 2 bytes start
            (1L << 14) - 1,  // 2 bytes end
            (1L << 14),      // 3 bytes start
            (1L << 21) - 1,
            (1L << 21),
            (1L << 28) - 1,
            (1L << 28),
            (1L << 35),
            (1L << 42),
            (1L << 49),
            (1L << 56) - 1,
            Long.MAX_VALUE,

            -1L,
            -2L,
            -127L,
            -128L,
            -129L,
            -(1L << 7),
            -(1L << 14),
            -(1L << 21),
            -(1L << 28),
            -(1L << 35),
            -(1L << 42),
            -(1L << 49),
            -(1L << 56),
            Long.MIN_VALUE
    };

    @ParameterizedTest
    @FieldSource("TEST_VALUES")
    void testWriteToBuffer(long value) {
        ByteBuffer buf = ByteBuffer.allocate(10);

        int start = buf.position();

        int written = VarlenEncoder.writeLong(value, buf);

        assertThat(written, is(VarlenEncoder.sizeInBytes(value)));

        assertThat(buf.position(), is(start + written));

        buf.rewind();

        assertThat(VarlenEncoder.readLong(buf), is(value));
    }

    @ParameterizedTest
    @FieldSource("TEST_VALUES")
    void testWriteToDirectBufferUsingAddr(long value) {
        ByteBuffer buf = ByteBuffer.allocateDirect(10);

        int written = VarlenEncoder.writeLong(value, GridUnsafe.bufferAddress(buf));

        assertThat(written, is(VarlenEncoder.sizeInBytes(value)));

        assertThat(VarlenEncoder.readLong(buf), is(value));
    }
}
