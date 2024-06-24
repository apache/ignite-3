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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.apache.ignite.internal.network.direct.stream.DirectByteBufferStreamImplV1;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;

/**
 * Direct byte-buffer stream implementation that contains specific optimizations for optimized marshaller.
 */
public class OptimizedStream extends DirectByteBufferStreamImplV1 {
    /** Masks for {@link #readInt()}. */
    private static final int[] MASKS_32 = {
            0,
            0,
            0xFFFFFF80,
            0x3F80,
            0xFFE03F80,
            0xFE03F80
    };

    /** Masks for {@link #readLong()}. */
    private static final long[] MASKS_64 = {
            0,
            0,
            0xFFFFFFFFFFFFFF80L,
            0x3F80L,
            0xFFFFFFFFFFE03F80L,
            0xFE03F80L,
            0xFFFFFFF80FE03F80L,
            0x3F80FE03F80L,
            0xFFFE03F80FE03F80L,
            0xFE03F80FE03F80L,
            0x80FE03F80FE03F80L
    };

    /**
     * Constructor.
     *
     * @param serializationRegistry Serialization registry.
     */
    public OptimizedStream(MessageSerializationRegistry serializationRegistry) {
        super(serializationRegistry);

        lastFinished = true;
    }

    @Override
    public short readShort() {
        return (short) readInt();
    }

    //TODO Benchmark against protobuf, it has the same binary format. https://issues.apache.org/jira/browse/IGNITE-22559
    @Override
    public int readInt() {
        int res = 0;

        int pos = buf.position();
        int startPos = pos;

        int b = heapArr[pos++];

        // Fast-path.
        if (b >= 0) {
            setPosition(pos);

            return b - 1;
        }

        for (int shift = 0; ; shift += 7) {
            // Instead of nullifying a sign bit of "b", we extend it into "int" and use "^" instead of "|".
            // It makes arithmetic on every iteration noticeable simpler, which helps in benchmarks.
            // Accumulated error (xor of all extended sign bits) is removed using "MASKS_32" at the end.
            res ^= b << shift;
            if (b < 0) {
                b = heapArr[pos++];
            } else {
                res ^= MASKS_32[pos - startPos];

                setPosition(pos);

                return res - 1;
            }
        }
    }

    //TODO Benchmark against protobuf, it has the same binary format. https://issues.apache.org/jira/browse/IGNITE-22559
    @Override
    public long readLong() {
        long res = 0;

        int pos = buf.position();
        int startPos = pos;

        for (int shift = 0; ; shift += 7) {
            long b = heapArr[pos++];

            // Instead of nullifying a sign bit of "b", we extend it into "int" and use "^" instead of "|".
            // It makes arithmetic on every iteration noticeable simpler, which helps in benchmarks.
            // Accumulated error (xor of all extended sign bits) is removed using "MASKS_64" at the end.
            res ^= b << shift;
            if (b >= 0) {
                res ^= MASKS_64[pos - startPos];

                setPosition(pos);

                return res - 1;
            }
        }
    }

    @Override
    public String readString() {
        int len = readInt();

        if (len == -1) {
            return null;
        } else if (len == 0) {
            return "";
        } else {
            // In this implementation we read string directly from buffer instead of allocating additional byte array.
            int pos = buf.position();

            setPosition(pos + len);

            return new String(heapArr, buf.arrayOffset() + pos, len, StandardCharsets.UTF_8);
        }
    }

    @Override
    public ByteBuffer readByteBuffer() {
        assert buf.hasArray();

        byte flag = readByte();

        if ((flag & BYTE_BUFFER_NOT_NULL_FLAG) == 0) {
            return null;
        }

        int length = readInt();

        int oldLimit = buf.limit();

        try {
            ByteBuffer val = buf.limit(buf.position() + length).slice();

            buf.position(buf.limit());

            if ((flag & BYTE_BUFFER_BIG_ENDIAN_FLAG) == 0) {
                val.order(ByteOrder.LITTLE_ENDIAN);
            } else {
                val.order(ByteOrder.BIG_ENDIAN);
            }

            return val;
        } finally {
            buf.limit(oldLimit);
        }
    }
}
