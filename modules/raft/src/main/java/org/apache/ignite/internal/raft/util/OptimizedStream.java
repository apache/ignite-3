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
import org.apache.ignite.internal.network.direct.stream.DirectByteBufferStreamImplV1;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Direct byte-buffer stream implementation that contains specific optimizations for optimized marshaller.
 */
public class OptimizedStream extends DirectByteBufferStreamImplV1 {
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
        return (short) readLong();
    }

    @Override
    public int readInt() {
        return (int) readLong();
    }

    @Override
    public long readLong() {
        long res = 0;

        int pos = buf.position();

        for (int shift = 0; ; shift += 7) {
            byte b = GridUnsafe.getByte(heapArr, baseOff + pos++);

            res |= (b & 0x7FL) << shift;

            if (b >= 0) {
                break;
            }
        }

        buf.position(pos);

        return res - 1;
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
