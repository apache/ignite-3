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

import static org.apache.ignite.internal.network.direct.DirectMessageWriter.EMPTY_BYTE_BUFFER;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.direct.DirectMessageReader;
import org.apache.ignite.internal.network.direct.DirectMessageWriter;
import org.apache.ignite.internal.network.direct.stream.DirectByteBufferStream;
import org.apache.ignite.internal.network.direct.stream.DirectByteBufferStreamImplV1;
import org.apache.ignite.internal.network.serialization.MessageReader;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.MessageWriter;
import org.apache.ignite.internal.raft.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Marshaller implementation that uses a {@link DirectByteBufferStream} variant to serialize/deserialize data.
 */
public class OptimizedMarshaller implements Marshaller {
    /**
     * Byte buffer pool for {@link OptimizedMarshaller}. Helps re-using old buffers, saving some time on allocations.
     */
    public interface ByteBuffersPool {
        /**
         * Removes one buffer from cache and returns it, if possible. Returns {@code null} otherwise.
         */
        @Nullable ByteBuffer borrow();

        /**
         * Adds a buffer back to the pool. Should only be called if previous {@link #borrow()} call returned a non-null buffer.
         *
         * @param buffer The buffer to add back to the pool. Its capacity must not be higher than
         *      {@link OptimizedMarshaller#MAX_CACHED_BUFFER_BYTES} or the sake of controlling the amount of RAM. If the capacity is higher,
         *      the behavior is undefined.
         */
        void release(ByteBuffer buffer);
    }

    /** Default buffer size. */
    public static final int DEFAULT_BUFFER_SIZE = 1024;
    /** Maximal size of the buffer that can be stored in the pool. */
    public static final int MAX_CACHED_BUFFER_BYTES = 256 * 1024;
    /** Default "no pool" instance for always-empty pool. */
    public static final ByteBuffersPool NO_POOL = new EmptyByteBuffersPool();

    /** Protocol version. */
    private static final byte PROTO_VER = 1;

    /** Byte buffer order. */
    public static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;

    /** Pool of byte buffers. */
    private final ByteBuffersPool pool;

    /** Direct byte-buffer stream instance. */
    protected final OptimizedStream stream;

    /** Message writer. */
    private final MessageWriter messageWriter;

    /** Message reader. */
    private final MessageReader messageReader;

    /**
     * Constructor.
     *
     * @param serializationRegistry Serialization registry.
     * @param pool Pool of byte buffers.
     */
    public OptimizedMarshaller(MessageSerializationRegistry serializationRegistry, ByteBuffersPool pool) {
        this.pool = pool;
        stream = new OptimizedStream(serializationRegistry);

        messageWriter = new DirectMessageWriter(serializationRegistry, PROTO_VER) {
            @Override
            protected DirectByteBufferStreamImplV1 createStream(MessageSerializationRegistry serializationRegistry, byte protoVer) {
                assert protoVer == PROTO_VER : protoVer;

                return new OptimizedStream(serializationRegistry);
            }
        };

        messageReader = new DirectMessageReader(serializationRegistry, PROTO_VER) {
            @Override
            protected DirectByteBufferStream createStream(MessageSerializationRegistry serializationRegistry, byte protoVer) {
                assert protoVer == PROTO_VER : protoVer;

                return new OptimizedStream(serializationRegistry);
            }
        };
    }

    @Override
    public byte[] marshall(Object o) {
        assert o instanceof NetworkMessage;

        ByteBuffer poolBuffer = pool.borrow();

        ByteBuffer buffer = poolBuffer == null ? ByteBuffer.allocate(DEFAULT_BUFFER_SIZE).order(ORDER) : poolBuffer;

        NetworkMessage message = (NetworkMessage) o;

        beforeWriteMessage(o, buffer);

        while (true) {
            stream.setBuffer(buffer);

            stream.writeMessage(message, messageWriter);

            if (stream.lastFinished()) {
                break;
            }

            buffer = expandBuffer(buffer);

            if (buffer.capacity() <= MAX_CACHED_BUFFER_BYTES && poolBuffer != null) {
                poolBuffer = buffer;
            } else if (poolBuffer != null) {
                poolBuffer.position(0);
                pool.release(poolBuffer);

                poolBuffer = null;
            }
        }

        // Prevent holding the reference for too long.
        stream.setBuffer(EMPTY_BYTE_BUFFER);

        byte[] result = Arrays.copyOf(buffer.array(), buffer.position());

        if (poolBuffer != null) {
            poolBuffer.position(0);
            pool.release(poolBuffer);
        }

        return result;
    }

    /**
     * Invoked on empty buffer, before writing any data to it.
     */
    protected void beforeWriteMessage(Object o, ByteBuffer buffer) {
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unmarshall(ByteBuffer bytes) {
        stream.setBuffer(bytes.duplicate().order(ORDER));

        return stream.readMessage(messageReader);
    }

    /**
     * Creates a bigger copy of the buffer.
     *
     * @param buffer Smaller byte buffer.
     * @return Bigger byte buffer.
     */
    private ByteBuffer expandBuffer(ByteBuffer buffer) {
        byte[] newArray = Arrays.copyOf(buffer.array(), (int) (buffer.capacity() * 1.5));

        return ByteBuffer.wrap(newArray).position(buffer.position()).order(ORDER);
    }
}
