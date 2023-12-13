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

import static org.apache.ignite.internal.raft.util.OptimizedMarshaller.DEFAULT_BUFFER_SIZE;
import static org.apache.ignite.internal.raft.util.OptimizedMarshaller.MAX_CACHED_BUFFER_BYTES;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.raft.util.OptimizedMarshaller.ByteBuffersPool;
import org.jetbrains.annotations.Nullable;

/**
 * Basic pool implementation with limited capacity.
 */
public class DefaultByteBuffersPool implements ByteBuffersPool {
    /** Max pool size. */
    private final int capacity;

    /** Queue with cached buffers. */
    private final Queue<ByteBuffer> queue;

    /** Pool size. */
    private final AtomicInteger size = new AtomicInteger();

    /**
     * Constructor.
     *
     * @param capacity Max pool size.
     */
    public DefaultByteBuffersPool(int capacity) {
        this.capacity = capacity;

        queue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public @Nullable ByteBuffer borrow() {
        ByteBuffer buffer = queue.poll();

        if (buffer != null) {
            return buffer;
        }

        if (size.get() < capacity && size.getAndIncrement() < capacity) {
            return ByteBuffer.allocate(DEFAULT_BUFFER_SIZE).order(OptimizedMarshaller.ORDER);
        }

        return null;
    }

    @Override
    public void release(ByteBuffer buffer) {
        assert buffer.position() == 0;
        assert buffer.capacity() <= MAX_CACHED_BUFFER_BYTES;

        queue.add(buffer);
    }
}
