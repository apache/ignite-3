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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.Nullable;

/**
 * Basic cache implementation with limited capacity.
 */
public class DefaultByteBufferCache implements ByteBufferCache {
    /** Cache capacity. Not strict, just strongly suggested. */
    private final int capacity;

    /** Queue with cached buffers. */
    private final Queue<ByteBuffer> cache;

    /** Queue size, it's faster to store it separately. */
    private final AtomicInteger size = new AtomicInteger();

    /**
     * @param capacity The maximum number of buffers that the cache should hold. Not strict.
     */
    public DefaultByteBufferCache(int capacity) {
        this.capacity = capacity;

        cache = new ConcurrentLinkedQueue<>();
    }

    @Override
    public @Nullable ByteBuffer borrow() {
        ByteBuffer buffer = cache.poll();

        if (buffer != null) {
            size.decrementAndGet();
        }

        return buffer;
    }

    @Override
    public void offer(ByteBuffer buffer) {
        if (size.get() < capacity) {
            // Ignore races, there's no point in writing complicated code here.
            cache.add(buffer);
            size.incrementAndGet();
        }
    }
}
