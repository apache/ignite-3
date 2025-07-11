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
package org.apache.ignite.raft.jraft.option;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.ignite.raft.jraft.util.ByteBufferCollector;
import org.jetbrains.annotations.Nullable;

public class DefaultByteBufferCollectorPool implements ByteBufferCollectorPool {
    private final int capacity;

    private final ConcurrentLinkedDeque<ByteBufferCollector> stack = new ConcurrentLinkedDeque<>();

    private final AtomicInteger size = new AtomicInteger();

    public DefaultByteBufferCollectorPool(int capacity) {
        this.capacity = capacity;
    }

    @Override
    public @Nullable ByteBufferCollector borrow() {
        while (true) {
            int size = this.size.get();

            if (size == 0) {
                return null;
            } else if (this.size.compareAndSet(size, size - 1)) {
                return stack.pollLast();
            }
        }
    }

    @Override
    public void release(ByteBufferCollector c) {
        while (true) {
            int size = this.size.get();

            if (size == capacity) {
                return;
            } else if (this.size.compareAndSet(size, size + 1)) {
                stack.offerLast(c);

                return;
            }
        }
    }

    @Override
    public Stream<ByteBufferCollector> stream() {
        return stack.stream();
    }
}
