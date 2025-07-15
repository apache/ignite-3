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

package org.apache.ignite.raft.jraft.util.concurrent;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.raft.jraft.util.ByteBufferCollector;
import org.apache.ignite.raft.jraft.util.ByteBufferCollectorPool;
import org.jetbrains.annotations.Nullable;

/**
 * Thread safe implementation of a pool based on a fixed-size FIFO linked list.
 *
 * <p>{@link ByteBufferCollector} are given out on a last-in, first-out basis.</p>
 */
public class ConcurrentLinkedLifoByteBufferCollectorPool implements ByteBufferCollectorPool {
    private final int capacity;

    private final AtomicReference<Node> stack = new AtomicReference<>();

    /**
     * Constructor.
     *
     * @param capacity Maximum capacity of the pool, expected to be greater than zero.
     */
    public ConcurrentLinkedLifoByteBufferCollectorPool(int capacity) {
        assert capacity > 0 : capacity;

        this.capacity = capacity;
    }

    /** Removes and returns the last added {@link ByteBufferCollector}, {@code null} if the pool is empty. */
    @Override
    public @Nullable ByteBufferCollector borrow() {
        while (true) {
            Node node = stack.get();

            if (node == null) {
                return null;
            } else if (stack.compareAndSet(node, node.next)) {
                return node.collector;
            }
        }
    }

    /** Adds a {@link ByteBufferCollector} to the pool if and only if it does not exceed the capacity. */
    @Override
    public void release(ByteBufferCollector c) {
        while (true) {
            Node node = stack.get();
            int newIndex = node == null ? 0 : node.index + 1;

            if (newIndex == capacity) {
                return;
            } else if (stack.compareAndSet(node, new Node(c, newIndex, node))) {
                return;
            }
        }
    }

    private static final class Node {
        private final ByteBufferCollector collector;

        private final int index;

        private final @Nullable Node next;

        private Node(ByteBufferCollector collector, int index, @Nullable Node next) {
            this.collector = collector;
            this.index = index;
            this.next = next;
        }
    }
}
