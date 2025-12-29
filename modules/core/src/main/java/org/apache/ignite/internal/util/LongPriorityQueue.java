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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.ToLongFunction;

/**
 * A thread-safe wrapper over {@link PriorityQueue}, which uses {@code long} value for ordering.
 * The implementation provides a method to poll top item up to the given priority.
 *
 * @param <T> Item type.
 */
public class LongPriorityQueue<T> {
    /** A queue. Guarded by itself. */
    private final PriorityQueue<T> queue;
    private final ToLongFunction<T> priorityExtractor;

    /**
     * Creates a queue.
     *
     * @param priorityExtractor Priority extractor.
     */
    public LongPriorityQueue(ToLongFunction<T> priorityExtractor) {
        this.priorityExtractor = priorityExtractor;
        this.queue = new PriorityQueue<>(Comparator.comparingLong(priorityExtractor));
    }

    /**
     * Offers a new item to the queue.
     *
     * @param newItem New item.
     */
    public boolean enqueue(T newItem) {
        synchronized (queue) {
            return queue.offer(newItem);
        }
    }

    /**
     * Dequeue items, which priority is less or equal to the given one.
     *
     * @param priority A priority to drain up to.
     * @return Dequeued events.
     */
    public List<T> drainUpTo(long priority) {
        synchronized (queue) {
            if (!hasItems0(priority)) {
                return List.of();
            }

            List<T> events = new ArrayList<>();
            do {
                T event = queue.poll();

                events.add(event);
            } while (hasItems0(priority));

            return events;
        }
    }

    /**
     * Returns queue size.
     */
    public int size() {
        synchronized (queue) {
            return queue.size();
        }
    }

    /**
     * Returns {@code true} if queue is empty, {@code false} otherwise.
     */
    public boolean isEmpty() {
        synchronized (queue) {
            return queue.isEmpty();
        }
    }

    /**
     * Returns {@code true} if found events, which priority is less or equal to the given one, {@code false} otherwise.
     */
    public boolean hasItems(long watermark) {
        synchronized (queue) {
            return hasItems0(watermark);
        }
    }

    /**
     * Removes all events from the queue.
     */
    public void clear() {
        synchronized (queue) {
            queue.clear();
        }
    }

    private boolean hasItems0(long watermark) {
        assert Thread.holdsLock(queue);

        T next = queue.peek();

        return next != null && priorityExtractor.applyAsLong(next) <= watermark;
    }
}
