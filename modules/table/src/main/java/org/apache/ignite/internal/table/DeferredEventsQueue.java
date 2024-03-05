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

package org.apache.ignite.internal.table;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.ToLongFunction;

/**
 * A queue for deferred events, which provide method to drain events up to given watermark. An implementation is a thread-safe wrapper over
 * {@link java.util.PriorityQueue}.
 *
 * @param <T> Event type.
 */
public class DeferredEventsQueue<T> {
    private final PriorityQueue<T> queue;
    private final ToLongFunction<T> mapper;

    /**
     * Creates a queue.
     *
     * @param mapper Event timestamp extractor.
     */
    public DeferredEventsQueue(ToLongFunction<T> mapper) {
        this.mapper = mapper;
        this.queue = new PriorityQueue<>(Comparator.comparingLong(this.mapper));
    }

    /**
     * Offers a new event to the queue.
     *
     * @param event New deferred event.
     */
    public boolean enqueue(T event) {
        synchronized (queue) {
            return queue.offer(event);
        }
    }

    /**
     * Drain queue up to given watermark and return dequeued events.
     *
     * @param watermark Timestamp to drain up to.
     * @return Dequeued events.
     */
    public List<T> drainUpTo(long watermark) {
        synchronized (queue) {
            if (!hasExpired0(watermark)) {
                return List.of();
            }

            List<T> events = new ArrayList<>();
            do {
                T event = queue.poll();

                events.add(event);
            } while (hasExpired0(watermark));

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
     * Returns {@code true} if found events below watermark, {@code false} otherwise.
     */
    public boolean hasExpiredEvents(long watermark) {
        synchronized (queue) {
            return hasExpired0(watermark);
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

    private boolean hasExpired0(long watermark) {
        assert Thread.holdsLock(queue);

        T next = queue.peek();

        return next != null && mapper.applyAsLong(next) <= watermark;
    }
}
