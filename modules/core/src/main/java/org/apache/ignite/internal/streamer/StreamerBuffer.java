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

package org.apache.ignite.internal.streamer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

class StreamerBuffer<T> {
    private final int capacity;

    private final Function<List<T>, CompletableFuture<?>> flusher;

    /** Primary buffer. Won't grow over capacity. */
    private List<T> buf;

    private boolean closed;

    /** Last flush completion timestamp, in nanoseconds. */
    private long lastFlushNanos = System.nanoTime();

    private CompletableFuture<?> flushFut;

    StreamerBuffer(int capacity, Function<List<T>, CompletableFuture<?>> flusher) {
        this.capacity = capacity;
        this.flusher = flusher;
        buf = new ArrayList<>(capacity);
    }

    /**
     * Adds item to the buffer.
     *
     * @param item Item.
     */
    void add(T item) {
        List<T> bufToFlush = null;

        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("Streamer is closed, can't add items.");
            }

            buf.add(item);

            if (buf.size() >= capacity) {
                bufToFlush = buf;
                buf = new ArrayList<>(capacity);
                lastFlushNanos = System.nanoTime();
            }
        }

        flushBuf(bufToFlush); // Flush outside of lock to avoid deadlocks.
    }

    void flushAndClose() {
        List<T> bufToFlush;

        synchronized (this) {
            if (closed) {
                return;
            }

            closed = true;

            bufToFlush = buf;
        }

        flushBuf(bufToFlush); // Flush outside of lock to avoid deadlocks.
    }

    void autoFlush(long intervalNanos) {
        List<T> bufToFlush;

        synchronized (this) {
            if (closed || buf.isEmpty()) {
                return;
            }

            if (flushFut != null && !flushFut.isDone()) {
                // Flush in progress.
                return;
            }

            if (System.nanoTime() - lastFlushNanos < intervalNanos) {
                // Not enough time has passed since the last flush.
                return;
            }

            bufToFlush = buf;
            buf = new ArrayList<>(capacity);
            lastFlushNanos = System.nanoTime();
        }

        flushBuf(bufToFlush); // Flush outside of lock to avoid deadlocks.
    }

    synchronized void forEach(Consumer<T> consumer) {
        if (closed) {
            return;
        }

        buf.forEach(consumer);
    }

    private void flushBuf(@Nullable List<T> bufToFlush) {
        if (bufToFlush == null || bufToFlush.isEmpty()) {
            return;
        }

        CompletableFuture<?> fut = flusher.apply(bufToFlush);

        synchronized (this) {
            flushFut = fut;
        }
    }
}
