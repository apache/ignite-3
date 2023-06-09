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

package org.apache.ignite.internal.client.table;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

class StreamerBuffer<T> {
    private final int capacity;

    private final Function<List<T>, CompletableFuture<Void>> flusher;

    /** Primary buffer. Won't grow over capacity. */
    private List<T> buf;

    private CompletableFuture<Void> flushFut;

    private boolean closed;

    private long lastFlushTime;

    StreamerBuffer(int capacity, Function<List<T>, CompletableFuture<Void>> flusher) {
        this.capacity = capacity;
        this.flusher = flusher;
        buf = new ArrayList<>(capacity);
    }

    /**
     * Adds item to the buffer.
     *
     * @param item Item.
     */
    synchronized void add(T item) {
        if (closed) {
            throw new IllegalStateException("Streamer is closed, can't add items.");
        }

        buf.add(item);

        if (buf.size() >= capacity) {
            flush(buf);
            buf = new ArrayList<>(capacity);
        }
    }

    synchronized CompletableFuture<Void> flushAndClose() {
        if (closed) {
            throw new IllegalStateException("Streamer is already closed.");
        }

        closed = true;

        if (!buf.isEmpty()) {
            flush(buf);
        }

        return flushFut == null ? CompletableFuture.completedFuture(null) : flushFut;
    }

    synchronized void flush(long period) {
        if (closed || buf.isEmpty()) {
            return;
        }

        if (System.nanoTime() - lastFlushTime > period) {
            flush(buf);
            buf = new ArrayList<>(capacity);
        }
    }

    private void flush(List<T> b) {
        if (flushFut == null || flushFut.isDone()) {
            flushFut = flusher.apply(b);
        } else {
            // Chain flush futures to ensure the order of items.
            flushFut = flushFut.thenCompose(ignored -> flusher.apply(b));
        }

        lastFlushTime = System.nanoTime();
    }
}
