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
import java.util.function.Consumer;

class StreamerBuffer<T> {
    private final int capacity;

    private final Consumer<List<T>> flusher;

    /** Primary buffer. Won't grow over capacity. */
    private List<T> buf;

    private boolean closed;

    StreamerBuffer(int capacity, Consumer<List<T>> flusher) {
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
            flusher.accept(buf);
            buf = new ArrayList<>(capacity);
        }
    }

    synchronized void flushAndClose() {
        if (closed) {
            throw new IllegalStateException("Streamer is already closed.");
        }

        closed = true;

        if (!buf.isEmpty()) {
            flusher.accept(buf);
        }
    }

    synchronized void flush() {
        if (closed || buf.isEmpty()) {
            return;
        }

        flusher.accept(buf);
        buf = new ArrayList<>(capacity);
    }
}
