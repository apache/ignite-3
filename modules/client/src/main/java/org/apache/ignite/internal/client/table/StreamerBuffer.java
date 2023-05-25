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
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

class StreamerBuffer<T> {
    private final int capacity;

    /** Pending buffers. We guarantee the order of items within one connection (node, partition). These buffers should be sent in order. */
    private final Queue<List<T>> pendingBufs = new LinkedList<>();

    private final Function<List<T>, CompletableFuture<Void>> flusher;

    /** Primary buffer. Won't grow over capacity. */
    private List<T> buf;

    private boolean flushing;

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
        if (flushing) {
            throw new IllegalStateException("Streamer is closing, can't add items.");
        }

        buf.add(item);

        if (buf.size() >= capacity) {
            // TODO: Flush here.
            // TODO: Chain futures to ensure the order of items. We can avoid the queue this way.
            // If a node goes down, the batch goes to default node thanks to built-it retry mechanism.
            pendingBufs.add(buf);
            buf = new ArrayList<>(capacity);
        }
    }

    synchronized void flush() {
        flushing = true;
    }
}
