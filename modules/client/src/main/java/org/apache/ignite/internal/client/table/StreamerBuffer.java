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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class StreamerBuffer<T> {
    private final int capacity;
    private final AtomicInteger count = new AtomicInteger();

    /** Primary buffer. Won't grow over capacity. */
    private List<T> buf;

    /** Secondary buffer. Used while primary buffer is being flushed. */
    private List<T> buf2;

    StreamerBuffer(int capacity) {
        this.capacity = capacity;
        buf = new ArrayList<>(capacity);
        buf2 = new ArrayList<>(capacity);
    }

    /**
     * Adds item to the buffer.
     *
     * @param item Item.
     * @return True if current item is the last one in the buffer; the buffer is full and should be flushed.
     */
    boolean add(T item) {
        // TODO: RW lock?
        int cnt = count.incrementAndGet();
        if (cnt <= capacity) {
            buf.add(item);
            return cnt == capacity;
        } else {
            buf2.add(item);
            return false;
        }
    }

    void onSent() {
        // TODO: RW lock?

        // Clear flushed buffer.
        buf.clear();

        // Swap buffers.
        List<T> tmp = buf;
        buf = buf2;
        buf2 = tmp;

        count.set(buf.size());
    }

    Collection<T> items() {
        return buf;
    }
}
