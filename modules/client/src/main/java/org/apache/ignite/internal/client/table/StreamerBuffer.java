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

    private final List<T> items;

    StreamerBuffer(int capacity) {
        this.capacity = capacity;
        items = new ArrayList<>(capacity);
    }

    boolean tryAdd(T item) {
        if (count.incrementAndGet() > capacity) {
            return false;
        }

        items.add(item);
        return true;
    }

    void clear() {
        items.clear();
        count.set(0);
    }

    Collection<T> items() {
        return items;
    }
}
