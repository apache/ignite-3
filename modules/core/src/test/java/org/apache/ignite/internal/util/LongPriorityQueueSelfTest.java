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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Objects;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LongPriorityQueue}.
 */
public class LongPriorityQueueSelfTest {
    @Test
    void testEmptyQueue() {
        LongPriorityQueue<Item> queue = new LongPriorityQueue<>(Item::timestamp);

        assertTrue(queue.isEmpty());
        assertThat(queue.size(), equalTo(0));

        assertFalse(queue.hasItems(Long.MAX_VALUE));
        assertThat(queue.drainUpTo(Long.MAX_VALUE), empty());
    }

    @Test
    void testQueueClear() {
        LongPriorityQueue<Item> queue = new LongPriorityQueue<>(Item::timestamp);

        assertTrue(queue.isEmpty());
        assertThat(queue.size(), equalTo(0));

        queue.enqueue(new Item(1, 100L));
        queue.enqueue(new Item(2, 100L));
        queue.enqueue(new Item(3, 200L));

        assertFalse(queue.isEmpty());
        assertThat(queue.size(), equalTo(3));

        queue.clear();

        assertTrue(queue.isEmpty());
        assertThat(queue.size(), equalTo(0));
    }

    @Test
    void testDrainEdgeCases() {
        LongPriorityQueue<Item> queue = new LongPriorityQueue<>(Item::timestamp);

        queue.enqueue(new Item(1, 300L));
        queue.enqueue(new Item(2, 100L));
        queue.enqueue(new Item(3, 200L));

        assertThat(queue.size(), equalTo(3));

        assertFalse(queue.hasItems(Long.MIN_VALUE));
        assertFalse(queue.hasItems(-1));
        assertFalse(queue.hasItems(0L));
        assertFalse(queue.hasItems(2L));

        assertTrue(queue.hasItems(100L));
        assertTrue(queue.hasItems(Long.MAX_VALUE));

        // Ensure too low value leas nothing to drain.
        assertThat(queue.drainUpTo(Long.MIN_VALUE), empty());
        assertThat(queue.drainUpTo(2L), empty());

        assertThat(queue.size(), equalTo(3));

        // Drain queue.
        assertThat(queue.drainUpTo(Long.MAX_VALUE), hasItems(
                new Item(1, 300L),
                new Item(2, 100L),
                new Item(3, 200L)
        ));

        assertThat(queue.size(), equalTo(0));
    }

    @Test
    void testDrain() {
        LongPriorityQueue<Item> queue = new LongPriorityQueue<>(Item::timestamp);

        queue.enqueue(new Item(1, 300L));
        queue.enqueue(new Item(2, 100L));
        queue.enqueue(new Item(3, 200L));
        queue.enqueue(new Item(4, 300L));
        queue.enqueue(new Item(5, 200L));

        assertThat(queue.size(), equalTo(5));

        // Drain some values
        assertThat(queue.drainUpTo(200L), hasItems(
                new Item(2, 100L),
                new Item(3, 200L),
                new Item(5, 200L)
        ));

        assertThat(queue.size(), equalTo(2));

        // Draining once again has no effect.
        assertThat(queue.drainUpTo(200L), empty());
        assertThat(queue.size(), equalTo(2));

        // Can add after drain.
        queue.enqueue(new Item(6, 100L));
        assertThat(queue.size(), equalTo(3));

        // Drain queue.
        assertThat(queue.drainUpTo(300L), hasItems(
                new Item(6, 100L),
                new Item(1, 300L),
                new Item(4, 300L)
        ));

        assertThat(queue.size(), equalTo(0));
    }

    static class Item {
        final long timestamp;
        final int value;

        Item(int value, long timestamp) {
            this.timestamp = timestamp;
            this.value = value;
        }

        public long timestamp() {
            return timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Item item = (Item) o;
            return timestamp == item.timestamp && value == item.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, value);
        }

        @Override
        public String toString() {
            return IgniteStringFormatter.format("Item [timestamp={}, value={}]", timestamp, value);
        }
    }
}
