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

package org.apache.ignite.internal.raft.storage;

import static org.apache.ignite.internal.util.IgniteUtils.isPow2;

import org.apache.ignite.raft.jraft.entity.LogId;

/**
 * Cyclic buffer to cache several last term values for log storage.
 */
public class TermCache {
    private final int mask;
    private final long[] indexes;
    private final long[] terms;

    // Head position. -1 means the cache is empty.
    private int head = -1;

    // Tail position. Might be equal to head if the cache only has a single term.
    private int tail;

    /**
     * Constructor.
     *
     * @param capacity Cache capacity. Must be a power of 2. Should be a small value, term update is a rare operation.
     */
    public TermCache(int capacity) {
        assert isPow2(capacity) : "Capacity must be a power of 2";

        this.mask = capacity - 1;
        this.indexes = new long[capacity];
        this.terms = new long[capacity];
    }

    /**
     * Should be called when appending a new log entry.
     */
    public void append(LogId id) {
        if (isEmpty()) {
            head = 0;
            indexes[tail] = id.getIndex();
            terms[tail] = id.getTerm();

            return;
        }

        // Term has not changed, nothing to update.
        if (terms[tail] == id.getTerm()) {
            return;
        }

        tail = next(tail);
        indexes[tail] = id.getIndex();
        terms[tail] = id.getTerm();

        // Handle buffer overflow by moving head to the next position.
        if (tail == head) {
            head = next(head);
        }
    }

    private int prev(int i) {
        return (i - 1) & mask;
    }

    private int next(int i) {
        return (i + 1) & mask;
    }

    private boolean isEmpty() {
        return head == -1;
    }

    private int findIndex(long idx) {
        // Could be replaced with a binary search, but why bother for such a small cache.
        for (int i = tail; i != head; i = prev(i)) {
            if (idx >= indexes[i]) {
                return i;
            }
        }

        return head;
    }

    /**
     * Lookup term for the given index. Returns {@code -1} if the index is not found in the cache.
     */
    public long lookup(long idx) {
        if (isEmpty() || idx < indexes[head]) {
            return -1;
        }

        return terms[findIndex(idx)];
    }

    /**
     * Resets the cache to the initial state.
     */
    public void reset() {
        head = -1;
        tail = 0;
    }

    /**
     * Truncates the cache to the given index, deleting all information for indexes greater than or equal to the given one.
     */
    public void truncateTail(long idx) {
        if (isEmpty() || idx < indexes[head]) {
            reset();

            return;
        }

        tail = findIndex(idx);

        if (indexes[tail] == idx) {
            if (head == tail) {
                reset();
            } else {
                tail = prev(tail);
            }
        }
    }
}
