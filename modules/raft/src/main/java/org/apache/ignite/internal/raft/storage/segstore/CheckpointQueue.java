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

package org.apache.ignite.internal.raft.storage.segstore;

import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * A blocking queue implementation tailored to be used by the {@link RaftLogCheckpointer}.
 *
 * <p>This queue only supports the following access scenario:
 *
 * <ol>
 *     <li>One thread writes to the queue at a time, adding new entries via the {@link #add} call;</li>
 *     <li>One thread peeks at the head of the queue via the {@link #peekHead} method and, possibly after some time, removes the
 *     head using the {@link #removeHead} method. Since only one thread performs these actions, there's no need for additional
 *     synchronization between the {@code peek} and {@code removeHead} calls.</li>
 *     <li>Multiple threads can read from the queue using the {@link #tailIterator} method.</li>
 * </ol>
 */
class CheckpointQueue implements ManuallyCloseable {
    static class Entry {
        private final SegmentFile segmentFile;

        private final ReadModeIndexMemTable memTable;

        @Nullable
        private volatile Entry next;

        @Nullable
        private volatile Entry prev;

        Entry(SegmentFile segmentFile, ReadModeIndexMemTable memTable) {
            this.segmentFile = segmentFile;
            this.memTable = memTable;
        }

        SegmentFile segmentFile() {
            return segmentFile;
        }

        ReadModeIndexMemTable memTable() {
            return memTable;
        }
    }

    private final int maxSize;

    private final Lock lock = new ReentrantLock();

    private final Condition notEmpty = lock.newCondition();

    private final Condition notFull = lock.newCondition();

    /**
     * Flag indicating whether the queue is closed.
     *
     * <p>Multi-threaded access is guarded by {@link #lock}.
     */
    private boolean isClosed = false;

    /**
     * Pointer to the first entry (oldest in terms of being added) in the queue.
     *
     * <p>Multi-threaded access is guarded by {@link #lock}.
     */
    @Nullable
    private Entry head;

    /**
     * Pointer to the last entry (newest in terms of being added) in the queue.
     *
     * <p>This field is intentionally left volatile to be used in {@link #tailIterator} method.
     */
    @Nullable
    private volatile Entry tail;

    /**
     * Number of entries in the queue.
     *
     * <p>Multi-threaded access is guarded by {@link #lock}.
     */
    private int size;

    CheckpointQueue(int maxSize) {
        this.maxSize = maxSize;
    }

    /**
     * Adds a new entry to the queue, blocking if the queue is full.
     */
    void add(SegmentFile segmentFile, ReadModeIndexMemTable memTable) throws InterruptedException {
        var newEntry = new Entry(segmentFile, memTable);

        lock.lock();

        try {
            while (size == maxSize) {
                if (isClosed) {
                    throw new IgniteInternalException(NODE_STOPPING_ERR, "The queue is closed.");
                }

                notFull.await();
            }

            Entry curTail = tail;

            if (curTail == null) {
                // The queue was empty.
                head = newEntry;
            } else {
                // Append the new entry to the tail of the queue.
                curTail.next = newEntry;

                newEntry.prev = curTail;
            }

            tail = newEntry;

            size++;

            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the head element of the queue (not removing it), blocking if the queue is empty.
     */
    Entry peekHead() throws InterruptedException {
        lock.lock();

        try {
            while (size == 0) {
                if (isClosed) {
                    throw new IgniteInternalException(NODE_STOPPING_ERR, "The queue is closed.");
                }

                notEmpty.await();
            }

            Entry head = this.head;

            assert head != null;

            return head;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes the head element of the queue.
     *
     * <p>This method must only be called after a successful call to {@link #peekHead}.
     */
    void removeHead() {
        lock.lock();

        try {
            Entry head = this.head;

            // This method must only be called in conjunction with peek(), so we expect the queue to be non-empty.
            assert head != null;

            Entry nextHead = head.next;

            if (nextHead == null) {
                // The queue is empty now.
                tail = null;
            } else {
                nextHead.prev = null;
            }

            this.head = nextHead;

            size--;

            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns an iterator that iterates over the entries in the queue from the tail to the head (from newest entries to oldest).
     */
    Iterator<Entry> tailIterator() {
        return new Iterator<>() {
            @Nullable
            private Entry curEntry = tail;

            @Override
            public boolean hasNext() {
                return curEntry != null;
            }

            @Override
            public Entry next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                Entry result = curEntry;

                curEntry = curEntry.prev;

                return result;
            }
        };
    }

    @Override
    public void close() {
        lock.lock();

        try {
            isClosed = true;

            // Unblock all waiting threads.
            notFull.signalAll();
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
