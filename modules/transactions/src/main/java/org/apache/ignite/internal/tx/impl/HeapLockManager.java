/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.tx.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.Waiter;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link LockManager} implementation which stores lock queues in the heap.
 */
public class HeapLockManager implements LockManager {
    private ConcurrentHashMap<Object, LockState> locks = new ConcurrentHashMap<>();

    @Override public CompletableFuture<Void> tryAcquire(Object key, Timestamp timestamp) throws LockException {
        return lockState(key).tryAcquire(timestamp);
    }

    @Override public void tryRelease(Object key, Timestamp timestamp) {
        lockState(key).tryRelease(timestamp);
    }

    @Override public CompletableFuture<Void> tryAcquireShared(Object key, Timestamp timestamp) throws LockException {
        return lockState(key).tryAcquireShared(timestamp);
    }

    @Override public void tryReleaseShared(Object key, Timestamp timestamp) {
        lockState(key).tryReleaseShared(timestamp);
    }

    private @NotNull LockState lockState(Object key) {
        return locks.computeIfAbsent(key, k -> new LockState());
    }

    @Override public long readHoldCount(Object key) {
        return 0;
    }

    @Override public boolean isReadLocked(Object key) {
        return false;
    }

    @Override public boolean isWriteLocked(Object key) {
        return false;
    }

    @Override public Collection<Timestamp> queue(Object key) {
        return lockState(key).queue();
    }

    @Override public Waiter waiter(Object key, Timestamp timestamp) {
        return lockState(key).waiter(timestamp);
    }

    /** A lock state. */
    private static class LockState {
        private TreeMap<Timestamp, WaiterImpl> waiters = new TreeMap<>();

        private int writes = 0;

        public CompletableFuture<Void> tryAcquire(Timestamp timestamp) {
            WaiterImpl w = new WaiterImpl(new CompletableFuture<>(), timestamp, false);

            synchronized (waiters) {
                waiters.put(timestamp, w);

                // Check lock compatibility.
                NavigableMap<Timestamp, WaiterImpl> tailMap = waiters.tailMap(timestamp, false);

                if (!tailMap.isEmpty() && tailMap.firstEntry().getValue().state() == Waiter.State.LOCKED) {
                    waiters.remove(timestamp);

                    throw new LockException("Lock can't be taken because of the conflict");
                }

                writes++;

                // Lock oldest.
                if (waiters.firstKey() == timestamp)
                    w.lock();
            }

            return w.fut;
        }

        /**
         * @param timestamp The timestamp.
         */
        public void tryRelease(Timestamp timestamp) {
            synchronized (waiters) {
                Map.Entry<Timestamp, WaiterImpl> first = waiters.firstEntry();

                if (first == null ||
                    !first.getKey().equals(timestamp) ||
                    first.getValue().state() != Waiter.State.LOCKED ||
                    first.getValue().isForRead())
                    throw new LockException("Not exclusively locked by " + timestamp);

                waiters.pollFirstEntry(); // TODO try avoid double get

                writes--;

                if (waiters.isEmpty())
                    return;

                // Lock next waiters.
                first = waiters.firstEntry();

                if (!first.getValue().isForRead())
                    first.getValue().lock();
                else {
                    for (Map.Entry<Timestamp, WaiterImpl> entry : waiters.entrySet()) {
                        if (!entry.getValue().isForRead())
                            break;
                        else
                            entry.getValue().lock();
                    }
                }
            }
        }

        public CompletableFuture<Void> tryAcquireShared(Timestamp timestamp) {
            WaiterImpl w = new WaiterImpl(new CompletableFuture<>(), timestamp, true);

            // Grant a lock to the oldest waiter.
            synchronized (waiters) {
                waiters.put(timestamp, w);

                // Check lock compatibility.
                NavigableMap<Timestamp, WaiterImpl> tailMap = waiters.tailMap(timestamp, false);

                if (!tailMap.isEmpty() && tailMap.firstEntry().getValue().state() == Waiter.State.LOCKED && !tailMap.firstEntry().getValue().isForRead()) {
                    waiters.remove(timestamp);

                    throw new LockException("Lock can't be taken because of the conflict"); // TODO add conflicting waiter to the exception.
                }

                NavigableMap<Timestamp, WaiterImpl> headMap = waiters.headMap(timestamp, false);

                if (headMap.isEmpty() || headMap.lastEntry().getValue().isForRead())
                    w.lock();
            }

            return w.fut;
        }

        public void tryReleaseShared(Timestamp timestamp) {
            synchronized (waiters) {
                WaiterImpl waiter = waiters.get(timestamp);

                if (waiter == null ||
                    waiter.state() != Waiter.State.LOCKED ||
                    !waiter.isForRead())
                    throw new LockException("Not shared locked by " + timestamp);

                NavigableMap<Timestamp, WaiterImpl> tailMap = waiters.tailMap(timestamp, true);

                tailMap.remove(timestamp);

                // Queue is empty.
                if (tailMap.isEmpty())
                    return;

                // Lock next exclusive waiter.
                Map.Entry<Timestamp, WaiterImpl> first = tailMap.firstEntry();

                if (!first.getValue().isForRead())
                    first.getValue().lock();
            }
        }

        /**
         * @return Waiters queue.
         */
        public Collection<Timestamp> queue() {
            synchronized (waiters) {
                return new ArrayList<>(waiters.keySet());
            }
        }

        /**
         * @param timestamp The timestamp.
         * @return The waiter.
         */
        public Waiter waiter(Timestamp timestamp) {
            synchronized (waiters) {
                return waiters.get(timestamp);
            }
        }
    }

    private static class WaiterImpl implements Comparable<WaiterImpl>, Waiter {
        private final CompletableFuture<Void> fut;
        private final Timestamp timestamp;
        private boolean forRead; // TODO use flags
        private volatile State state = State.PENDING;

        WaiterImpl(CompletableFuture<Void> fut, Timestamp timestamp, boolean forRead) {
            this.fut = fut;
            this.timestamp = timestamp;
            this.forRead = forRead;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull WaiterImpl o) {
            return timestamp.compareTo(o.timestamp);
        }

        /** {@inheritDoc} */
        @Override public State state() {
            return state;
        }

        public void state(State state) {
            this.state = state;
        }

        /** {@inheritDoc} */
        @Override public Timestamp timestamp() {
            return timestamp;
        }

        /**
         * @return {@code True} if is locked for read.
         */
        public boolean isForRead() {
            return forRead;
        }

        private void lock() {
            state(Waiter.State.LOCKED);
            fut.complete(null);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (!(o instanceof WaiterImpl)) return false;
            return compareTo((WaiterImpl) o) == 0;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return timestamp.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(WaiterImpl.class, this);
        }
    }
}
