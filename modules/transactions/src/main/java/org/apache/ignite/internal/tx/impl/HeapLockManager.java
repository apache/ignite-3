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

import static java.util.concurrent.CompletableFuture.failedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.Waiter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link LockManager} implementation which stores lock queues in the heap.
 *
 * <p>Lock waiters are placed in the queue, ordered from oldest to yongest (highest Timestamp). When
 * a new waiter is placed in the queue, it's validated against current lock owner: if where is an owner with a higher timestamp lock request
 * is denied.
 *
 * <p>Read lock can be upgraded to write lock (only available for the oldest read-locked entry of
 * the queue).
 *
 * <p>If a younger read lock was upgraded, it will be invalidated if a oldest read-locked entry was upgraded. This corresponds
 * to the following scenario:
 *
 * <p>v1 = get(k, timestamp1) // timestamp1 < timestamp2
 *
 * <p>v2 = get(k, timestamp2)
 *
 * <p>put(k, v1, timestamp2) // Upgrades a younger read-lock to write-lock and waits for acquisition.
 *
 * <p>put(k, v1, timestamp1) // Upgrades an older read-lock. This will invalidate the younger write-lock.
 *
 * @see org.apache.ignite.internal.table.TxAbstractTest#testUpgradedLockInvalidation()
 */
public class HeapLockManager implements LockManager {
    private ConcurrentHashMap<LockKey, LockState> locks = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Lock> acquire(UUID txId, LockKey lockKey, LockMode lockMode) {
        // TODO: tmp
        switch (lockMode) {
            case EXCLUSIVE:

                while (true) {
                    LockState state = lockState(lockKey);

                    CompletableFuture<Void> future = state.tryAcquire(txId);

                    if (future == null) {
                        continue; // Obsolete state.
                    }

                    return future.thenApply(res -> new Lock(lockKey, lockMode, txId));
                }

            case SHARED:

                while (true) {
                    LockState state = lockState(lockKey);

                    CompletableFuture<Void> future = state.tryAcquireShared(txId);

                    if (future == null) {
                        continue; // Obsolete state.
                    }

                    return future.thenApply(res -> new Lock(lockKey, lockMode, txId));
                }

            default:
                return null;
        }
    }

    @Override
    public void release(Lock lock) throws LockException {
        // TODO: tmp
        LockState state = lockState(lock.lockKey());

        switch (lock.lockMode()) {
            case EXCLUSIVE:
                if (state.tryRelease(lock.txId())) { // Probably we should clean up empty keys asynchronously.
                    locks.remove(lock.lockKey(), state);
                }

                break;

            case SHARED:
                if (state.tryReleaseShared(lock.txId())) {
                    assert state.markedForRemove;

                    locks.remove(lock.lockKey(), state);
                }
                break;

            default:
                // No-op.
        }
    }

    @Override
    public Iterator<Lock> locks(UUID txId) {
        // TODO: tmp, use index instead.
        List<Lock> result = new ArrayList<>();

        for (Map.Entry<LockKey, LockState> entry : locks.entrySet()) {
            Waiter waiter = entry.getValue().waiter(txId);

            if (waiter != null) {
                result.add(
                        new Lock(
                                entry.getKey(),
                                waiter.isForRead() ? LockMode.SHARED : LockMode.EXCLUSIVE,
                                txId
                        )
                );
            }
        }

        return result.iterator();
    }

    /**
     * Returns the lock state for the key.
     *
     * @param key The key.
     */
    private @NotNull LockState lockState(LockKey key) {
        return locks.computeIfAbsent(key, k -> new LockState());
    }

    /** {@inheritDoc} */
    @Override
    public Collection<UUID> queue(LockKey key) {
        return lockState(key).queue();
    }

    /** {@inheritDoc} */
    @Override
    public Waiter waiter(LockKey key, UUID txId) {
        return lockState(key).waiter(txId);
    }

    /**
     * A lock state.
     */
    private static class LockState {
        /**
         * Waiters.
         */
        private TreeMap<UUID, WaiterImpl> waiters = new TreeMap<>();

        /**
         * Marked for removal flag.
         */
        private boolean markedForRemove = false;

        /**
         * Attempts to acquire a lock for the specified {@code key} in exclusive mode.
         *
         * @param txId Transaction id.
         * @return The future or null if state is marked for removal.
         */
        public @Nullable CompletableFuture<Void> tryAcquire(UUID txId) {
            WaiterImpl waiter = new WaiterImpl(txId, false);

            boolean locked;

            synchronized (waiters) {
                if (markedForRemove) {
                    return null;
                }

                WaiterImpl prev = waiters.putIfAbsent(txId, waiter);

                // Reenter
                if (prev != null && prev.locked) {
                    if (!prev.forRead) { // Allow reenter.
                        return CompletableFuture.completedFuture(null);
                    } else {
                        waiter.upgraded = true;

                        waiters.put(txId, waiter); // Upgrade.
                    }
                }

                // Check lock compatibility.
                Map.Entry<UUID, WaiterImpl> nextEntry = waiters.higherEntry(txId);

                // If we have a younger waiter in a locked state, when refuse to wait for lock.
                if (nextEntry != null && nextEntry.getValue().locked()) {
                    if (prev == null) {
                        waiters.remove(txId);
                    } else {
                        waiters.put(txId, prev); // Restore old lock.
                    }

                    return failedFuture(new LockException(nextEntry.getValue()));
                }

                // Lock if oldest.
                locked = waiters.firstKey().equals(txId);

                if (locked) {
                    waiter.lock();
                }
            }

            // Notify outside the monitor.
            if (locked) {
                waiter.notifyLocked();
            }

            return waiter.fut;
        }

        /**
         * Attempts to release a lock for the specified {@code key} in exclusive mode.
         *
         * @param txId Transaction id.
         * @return {@code True} if the queue is empty.
         */
        public boolean tryRelease(UUID txId) throws LockException {
            Collection<WaiterImpl> locked = new ArrayList<>();
            Collection<WaiterImpl> toFail = new ArrayList<>();

            Map.Entry<UUID, WaiterImpl> unlocked;

            synchronized (waiters) {
                Map.Entry<UUID, WaiterImpl> first = waiters.firstEntry();

                if (first == null || !first.getKey().equals(txId) || !first.getValue().locked() || first.getValue().isForRead()) {
                    throw new LockException("Not exclusively locked by " + txId);
                }

                unlocked = waiters.pollFirstEntry();

                markedForRemove = waiters.isEmpty();

                if (markedForRemove) {
                    return true;
                }

                // Lock next waiter(s).
                WaiterImpl waiter = waiters.firstEntry().getValue();

                if (!waiter.isForRead() && !waiter.upgraded) {
                    waiter.lock();

                    locked.add(waiter);
                } else {
                    // Grant lock to all adjacent readers.
                    for (Map.Entry<UUID, WaiterImpl> entry : waiters.entrySet()) {
                        WaiterImpl tmp = entry.getValue();

                        if (tmp.upgraded) {
                            // Fail upgraded waiters because of write.
                            assert !tmp.locked;

                            // Downgrade to acquired read lock.
                            tmp.upgraded = false;
                            tmp.forRead = true;
                            tmp.locked = true;

                            toFail.add(tmp);
                        } else if (!tmp.isForRead()) {
                            break;
                        } else {
                            tmp.lock();

                            locked.add(tmp);
                        }
                    }
                }
            }

            // Notify outside the monitor.
            for (WaiterImpl waiter : locked) {
                waiter.notifyLocked();
            }

            for (WaiterImpl waiter : toFail) {
                waiter.fut.completeExceptionally(new LockException(unlocked.getValue()));
            }

            return false;
        }

        /**
         * Attempts to acquire a lock for the specified {@code key} in shared mode.
         *
         * @param txId Transaction id.
         * @return The future or null if a state is marked for removal from map.
         */
        public @Nullable CompletableFuture<Void> tryAcquireShared(UUID txId) {
            WaiterImpl waiter = new WaiterImpl(txId, true);

            boolean locked;

            // Grant a lock to the oldest waiter.
            synchronized (waiters) {
                if (markedForRemove) {
                    return null;
                }

                WaiterImpl prev = waiters.putIfAbsent(txId, waiter);

                // Allow reenter. A write lock implies a read lock.
                if (prev != null && prev.locked) {
                    return CompletableFuture.completedFuture(null);
                }

                // Check lock compatibility.
                Map.Entry<UUID, WaiterImpl> nextEntry = waiters.higherEntry(txId);

                if (nextEntry != null) {
                    WaiterImpl nextWaiter = nextEntry.getValue();

                    if (nextWaiter.locked() && !nextWaiter.isForRead()) {
                        waiters.remove(txId);

                        return failedFuture(new LockException(nextWaiter));
                    }
                }

                Map.Entry<UUID, WaiterImpl> prevEntry = waiters.lowerEntry(txId);

                // Grant read lock if previous entry is read-locked (by induction).
                locked = prevEntry == null || (prevEntry.getValue().isForRead() && prevEntry
                        .getValue().locked());

                if (locked) {
                    waiter.lock();
                }
            }

            // Notify outside the monitor.
            if (locked) {
                waiter.notifyLocked();
            }

            return waiter.fut;
        }

        /**
         * Attempts to release a lock for the specified {@code key} in shared mode.
         *
         * @param txId Transaction id.
         * @return {@code True} if the queue is empty.
         */
        public boolean tryReleaseShared(UUID txId) throws LockException {
            WaiterImpl locked = null;

            synchronized (waiters) {
                WaiterImpl waiter = waiters.get(txId);

                if (waiter == null || !waiter.locked() || !waiter.isForRead()) {
                    throw new LockException("Not shared locked by " + txId);
                }

                Map.Entry<UUID, WaiterImpl> nextEntry = waiters.higherEntry(txId);

                waiters.remove(txId);

                if (nextEntry == null) {
                    return (markedForRemove = waiters.isEmpty());
                }

                // Lock next exclusive waiter.
                WaiterImpl nextWaiter = nextEntry.getValue();

                if (!nextWaiter.isForRead() && nextWaiter.txId()
                        .equals(waiters.firstEntry().getKey())) {
                    nextWaiter.lock();

                    locked = nextWaiter;
                }
            }

            if (locked != null) {
                locked.notifyLocked();
            }

            return false;
        }

        /**
         * Returns a collection of timestamps that is associated with the specified {@code key}.
         *
         * @return The waiters queue.
         */
        public Collection<UUID> queue() {
            synchronized (waiters) {
                return new ArrayList<>(waiters.keySet());
            }
        }

        /**
         * Returns a waiter for the specified {@code key}.
         *
         * @param txId Transaction id.
         * @return The waiter.
         */
        public Waiter waiter(UUID txId) {
            synchronized (waiters) {
                return waiters.get(txId);
            }
        }
    }

    /**
     * A waiter implementation.
     */
    private static class WaiterImpl implements Comparable<WaiterImpl>, Waiter {
        /** Locked future. */
        @IgniteToStringExclude
        private final CompletableFuture<Void> fut;

        /** Waiter transaction id. */
        private final UUID txId;

        /** Upgraded lock. */
        private boolean upgraded;

        /** {@code True} if a read request. */
        private boolean forRead;

        /** The state. */
        private boolean locked = false;

        /**
         * The constructor.
         *
         * @param txId Transaction id.
         * @param forRead {@code True} to request a read lock.
         */
        WaiterImpl(UUID txId, boolean forRead) {
            this.fut = new CompletableFuture<>();
            this.txId = txId;
            this.forRead = forRead;
        }

        /** {@inheritDoc} */
        @Override
        public int compareTo(@NotNull WaiterImpl o) {
            return txId.compareTo(o.txId);
        }

        /** Notifies a future listeners. */
        private void notifyLocked() {
            assert locked;

            fut.complete(null);
        }

        /** {@inheritDoc} */
        @Override
        public boolean locked() {
            return this.locked;
        }

        public boolean lockedForRead() {
            return this.locked && forRead;
        }

        public boolean lockedForWrite() {
            return this.locked && !forRead;
        }

        /** Grant a lock. */
        private void lock() {
            locked = true;
        }

        /** {@inheritDoc} */
        @Override
        public UUID txId() {
            return txId;
        }

        /** Returns {@code true} if is locked for read. */
        @Override
        public boolean isForRead() {
            return forRead;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof WaiterImpl)) {
                return false;
            }

            return compareTo((WaiterImpl) o) == 0;
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return txId.hashCode();
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return S.toString(WaiterImpl.class, this, "isDone", fut.isDone());
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean isEmpty() {
        return locks.isEmpty();
    }
}
