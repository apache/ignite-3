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

package org.apache.ignite.internal.tx.impl;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.lang.ErrorGroups.Transactions.ACQUIRE_LOCK_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.DOWNGRADE_LOCK_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.RELEASE_LOCK_ERR;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link LockManager} implementation which stores lock queues in the heap.
 *
 * <p>Lock waiters are placed in the queue, ordered from oldest to youngest (highest txId). When
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
 */
public class HeapLockManager implements LockManager {
    private ConcurrentHashMap<LockKey, LockState> locks = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Lock> acquire(UUID txId, LockKey lockKey, LockMode lockMode) {
        //TODO: IGNITE-17733 Resume honest index lock
        if (lockKey.key() instanceof ByteBuffer) {
            lockMode = LockMode.NAL;
        }

        while (true) {
            LockState state = lockState(lockKey);

            IgniteBiTuple<CompletableFuture<Void>, LockMode> futureTuple = state.tryAcquire(txId, lockMode);

            if (futureTuple.get1() == null) {
                continue; // Obsolete state.
            }

            LockMode newLockMode = futureTuple.get2();

            return futureTuple.get1().thenApply(res -> new Lock(lockKey, newLockMode, txId));
        }
    }

    @Override
    public void release(Lock lock) {
        LockState state = lockState(lock.lockKey());

        if (state.tryRelease(lock.txId())) {
            locks.remove(lock.lockKey(), state);
        }
    }

    @Override
    public void downgrade(Lock lock, LockMode lockMode) throws LockException {
        LockState state = lockState(lock.lockKey());

        state.tryDowngrade(lock, lockMode);
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
                                waiter.lockMode(),
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
        /** Waiters. */
        private final TreeMap<UUID, WaiterImpl> waiters = new TreeMap<>();

        /** Marked for removal flag. */
        private boolean markedForRemove = false;

        /**
         * Attempts to acquire a lock for the specified {@code key} in specified lock mode.
         *
         * @param txId Transaction id.
         * @param lockMode Lock mode.
         * @return The future or null if state is marked for removal and acquired lock mode.
         */
        public @Nullable IgniteBiTuple<CompletableFuture<Void>, LockMode> tryAcquire(UUID txId, LockMode lockMode) {
            WaiterImpl waiter = new WaiterImpl(txId, lockMode);

            boolean locked;

            synchronized (waiters) {
                if (markedForRemove) {
                    return new IgniteBiTuple(null, lockMode);
                }

                WaiterImpl prev = waiters.putIfAbsent(txId, waiter);

                // Reenter
                if (prev != null && prev.locked) {
                    if (prev.lockMode.allowReenter(lockMode)) {
                        return new IgniteBiTuple(CompletableFuture.completedFuture(null), lockMode);
                    } else {
                        waiter.upgraded = true;

                        lockMode = LockMode.supremum(prev.lockMode, lockMode);

                        waiter.prevLockMode = prev.lockMode;

                        waiter.lockMode = lockMode;

                        waiters.put(txId, waiter); // Upgrade.
                    }
                }

                // Check lock compatibility.
                Map.Entry<UUID, WaiterImpl> nextEntry = waiters.higherEntry(txId);

                // If we have a younger waiter in a locked state, when refuse to wait for lock.
                if (nextEntry != null
                        && nextEntry.getValue().locked()
                        && !lockMode.isCompatible(nextEntry.getValue().lockMode)) {
                    if (prev == null) {
                        waiters.remove(txId);
                    } else {
                        waiters.put(txId, prev); // Restore old lock.
                    }

                    return new IgniteBiTuple(
                            failedFuture(new LockException(
                                    ACQUIRE_LOCK_ERR,
                                    "Failed to acquire a lock due to a conflict [txId=" + txId + ", waiter=" + nextEntry.getValue() + ']')),
                            lockMode);
                }

                // Lock if oldest.
                locked = waiters.firstKey().equals(txId);

                if (!locked) {
                    Map.Entry<UUID, WaiterImpl> prevEntry = waiters.lowerEntry(txId);

                    // Grant lock if previous entry lock is compatible (by induction).
                    locked = prevEntry == null || (prevEntry.getValue().lockMode.isCompatible(lockMode) && prevEntry
                            .getValue().locked());
                }

                if (locked) {
                    if (waiter.upgraded) {
                        // Upgrade lock.
                        waiter.upgraded = false;
                        waiter.prevLockMode = null;
                        waiter.locked = true;
                    } else {
                        waiter.lock();
                    }
                }
            }

            // Notify outside the monitor.
            if (locked) {
                waiter.notifyLocked();
            }

            return new IgniteBiTuple(waiter.fut, lockMode);
        }

        /**
         * Attempts to release a lock for the specified {@code key} in exclusive mode.
         *
         * @param txId Transaction id.
         * @return {@code True} if the queue is empty.
         */
        public boolean tryRelease(UUID txId) {
            Collection<WaiterImpl> locked = new ArrayList<>();
            Collection<WaiterImpl> toFail = new ArrayList<>();

            WaiterImpl removed;

            synchronized (waiters) {
                removed = waiters.remove(txId);

                markedForRemove = waiters.isEmpty();

                if (markedForRemove) {
                    return true;
                }

                Set<LockMode> lockModes = new HashSet<>();

                // Grant lock to all adjacent readers.
                for (Map.Entry<UUID, WaiterImpl> entry : waiters.entrySet()) {
                    WaiterImpl tmp = entry.getValue();

                    if (tmp.upgraded && !removed.lockMode.isCompatible(tmp.prevLockMode)) {
                        // Fail upgraded waiters.
                        assert !tmp.locked;

                        // Downgrade to acquired lock.
                        tmp.upgraded = false;
                        tmp.lockMode = tmp.prevLockMode;
                        tmp.prevLockMode = null;
                        tmp.locked = true;

                        toFail.add(tmp);
                    } else if (lockModes.stream().allMatch(tmp.lockMode::isCompatible)) {
                        if (tmp.upgraded) {
                            // Fail upgraded waiters.
                            assert !tmp.locked;

                            // Upgrade lock.
                            tmp.upgraded = false;
                            tmp.prevLockMode = null;
                            tmp.locked = true;
                        } else {
                            tmp.lock();
                        }

                        lockModes.add(tmp.lockMode);

                        locked.add(tmp);
                    }
                }
            }

            // Notify outside the monitor.
            for (WaiterImpl waiter : locked) {
                waiter.notifyLocked();
            }

            for (WaiterImpl waiter : toFail) {
                waiter.fut.completeExceptionally(
                        new LockException(
                                RELEASE_LOCK_ERR,
                                "Failed to acquire a lock due to a conflict [txId=" + txId + ", waiter=" + removed + ']'));
            }

            return false;
        }

        /**
         * Attempts to downgrade a lock for the specified {@code key} to a specified lock mode.
         *
         * @param lock Lock.
         * @param lockMode Lock mode.
         * @throws LockException If the downgrade operation is invalid.
         */
        void tryDowngrade(Lock lock, LockMode lockMode) throws LockException {
            WaiterImpl waiter = new WaiterImpl(lock.txId(), lockMode);

            synchronized (waiters) {
                WaiterImpl prev = waiters.remove(lock.txId());

                if (prev != null) {
                    if (prev.lockMode == LockMode.IX && lockMode == LockMode.S
                            || prev.lockMode == LockMode.S && lockMode == LockMode.IX
                            || prev.lockMode.compareTo(lockMode) < 0) {
                        waiters.put(lock.txId(), prev);

                        throw new LockException(DOWNGRADE_LOCK_ERR, "Cannot change lock mode from " + prev.lockMode + " to " + lockMode);
                    }

                    for (Map.Entry<UUID, WaiterImpl> entry : waiters.entrySet()) {
                        WaiterImpl tmp = entry.getValue();

                        if (!lockMode.isCompatible(tmp.lockMode)) {
                            waiters.put(lock.txId(), waiter);

                            throw new LockException(
                                    DOWNGRADE_LOCK_ERR,
                                    "Cannot change lock mode from " + prev.lockMode + " to " + lockMode);
                        }
                    }

                    waiters.put(lock.txId(), waiter);
                }
            }
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

        /** The previous lock mode. */
        private LockMode prevLockMode;

        /** The lock mode. */
        private LockMode lockMode;

        /** The state. */
        private boolean locked = false;

        /**
         * The constructor.
         *
         * @param txId Transaction id.
         * @param lockMode Lock mode.
         */
        WaiterImpl(UUID txId, LockMode lockMode) {
            this.fut = new CompletableFuture<>();
            this.txId = txId;
            this.lockMode = lockMode;
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

        /** {@inheritDoc} */
        @Override
        public LockMode lockMode() {
            return lockMode;
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
