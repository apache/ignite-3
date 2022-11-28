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

import static org.apache.ignite.lang.ErrorGroups.Transactions.ACQUIRE_LOCK_ERR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
    public void release(UUID txId, LockKey lockKey, LockMode lockMode) {
        LockState state = lockState(lockKey);

        if (state.tryRelease(txId, lockMode)) {
            locks.remove(lockKey, state);
        }
    }

    @Override
    public Iterator<Lock> locks(UUID txId) {
        // TODO: IGNITE-17811 Use index or similar instead of full locks set iteration.
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

            synchronized (waiters) {
                if (markedForRemove) {
                    return new IgniteBiTuple(null, lockMode);
                }

                WaiterImpl prev = waiters.putIfAbsent(txId, waiter);

                // Reenter
                if (prev != null && prev.locked()) {
                    if (prev.lockMode.allowReenter(lockMode)) {
                        prev.addLock(lockMode, 1);

                        return new IgniteBiTuple(CompletableFuture.completedFuture(null), lockMode);
                    } else {
                        waiter.addLocks(prev.locks);

                        waiter.upgraded = true;

                        lockMode = LockMode.supremum(prev.lockMode, lockMode);

                        waiter.prevLockMode = prev.lockMode;

                        waiter.lockMode = lockMode;

                        waiters.put(txId, waiter); // Upgrade.
                    }
                }

                if (!isWaiterReadyToNotify(waiter, false)) {
                    return new IgniteBiTuple(waiter.fut, lockMode);
                }

                if (!waiter.locked()) {
                    if (prev == null) {
                        waiters.remove(waiter.txId());
                    } else {
                        waiters.put(waiter.txId(), prev); // Restore old lock.
                    }
                }
            }

            // Notify outside the monitor.
            waiter.notifyLocked();

            return new IgniteBiTuple(waiter.fut, lockMode);
        }

        /**
         * Checks current waiter.
         *
         * @param waiter Checked waiter.
         * @return True if current waiter ready to notify, false otherwise.
         */
        private boolean isWaiterReadyToNotify(WaiterImpl waiter, boolean skipFail) {
            for (Map.Entry<UUID, WaiterImpl> entry : waiters.tailMap(waiter.txId(), false).entrySet()) {
                WaiterImpl tmp = entry.getValue();
                LockMode mode = lockedMode(tmp);

                if (mode != null && !mode.isCompatible(waiter.lockMode())) {
                    return false;
                }
            }

            for (Map.Entry<UUID, WaiterImpl> entry : waiters.headMap(waiter.txId()).entrySet()) {
                WaiterImpl tmp = entry.getValue();
                LockMode mode = lockedMode(tmp);

                if (mode != null && !mode.isCompatible(waiter.lockMode())) {
                    if (skipFail) {
                        return false;
                    } else {
                        waiter.fail(new LockException(ACQUIRE_LOCK_ERR, "Failed to acquire a lock due to a conflict [txId=" + waiter.txId()
                                + ", waiter=" + tmp + ']'));

                        return true;
                    }
                }
            }

            if (waiter.upgraded) {
                // Upgrade lock.
                waiter.upgraded = false;
                waiter.prevLockMode = null;
                waiter.locked = true;
            } else {
                waiter.lock();
            }

            return true;
        }

        /**
         * Attempts to release a lock for the specified {@code key} in exclusive mode.
         *
         * @param txId Transaction id.
         * @return {@code True} if the queue is empty.
         */
        public boolean tryRelease(UUID txId) {
            Collection<WaiterImpl> toNotify;

            synchronized (waiters) {
                toNotify = release(txId);
            }

            // Notify outside the monitor.
            for (WaiterImpl waiter : toNotify) {
                waiter.notifyLocked();
            }

            return markedForRemove;
        }

        /**
         * Releases a specific lock of the key.
         *
         * @param txId Transaction id.
         * @param lockMode Lock mode.
         * @return If the value is true, no one waits of any lock of the key, false otherwise.
         */
        public boolean tryRelease(UUID txId, LockMode lockMode) {
            List<WaiterImpl> toNotify = Collections.emptyList();
            synchronized (waiters) {
                WaiterImpl waiter = waiters.get(txId);

                if (waiter != null) {
                    waiter.removeLock(lockMode);

                    LockMode modeToDowngrade = waiter.recalculateMode();

                    if (!waiter.locked()) {
                        assert waiter.lockMode() == modeToDowngrade : "The lock mode is not locked [mode=" + lockMode + ']';

                        return false;
                    }

                    if (modeToDowngrade == null) {
                        toNotify = release(txId);
                    } else {
                        toNotify = downgrade(txId, modeToDowngrade);
                    }
                }
            }

            // Notify outside the monitor.
            for (WaiterImpl waiter : toNotify) {
                waiter.notifyLocked();
            }

            return markedForRemove;
        }

        /**
         * Releases all locks are held by a specific transaction.
         * This method should be invoked synchronously.
         *
         * @param txId Transaction id.
         * @return List of waiters to notify.
         */
        private List<WaiterImpl> release(UUID txId) {
            WaiterImpl removed = waiters.remove(txId);

            if (waiters.isEmpty()) {
                markedForRemove = true;

                return Collections.emptyList();
            }

            List<WaiterImpl> toNotify = unlockCompatibleWaiters();

            return toNotify;
        }

        /**
         * Unlock compatible waiters.
         *
         * @return List of waiters to notify.
         */
        private ArrayList<WaiterImpl> unlockCompatibleWaiters() {
            ArrayList<WaiterImpl> toNotify = new ArrayList<>();
            Set<UUID> toFail = new HashSet<>();

            for (Map.Entry<UUID, WaiterImpl> entry : waiters.entrySet()) {
                WaiterImpl tmp = entry.getValue();

                if (!tmp.locked() && isWaiterReadyToNotify(tmp, true)) {
                    toNotify.add(tmp);
                }
            }

            for (Map.Entry<UUID, WaiterImpl> entry : waiters.entrySet()) {
                WaiterImpl tmp = entry.getValue();

                if (!tmp.locked() && isWaiterReadyToNotify(tmp, false)) {
                    assert !tmp.locked();

                    toNotify.add(tmp);
                    toFail.add(tmp.txId());
                }
            }

            for (UUID failTx : toFail) {
                waiters.remove(failTx);
            }

            return toNotify;
        }

        /**
         * Gets a lock mode for this waiter.
         *
         * @param waiter Waiter.
         * @return Lock mode, which is held by the waiter or {@code null}, if the waiter holds nothing.
         */
        private LockMode lockedMode(WaiterImpl waiter) {
            LockMode mode = null;

            if (waiter.locked()) {
                mode = waiter.lockMode();
            } else if (waiter.upgraded) {
                mode = waiter.prevLockMode;
            }

            return mode;
        }

        /**
         * Downgrades a lock on a specific key.
         * This method should be invoked synchronously.
         *
         * @param txId Transaction id.
         * @param lockMode Lock mode.
         * @return List of waiters to notify.
         */
        private List<WaiterImpl> downgrade(UUID txId, LockMode lockMode) {
            WaiterImpl waiter = waiters.get(txId);

            if (waiter == null || waiter.lockMode == lockMode) {
                return Collections.emptyList();
            }

            assert waiter.lockMode != LockMode.S || lockMode != LockMode.IX :
                    "Cannot change lock [from=" + waiter.lockMode + ", to=" + lockMode + ']';

            assert waiter.lockMode.compareTo(lockMode) > 0 :
                    "Held lock mode have to be more strict than mode to downgrade [from=" + waiter.lockMode + ", to=" + lockMode
                            + ']';

            waiter.lockMode = lockMode;

            List<WaiterImpl> toNotify = unlockCompatibleWaiters();

            return toNotify;
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

        /** Holding locks by type. */
        private final Map<LockMode, Integer> locks = new HashMap<>();

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
         * The filed has a value when the waiter couldn't lock a key.
         */
        private LockException ex;

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

            locks.put(lockMode, 1);
        }

        /**
         * Adds a lock mode.
         *
         * @param lockMode Lock mode.
         * @param increment Value to increment amount.
         */
        void addLock(LockMode lockMode, int increment) {
            locks.merge(lockMode, increment, Integer::sum);
        }

        /**
         * Removes a lock mode.
         *
         * @param lockMode Lock mode.
         */
        void removeLock(LockMode lockMode) {
            Integer counter = locks.get(lockMode);

            if (counter == null || counter < 2) {
                locks.remove(lockMode);
            } else {
                locks.put(lockMode, counter - 1);
            }
        }

        /**
         * Recalculates lock mode based of all locks which the waiter has took.
         *
         * @return Recalculated lock mode.
         */
        LockMode recalculateMode() {
            LockMode mode = null;

            for (LockMode heldMode : locks.keySet()) {
                assert locks.get(heldMode) > 0 : "Incorrect lock counter [txId=" + txId + ", mode=" + heldMode + "]";

                mode = mode == null ? heldMode : LockMode.supremum(mode, heldMode);
            }

            return mode;
        }

        /**
         * Adds several locks modes to the waiter.
         *
         * @param locksToAdd Map with lock modes.
         */
        void addLocks(Map<LockMode, Integer> locksToAdd) {
            for (LockMode mode : locksToAdd.keySet()) {
                Integer inc = locksToAdd.get(mode);

                addLock(mode, inc);
            }
        }

        /** {@inheritDoc} */
        @Override
        public int compareTo(@NotNull WaiterImpl o) {
            return txId.compareTo(o.txId);
        }

        /** Notifies a future listeners. */
        private void notifyLocked() {
            if (ex != null) {
                fut.completeExceptionally(ex);
            } else {
                assert locked;

                fut.complete(null);
            }
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

        /**
         * Fails the lock waiter.
         *
         * @param e Lock exception.
         */
        private void fail(LockException e) {
            ex = e;
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
