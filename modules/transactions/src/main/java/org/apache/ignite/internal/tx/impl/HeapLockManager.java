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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.lang.ErrorGroups.Transactions.ACQUIRE_LOCK_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.ACQUIRE_LOCK_TIMEOUT_ERR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.DeadlockPreventionPolicy;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.Waiter;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link LockManager} implementation which stores lock queues in the heap.
 *
 * <p>Lock waiters are placed in the queue, ordered according to comparator provided by {@link HeapLockManager#deadlockPreventionPolicy}.
 * When a new waiter is placed in the queue, it's validated against current lock owner: if there is an owner with a higher transaction id
 * lock request is denied.
 *
 * <p>Read lock can be upgraded to write lock (only available for the lowest read-locked entry of
 * the queue).
 */
public class HeapLockManager implements LockManager {
    private ConcurrentHashMap<LockKey, LockState> locks = new ConcurrentHashMap<>();

    private final DeadlockPreventionPolicy deadlockPreventionPolicy;

    /** Executor that is used to fail waiters after timeout. */
    private final Executor delayedExecutor;

    /**
     * Constructor.
     */
    public HeapLockManager() {
        this(new WaitDieDeadlockPreventionPolicy());
    }

    /**
     * Constructor.
     *
     * @param deadlockPreventionPolicy Deadlock prevention policy.
     */
    public HeapLockManager(DeadlockPreventionPolicy deadlockPreventionPolicy) {
        this.deadlockPreventionPolicy = deadlockPreventionPolicy;
        this.delayedExecutor = deadlockPreventionPolicy.waitTimeout() > 0
                ? CompletableFuture.delayedExecutor(deadlockPreventionPolicy.waitTimeout(), TimeUnit.MILLISECONDS)
                : null;
    }

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
    private LockState lockState(LockKey key) {
        return locks.computeIfAbsent(key, k -> new LockState(deadlockPreventionPolicy, delayedExecutor));
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
        private final TreeMap<UUID, WaiterImpl> waiters;

        private final DeadlockPreventionPolicy deadlockPreventionPolicy;

        /** Delayed executor for waiters timeout callback. */
        private final Executor delayedExecutor;

        /** Marked for removal flag. */
        private boolean markedForRemove = false;

        public LockState(DeadlockPreventionPolicy deadlockPreventionPolicy, Executor delayedExecutor) {
            Comparator<UUID> txComparator =
                    deadlockPreventionPolicy.txIdComparator() != null ? deadlockPreventionPolicy.txIdComparator() : UUID::compareTo;

            this.waiters = new TreeMap<>(txComparator);
            this.deadlockPreventionPolicy = deadlockPreventionPolicy;
            this.delayedExecutor = delayedExecutor;
        }

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

                // We always replace the previous waiter with the new one. If the previous waiter has lock intention then incomplete
                // lock future is copied to the new waiter. This guarantees that, if the previous waiter was locked concurrently, then
                // it doesn't have any lock intentions, and the future is not copied to the new waiter. Otherwise, if there is lock
                // intention, this means that the lock future contained in previous waiter, is not going to be completed and can be
                // copied safely.
                WaiterImpl prev = waiters.put(txId, waiter);

                // Reenter
                if (prev != null) {
                    if (prev.locked() && prev.lockMode().allowReenter(lockMode)) {
                        waiter.lock();

                        waiter.upgrade(prev);

                        return new IgniteBiTuple(completedFuture(null), prev.lockMode());
                    } else {
                        waiter.upgrade(prev);

                        assert prev.lockMode() == waiter.lockMode() :
                                "Lock modes are incorrect [prev=" + prev.lockMode() + ", new=" + waiter.lockMode() + ']';
                    }
                }

                if (!isWaiterReadyToNotify(waiter, false)) {
                    if (deadlockPreventionPolicy.waitTimeout() > 0) {
                        setWaiterTimeout(waiter);
                    }

                    return new IgniteBiTuple<>(waiter.fut, waiter.lockMode());
                }

                if (!waiter.locked()) {
                    waiters.remove(waiter.txId());
                } else if (waiter.hasLockIntent()) {
                    waiter.refuseIntent(); // Restore old lock.
                }
            }

            // Notify outside the monitor.
            waiter.notifyLocked();

            return new IgniteBiTuple(waiter.fut, waiter.lockMode());
        }

        /**
         * Checks current waiter. It can change the internal state of the waiter.
         *
         * @param waiter Checked waiter.
         * @return True if current waiter ready to notify, false otherwise.
         */
        private boolean isWaiterReadyToNotify(WaiterImpl waiter, boolean skipFail) {
            for (Map.Entry<UUID, WaiterImpl> entry : waiters.tailMap(waiter.txId(), false).entrySet()) {
                WaiterImpl tmp = entry.getValue();
                LockMode mode = lockedMode(tmp);

                if (mode != null && !mode.isCompatible(waiter.intendedLockMode())) {
                    if (!deadlockPreventionPolicy.usePriority() && deadlockPreventionPolicy.waitTimeout() == 0) {
                        waiter.fail(lockException(waiter.txId(), tmp));

                        return true;
                    }

                    return false;
                }
            }

            for (Map.Entry<UUID, WaiterImpl> entry : waiters.headMap(waiter.txId()).entrySet()) {
                WaiterImpl tmp = entry.getValue();
                LockMode mode = lockedMode(tmp);

                if (mode != null && !mode.isCompatible(waiter.intendedLockMode())) {
                    if (skipFail) {
                        return false;
                    } else if (deadlockPreventionPolicy.waitTimeout() == 0) {
                        waiter.fail(lockException(waiter.txId(), tmp));

                        return true;
                    } else {
                        return false;
                    }
                }
            }

            waiter.lock();

            return true;
        }

        /**
         * Create lock exception with given parameters.
         *
         * @param txId Transaction id.
         * @param conflictingWaiter Conflicting waiter.
         * @return Lock exception.
         */
        private LockException lockException(UUID txId, WaiterImpl conflictingWaiter) {
            return new LockException(ACQUIRE_LOCK_ERR, "Failed to acquire a lock due to a conflict [txId="
                    + txId + ", conflictingWaiter=" + conflictingWaiter + ']');
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
                    assert lockMode.supremum(lockMode, waiter.lockMode()) == waiter.lockMode() :
                            "The lock mode is not locked [mode=" + lockMode + ", locked=" + waiter.lockMode() + ']';

                    LockMode modeFromDowngrade = waiter.recalculateMode(lockMode);

                    if (!waiter.locked() && !waiter.hasLockIntent()) {
                        toNotify = release(txId);
                    } else if (modeFromDowngrade != waiter.lockMode()) {
                        toNotify = unlockCompatibleWaiters();
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
            waiters.remove(txId);

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
        private List<WaiterImpl> unlockCompatibleWaiters() {
            if (!deadlockPreventionPolicy.usePriority() && deadlockPreventionPolicy.waitTimeout() == 0) {
                return Collections.emptyList();
            }

            ArrayList<WaiterImpl> toNotify = new ArrayList<>();
            Set<UUID> toFail = new HashSet<>();

            for (Map.Entry<UUID, WaiterImpl> entry : waiters.entrySet()) {
                WaiterImpl tmp = entry.getValue();

                if (tmp.hasLockIntent() && isWaiterReadyToNotify(tmp, true)) {
                    assert !tmp.hasLockIntent() : "This waiter in not locked for notification [waiter=" + tmp + ']';

                    toNotify.add(tmp);
                }
            }

            if (deadlockPreventionPolicy.usePriority() && deadlockPreventionPolicy.waitTimeout() >= 0) {
                for (Map.Entry<UUID, WaiterImpl> entry : waiters.entrySet()) {
                    WaiterImpl tmp = entry.getValue();

                    if (tmp.hasLockIntent() && isWaiterReadyToNotify(tmp, false)) {
                        assert tmp.hasLockIntent() : "Only failed waiter can be notified here [waiter=" + tmp + ']';

                        toNotify.add(tmp);
                        toFail.add(tmp.txId());
                    }
                }

                for (UUID failTx : toFail) {
                    var w = waiters.get(failTx);

                    if (w.locked()) {
                        w.refuseIntent();
                    } else {
                        waiters.remove(failTx);
                    }
                }
            }

            return toNotify;
        }

        /**
         * Makes the waiter fail after specified timeout (in milliseconds), if intended lock was not acquired within this timeout.
         *
         * @param waiter Waiter.
         */
        private void setWaiterTimeout(WaiterImpl waiter) {
            delayedExecutor.execute(() -> {
                if (!waiter.fut.isDone()) {
                    waiter.fut.completeExceptionally(new LockException(ACQUIRE_LOCK_TIMEOUT_ERR, "Failed to acquire a lock due to "
                            + "timeout [txId=" + waiter.txId() + ", waiter=" + waiter
                            + ", timeout=" + deadlockPreventionPolicy.waitTimeout() + ']'));
                }
            });
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
            }

            return mode;
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
        /**
         * Holding locks by type.
         * TODO: IGNITE-18350 Abandon the collection in favor of BitSet.
         */
        private final Map<LockMode, Integer> locks = new HashMap<>();

        /**
         * Lock modes are marked as intended, but have not taken yet. This is NOT specific to intention lock modes, such as IS and IX.
         * TODO: IGNITE-18350 Abandon the collection in favor of BitSet.
         */
        private final Set<LockMode> intendedLocks = new HashSet<>();

        /** Locked future. */
        @IgniteToStringExclude
        private CompletableFuture<Void> fut;

        /** Waiter transaction id. */
        private final UUID txId;

        /** The lock mode to intend to hold. This is NOT specific to intention lock modes, such as IS and IX. */
        private LockMode intendedLockMode;

        /** The lock mode. */
        private LockMode lockMode;

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
            this.intendedLockMode = lockMode;

            locks.put(lockMode, 1);
            intendedLocks.add(lockMode);
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
         * @return True if the lock mode was removed, false otherwise.
         */
        private boolean removeLock(LockMode lockMode) {
            Integer counter = locks.get(lockMode);

            if (counter == null || counter < 2) {
                locks.remove(lockMode);

                return true;
            } else {
                locks.put(lockMode, counter - 1);

                return false;
            }
        }

        /**
         * Recalculates lock mode based of all locks which the waiter has taken.
         *
         * @param modeToRemove Mode without which, the recalculation will happen.
         * @return Previous lock mode.
         */
        LockMode recalculateMode(LockMode modeToRemove) {
            if (!removeLock(modeToRemove)) {
                return lockMode;
            }

            return recalculate();
        }

        /**
         * Recalculates lock supremums.
         *
         * @return Previous lock mode.
         */
        private LockMode recalculate() {
            LockMode newIntendedLockMode = null;
            LockMode newLockMode = null;

            for (LockMode mode : locks.keySet()) {
                assert locks.get(mode) > 0 : "Incorrect lock counter [txId=" + txId + ", mode=" + mode + "]";

                if (intendedLocks.contains(mode)) {
                    newIntendedLockMode = newIntendedLockMode == null ? mode : LockMode.supremum(newIntendedLockMode, mode);
                } else {
                    newLockMode = newLockMode == null ? mode : LockMode.supremum(newLockMode, mode);
                }
            }

            LockMode mode = lockMode;

            lockMode = newLockMode;
            intendedLockMode = newLockMode != null && newIntendedLockMode != null ? LockMode.supremum(newLockMode, newIntendedLockMode)
                    : newIntendedLockMode;

            return mode;
        }

        /**
         * Merge all locks that were held by another waiter to the current one.
         *
         * @param other Other waiter.
         */
        void upgrade(WaiterImpl other) {
            intendedLocks.addAll(other.intendedLocks);

            other.locks.entrySet().forEach(entry -> addLock(entry.getKey(), entry.getValue()));

            recalculate();

            if (other.hasLockIntent()) {
                fut = other.fut;
            }
        }

        /**
         * Removes all locks that were intended to hold.
         */
        void refuseIntent() {
            for (LockMode mode : intendedLocks) {
                locks.remove(mode);
            }

            intendedLocks.clear();
            intendedLockMode = null;
        }

        /** {@inheritDoc} */
        @Override
        public int compareTo(WaiterImpl o) {
            return txId.compareTo(o.txId);
        }

        /** Notifies a future listeners. */
        private void notifyLocked() {
            if (ex != null) {
                fut.completeExceptionally(ex);
            } else {
                assert lockMode != null;

                fut.complete(null);
            }
        }

        /** {@inheritDoc} */
        @Override
        public boolean locked() {
            return this.lockMode != null;
        }

        /**
         * Checks is the waiter has any intended to lock a key.
         *
         * @return True if the waiter has an intended lock, false otherwise.
         */
        public boolean hasLockIntent() {
            return this.intendedLockMode != null;
        }

        /** {@inheritDoc} */
        @Override
        public LockMode lockMode() {
            return lockMode;
        }

        /** {@inheritDoc} */
        @Override
        public LockMode intendedLockMode() {
            return intendedLockMode;
        }

        /** Grant a lock. */
        private void lock() {
            lockMode = intendedLockMode;

            intendedLockMode = null;

            intendedLocks.clear();
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
