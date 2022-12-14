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

import static org.apache.ignite.internal.tx.LockMode.IS;
import static org.apache.ignite.internal.tx.LockMode.IX;
import static org.apache.ignite.internal.tx.LockMode.NL;
import static org.apache.ignite.internal.tx.LockMode.S;
import static org.apache.ignite.internal.tx.LockMode.SIX;
import static org.apache.ignite.internal.tx.LockMode.X;
import static org.apache.ignite.lang.ErrorGroups.Transactions.ACQUIRE_LOCK_ERR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.tostring.IgniteToStringBuilder;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tx.DeadlockPreventionPolicy;
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

    public HeapLockManager() {
        this(new WaitDieDeadlockPreventionPolicy());
    }

    public HeapLockManager(DeadlockPreventionPolicy deadlockPreventionPolicy) {
        this.deadlockPreventionPolicy = deadlockPreventionPolicy;
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
    private @NotNull LockState lockState(LockKey key) {
        return locks.computeIfAbsent(key, k -> new LockState(deadlockPreventionPolicy));
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

        /** Marked for removal flag. */
        private boolean markedForRemove = false;

        public LockState(DeadlockPreventionPolicy deadlockPreventionPolicy) {
            this.waiters = new TreeMap<>(deadlockPreventionPolicy.txIdComparator());
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

                WaiterImpl prev = waiters.putIfAbsent(txId, waiter);

                // Reenter
                if (prev != null) {
                    if (prev.locked() && prev.lockMode().allowReenter(lockMode)) {
                        waiter.lock();

                        prev.upgrade(waiter);

                        return new IgniteBiTuple(CompletableFuture.completedFuture(null), prev.lockMode());
                    } else {
                        waiter.upgrade(prev);

                        assert prev.lockMode() == waiter.lockMode() :
                                "Lock modes are incorrect [prev=" + prev.lockMode() + ", new=" + waiter.lockMode() + ']';

                        waiters.put(txId, waiter); // Upgrade.
                    }
                }

                if (!isWaiterReadyToNotify(waiter, false)) {
                    return new IgniteBiTuple(waiter.fut, waiter.lockMode());
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
         * Checks current waiter.
         *
         * @param waiter Checked waiter.
         * @return True if current waiter ready to notify, false otherwise.
         */
        private boolean isWaiterReadyToNotify(WaiterImpl waiter, boolean skipFail) {
            for (Map.Entry<UUID, WaiterImpl> entry : waiters.tailMap(waiter.txId(), false).entrySet()) {
                WaiterImpl tmp = entry.getValue();
                LockMode mode = lockedMode(tmp);

                if (mode != null && !mode.isCompatible(waiter.intendedLockMode())) {
                    return false;
                }
            }

            for (Map.Entry<UUID, WaiterImpl> entry : waiters.headMap(waiter.txId()).entrySet()) {
                WaiterImpl tmp = entry.getValue();
                LockMode mode = lockedMode(tmp);

                if (mode != null && !mode.isCompatible(waiter.intendedLockMode())) {
                    if (skipFail) {
                        return false;
                    } else {
                        waiter.fail(new LockException(ACQUIRE_LOCK_ERR, "Failed to acquire a lock due to a conflict [txId=" + waiter.txId()
                                + ", waiter=" + tmp + ']'));

                        return true;
                    }
                }
            }

            waiter.lock();

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
        private ArrayList<WaiterImpl> unlockCompatibleWaiters() {
            ArrayList<WaiterImpl> toNotify = new ArrayList<>();
            Set<UUID> toFail = new HashSet<>();

            for (Map.Entry<UUID, WaiterImpl> entry : waiters.entrySet()) {
                WaiterImpl tmp = entry.getValue();

                if (tmp.hasLockIntent() && isWaiterReadyToNotify(tmp, true)) {
                    assert !tmp.hasLockIntent() : "This waiter in not locked for notification [waiter=" + tmp + ']';

                    toNotify.add(tmp);
                }
            }

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
        /** Bit mask for lock counter. */
        private static final long COUNTER_MASK = 0x3FF;

        /** Bit mask for lock intention flag. */
        private static final long INTENTION_MASK = 1 << 10;

        /** Number of bits occupied for lock counter and intention flag. */
        private static final int COUNTER_FLAG_BITS = 11;

        /** Lock modes that are considered as valid for lock counters and intention flag. Includes all lock modes except NL. */
        private static final LockMode[] LOCK_MODES = { IS, IX, S, SIX, X };

        /**
         * Lock counters. Should be considered as a bit map. Each counter occupies 10 bits, allowing {@code 2^10 -1} locks for
         * each lock mode within one transaction, and lock intention flag occupies additional 1 bit, {@link WaiterImpl#COUNTER_FLAG_BITS}
         * in total. So the 55 least significant bits of this long number are used as a map of {@link LockMode} to a pair of
         * { lock intention flag, lock counter }, assuming we have just 5 lock modes to use.
         */
        private long lockCounters = 0;

        /** Locked future. */
        @IgniteToStringExclude
        private CompletableFuture<Void> fut;

        /** Waiter transaction id. */
        private final UUID txId;

        /** The lock mode to intend to hold. */
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

            lockCounters = setIntention(addLockCounter(0, lockMode, 1), lockMode, true);
        }

        /**
         * Add a value to lock counter given as a short. Returns a result that is aware of intention flag of {@code counter} argument.
         *
         * @param counter Counter.
         * @param lockMode Lock mode.
         * @param value Value to add.
         * @return Result that is aware of intention flag of {@code counter} argument.
         */
        private long addLockCounter(long counter, LockMode lockMode, int value) {
            assert lockMode != NL;
            int offset = COUNTER_FLAG_BITS * (lockMode.ordinal() - 1);

            long newCount = ((counter >> offset) & COUNTER_MASK) + value;

            assert newCount >= 0 : "Incorrect lock counter change, lockMode=" + lockMode + ", txId=" + txId;
            assert newCount <= COUNTER_MASK : "Too many locks acquired by one transaction on one key in the same mode, "
                    + ", lockMode=" + lockMode + ", txId=" + txId;

            long mask = ~(COUNTER_MASK << offset);
            return (counter & mask) | (newCount << offset);
        }

        /**
         * Get lock count from the given counter.
         *
         * @param counter Counter.
         * @param lockMode Lock mode.
         * @return Result without an intention flag.
         */
        private static long getLockCount(long counter, LockMode lockMode) {
            assert lockMode != NL;
            int offset = COUNTER_FLAG_BITS * (lockMode.ordinal() - 1);
            return (counter >> offset) & COUNTER_MASK;
        }

        /**
         * Sets intention flag for given counter.
         *
         * @param counter Counter.
         * @param lockMode Lock mode.
         * @param intention Intention flag.
         * @return Result combining lock count and intention flag.
         */
        private static long setIntention(long counter, LockMode lockMode, boolean intention) {
            assert lockMode != NL;
            int offset = COUNTER_FLAG_BITS * (lockMode.ordinal() - 1);
            long intentionMask = INTENTION_MASK << offset;
            return intention ? counter | intentionMask : counter & ~intentionMask;
        }

        /**
         * Get intention flag from given counter.
         *
         * @param counter Counter.
         * @param lockMode Lock mode.
         * @return Intention flag.
         */
        private static boolean isIntention(long counter, LockMode lockMode) {
            assert lockMode != NL;
            int offset = COUNTER_FLAG_BITS * (lockMode.ordinal() - 1);
            return ((counter >> offset) & INTENTION_MASK) == INTENTION_MASK;
        }

        /**
         * Drop counter and intention flag for given lock mode.
         *
         * @param counter Counter.
         * @param lockMode Lock mode.
         * @return New counter.
         */
        private static long dropCounter(long counter, LockMode lockMode) {
            assert lockMode != NL;
            int offset = COUNTER_FLAG_BITS * (lockMode.ordinal() - 1);
            long mask = ~((COUNTER_MASK | INTENTION_MASK) << offset);
            return counter & mask;
        }

        /**
         * Adds a lock mode.
         *
         * @param lockMode Lock mode.
         * @param increment Value to increment amount.
         */
        void addLock(LockMode lockMode, int increment) {
            lockCounters = addLockCounter(lockCounters, lockMode, increment);
        }

        /**
         * Removes a lock mode.
         *
         * @param lockMode Lock mode.
         * @return True if the lock mode was removed, false otherwise.
         */
        private boolean removeLock(LockMode lockMode) {
            long counter = getLockCount(lockCounters, lockMode);

            if (counter < 2) {
                lockCounters = dropCounter(lockCounters, lockMode);

                return true;
            } else {
                lockCounters = addLockCounter(lockCounters, lockMode, -1);

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

            for (LockMode mode : LOCK_MODES) {
                if (getLockCount(lockCounters, mode) > 0) {
                    if (isIntention(lockCounters, mode)) {
                        newIntendedLockMode = newIntendedLockMode == null ? mode : LockMode.supremum(newIntendedLockMode, mode);
                    } else {
                        newLockMode = newLockMode == null ? mode : LockMode.supremum(newLockMode, mode);
                    }
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
            for (LockMode mode : LOCK_MODES) {
                if (isIntention(other.lockCounters, mode)) {
                    lockCounters = setIntention(lockCounters, mode, true);
                }

                addLock(mode, (int) getLockCount(other.lockCounters, mode));
            }

            recalculate();

            if (other.hasLockIntent()) {
                fut = other.fut;
            }
        }

        /**
         * Removes all locks that were intended to hold.
         */
        void refuseIntent() {
            for (LockMode mode : LOCK_MODES) {
                if (isIntention(lockCounters, mode)) {
                    lockCounters = dropCounter(lockCounters, mode);
                }
            }

            intendedLockMode = null;
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

            for (LockMode mode : LOCK_MODES) {
                lockCounters = setIntention(lockCounters, mode, false);
            }
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
            return IgniteToStringBuilder.toString(WaiterImpl.class, this, "isDone", fut.isDone());
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean isEmpty() {
        return locks.isEmpty();
    }
}
