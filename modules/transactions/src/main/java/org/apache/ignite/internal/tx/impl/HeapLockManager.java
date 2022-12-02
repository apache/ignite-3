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
import static org.apache.ignite.lang.ErrorGroups.Transactions.RELEASE_LOCK_ERR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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

            if (waiter != null && waiter.locked()) {
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
            WaiterImpl waiter;
            WaiterNotification waiterNotification;

            synchronized (waiters) {
                if (markedForRemove) {
                    return new IgniteBiTuple(null, lockMode);
                }

                waiter = waiters.computeIfAbsent(txId, k -> new WaiterImpl(txId));

                // Reenter
                if (waiter.locked()) {
                    if (waiter.lockMode().allowReenter(lockMode)) {
                        waiter.addLock(lockMode, false);

                        return new IgniteBiTuple(CompletableFuture.completedFuture(null), lockMode);
                    }
                }

                waiter.addLock(lockMode, true);

                waiterNotification = isWaiterReadyToBeNotified(waiter, null);

                if (waiterNotification.waiterNotificationType == WaiterNotificationType.FAIL) {
                    failWaiter(ACQUIRE_LOCK_ERR, waiter, waiterNotification.conflictingWaiter);
                }

                if (waiterNotification.waiterNotificationType != WaiterNotificationType.NONE) {
                    waiter.completeIntentions();
                }
            }

            // Notify outside the monitor.
            // TODO IGNITE-18316 possible races
            if (waiterNotification.waiterNotificationType != WaiterNotificationType.NONE) {
                waiter.notifyWaiter();
            }

            return new IgniteBiTuple(waiter.fut(), lockMode);
        }

        /**
         * Fail waiter, writing the exception and removing it from waiters collection, if it is not locked on another lock mode.
         *
         * @param code Exception code.
         * @param waiter Waiter
         * @param conflictingWaiter Conflicting waiter.
         */
        private void failWaiter(int code, WaiterImpl waiter, Waiter conflictingWaiter) {
            if (!waiter.locked()) {
                waiters.remove(waiter.txId());
            }

            waiter.fail(new LockException(code, "Failed to acquire a lock due to a conflict [txId=" + waiter.txId()
                    + ", waiter=" + conflictingWaiter + ']'));
        }

        /**
         * Checks current waiter.
         *
         * @param waiter Checked waiter.
         * @param additional Collection of additional waiters to check, assuming their intention locks are acquired.
         * @return Waiter notification, see {@link WaiterNotification}.
         */
        private WaiterNotification isWaiterReadyToBeNotified(
                WaiterImpl waiter,
                @Nullable Collection<WaiterImpl> additional
        ) {
            // TODO descending map call should be changed under IGNITE-18043
            WaiterNotification waiterNotification = isWaiterReadyToBeNotified(waiter, waiters.descendingMap().values(), false);

            if (waiterNotification.waiterNotificationType == WaiterNotificationType.FAIL || additional == null) {
                return waiterNotification;
            }

            // if waiterNotification is NONE or LOCK, we should still check additional
            WaiterNotification intentionWaiterNotification = isWaiterReadyToBeNotified(waiter, additional, true);

            if (intentionWaiterNotification.waiterNotificationType == WaiterNotificationType.FAIL) {
                return intentionWaiterNotification;
            } else if (waiterNotification.waiterNotificationType == WaiterNotificationType.NONE
                    || intentionWaiterNotification.waiterNotificationType == WaiterNotificationType.NONE) {
                return WaiterNotification.NO_NOTIFICATION;
            } else {
                return WaiterNotification.LOCK_NOTIFICATION;
            }
        }

        /**
         * Checks current waiter.
         *
         * @param waiter Checked waiter.
         * @param waitersToCheck Collection of waiters to check.
         * @param considerIntentionsAsLocks Whether to consider intention locks as locks.
         * @return Waiter notification, see {@link WaiterNotification}.
         */
        private WaiterNotification isWaiterReadyToBeNotified(
                WaiterImpl waiter,
                @Nullable Collection<WaiterImpl> waitersToCheck,
                boolean considerIntentionsAsLocks
        ) {
            for (WaiterImpl tmp : waitersToCheck) {
                LockMode mode = considerIntentionsAsLocks ? tmp.intentionLockMode() : tmp.lockMode();
                boolean mayBeConflict = considerIntentionsAsLocks ? tmp.waiting() : tmp.locked();

                int cmp = tmp.compareTo(waiter);

                if (cmp != 0 && mayBeConflict && !mode.isCompatible(waiter.intentionLockMode())) {
                    // TODO IGNITE-18043
                    if (cmp > 0) {
                        return WaiterNotification.NO_NOTIFICATION;
                    } else {
                        return new WaiterNotification(WaiterNotificationType.FAIL, tmp);
                    }
                }
            }

            return WaiterNotification.LOCK_NOTIFICATION;
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

                for (WaiterImpl waiter : toNotify) {
                    waiter.completeIntentions();
                }
            }

            // Notify outside the monitor.
            // TODO IGNITE-18316 possible races
            for (WaiterImpl waiter : toNotify) {
                waiter.notifyWaiter();
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
            Set<WaiterImpl> toNotify = Collections.emptySet();
            synchronized (waiters) {
                WaiterImpl waiter = waiters.get(txId);

                if (waiter != null) {
                    waiter.removeLock(lockMode);

                    if (!waiter.locked() && !waiter.waiting()) {
                        toNotify = release(txId);
                    } else {
                        toNotify = unlockCompatibleWaiters();
                    }
                }

                for (WaiterImpl w : toNotify) {
                    w.completeIntentions();
                }
            }

            // Notify outside the monitor.
            // TODO IGNITE-18316 possible races
            for (WaiterImpl waiter : toNotify) {
                waiter.notifyWaiter();
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
        private Set<WaiterImpl> release(UUID txId) {
            waiters.remove(txId);

            if (waiters.isEmpty()) {
                markedForRemove = true;
            }

            Set<WaiterImpl> toNotify = unlockCompatibleWaiters();

            return toNotify;
        }

        /**
         * Unlock compatible waiters.
         *
         * @return List of waiters to notify.
         */
        private Set<WaiterImpl> unlockCompatibleWaiters() {
            TreeSet<WaiterImpl> newlyLocked = new TreeSet<>();
            Set<IgniteBiTuple<WaiterImpl, WaiterImpl>> failCandidates = new HashSet<>();

            for (WaiterImpl tmp : waiters.values()) {
                if (tmp.waiting()) {
                    WaiterNotification waiterNotification = isWaiterReadyToBeNotified(tmp, newlyLocked);

                    if (waiterNotification.waiterNotificationType == WaiterNotificationType.LOCK) {
                        newlyLocked.add(tmp);
                    } else if (waiterNotification.waiterNotificationType == WaiterNotificationType.FAIL) {
                        failCandidates.add(new IgniteBiTuple<>(tmp, waiterNotification.conflictingWaiter));
                    }
                }
            }

            if (!newlyLocked.isEmpty()) {
                for (WaiterImpl tmp : waiters.values()) {
                    if (tmp.waiting()) {
                        WaiterNotification waiterNotification = isWaiterReadyToBeNotified(tmp, newlyLocked, true);

                        if (waiterNotification.waiterNotificationType == WaiterNotificationType.FAIL) {
                            failCandidates.add(new IgniteBiTuple<>(tmp, waiterNotification.conflictingWaiter));
                        }
                    }
                }

                // TODO IGNITE-18043
                UUID maxNewlyLockedTxId = newlyLocked.last().txId();

                for (Iterator<IgniteBiTuple<WaiterImpl, WaiterImpl>> it = failCandidates.iterator(); it.hasNext(); ) {
                    IgniteBiTuple<WaiterImpl, WaiterImpl> next = it.next();
                    WaiterImpl failCandidate = next.get1();

                    // Fail candidates are allowed to wait further if there is at least one waiter among newly locked ones which
                    // the fail candidate is allowed to wait.
                    // TODO IGNITE-18043
                    if (failCandidate.txId().compareTo(maxNewlyLockedTxId) < 0) {
                        it.remove();
                    }
                }
            }

            Set<WaiterImpl> res = new HashSet<>(newlyLocked);

            for (IgniteBiTuple<WaiterImpl, WaiterImpl> failed : failCandidates) {
                WaiterImpl waiter = failed.get1();

                failWaiter(RELEASE_LOCK_ERR, waiter, failed.get2());

                res.add(waiter);
            }

            return res;
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
    private static class WaiterImpl implements Waiter {
        /** Mask for intention flag. Intention flag is the most significant bit in lock count. */
        private static final short INTENTION_MASK = (short) (1 << (Short.SIZE - 1));

        /**
         * Lock counts array. The most significant bits in each of these shorts are intention flags, denoting whether the counter of
         * corresponding lock mode is the count of actually acquired locks or the count of lock intentions.
         */
        private final short[] lockCounts = new short[LockMode.values().length];

        /** The supremum of all locks acquired for this waiter. */
        private LockMode lockedSupremum;

        /**
         * The supremum of all lock intentions acquired for this waiter. It is calculated from both actually acquired locks and
         * lock intentions.
         */
        private LockMode intentionSupremum;

        /** Locked future. */
        @IgniteToStringExclude
        private CompletableFuture<Void> fut;

        /** Waiter transaction id. */
        private final UUID txId;

        /**
         * The filed has a value when the waiter couldn't lock a key.
         */
        private LockException ex;

        /**
         * The constructor.
         *
         * @param txId Transaction id.
         */
        public WaiterImpl(UUID txId) {
            this.txId = txId;
        }

        /**
         * Add a value to lock counter given as a short. Returns a result that is aware of intention flag of {@code counter} argument.
         *
         * @param counter Counter.
         * @param value Value to add.
         * @return Result that is aware of intention flag of {@code counter} argument.
         */
        private static short addLockCounter(short counter, short value) {
            return (short) (((counter & (~INTENTION_MASK)) + value) | (counter & INTENTION_MASK));
        }

        /**
         * Get lock count from the given counter.
         *
         * @param counter Counter.
         * @return Result without an intention flag.
         */
        private static short getLockCount(short counter) {
            return (short) (counter & (~INTENTION_MASK));
        }

        /**
         * Sets intention flag for given counter.
         *
         * @param counter Counter.
         * @param intention Intention flag.
         * @return Result combining lock count and intention flag.
         */
        private static short setIntention(short counter, boolean intention) {
            return (short) (intention ? counter | INTENTION_MASK : counter & (~INTENTION_MASK));
        }

        /**
         * Get intention flag from given counter.
         *
         * @param counter Counter.
         * @return Intention flag.
         */
        private static boolean isIntention(short counter) {
            return (counter & INTENTION_MASK) == INTENTION_MASK;
        }

        /**
         * Supremum of two lock modes, assuming the one lock mode can be null. In this case, second lock mode would be supremum.
         *
         * @param lockMode1 Lock mode.
         * @param lockMode2 Lock mode.
         * @return Supremum.
         */
        private static LockMode supremumNullAware(@Nullable LockMode lockMode1, LockMode lockMode2) {
            return lockMode1 == null ? lockMode2 : LockMode.supremum(lockMode1, lockMode2);
        }

        /**
         * Adds a lock mode.
         *
         * @param lockMode Lock mode.
         * @param intention Lock intention flag.
         */
        public void addLock(LockMode lockMode, boolean intention) {
            int m = lockMode.ordinal();

            final short count = lockCounts[m];

            short actualCount = getLockCount(count);

            if (actualCount > 0) {
                assert isIntention(count) == intention : "Switching the lock intention is impossible within this operation, intention="
                        + intention + ", current=" + isIntention(count) + ", lockMode=" + lockMode + ", txId=" + txId;

                assert getLockCount(count) < Short.MAX_VALUE : "Too many locks acquired by one transaction on one key in the same mode, "
                        + ", lockMode=" + lockMode + ", txId=" + txId;

                lockCounts[m] = addLockCounter(count, (short) 1);
            } else {
                assert actualCount == 0;

                if (!hasLocks(true)) {
                    fut = new CompletableFuture<>();
                }

                lockCounts[m] = setIntention((short) 1, intention);

                recalculateSuprema();
            }
        }

        /**
         * Removes a lock mode.
         *
         * @param lockMode Lock mode.
         */
        public void removeLock(LockMode lockMode) {
            int m = lockMode.ordinal();

            final short count = lockCounts[m];
            final short actualCount = getLockCount(count);

            assert actualCount > 0 : "Trying to remove lock which does not exist, lockMode=" + lockMode + ", txId=" + txId;
            assert !isIntention(count) : "Trying to remove lock intention as an acquired lock, lockMode=" + lockMode + ", txId=" + txId;

            if (actualCount == 1) {
                lockCounts[m] = 0;

                recalculateSuprema();
            } else {
                lockCounts[m] = addLockCounter(count, (short) -1);
            }
        }

        /**
         * Recalculates cached lock mode suprema, {@link #lockedSupremum} and {@link #intentionSupremum} based of all locks and
         * lock intentions which the waiter has took.
         */
        private void recalculateSuprema() {
            lockedSupremum = null;
            intentionSupremum = null;

            for (int i = 0; i < lockCounts.length; i++) {
                short c = lockCounts[i];

                short actualCount = getLockCount(c);

                lockedSupremum = !isIntention(c) && actualCount > 0
                        ? supremumNullAware(lockedSupremum, LockMode.values()[i]) : lockedSupremum;

                intentionSupremum = actualCount > 0 ? supremumNullAware(intentionSupremum, LockMode.values()[i]) : intentionSupremum;
            }
        }

        /** Notifies a future listeners. */
        public void notifyWaiter() {
            if (ex != null) {
                fut.completeExceptionally(ex);
            } else {
                fut.complete(null);
            }

            ex = null;
        }

        /**
         * Complete lock intentions and either transform them to locks or forget them.
         *
         * @return Count of completed lock intentions.
         */
        public void completeIntentions() {
            int completed = completeIntentions(ex == null);
            assert completed > 0;
        }

        /**
         * Complete lock intentions and either transform them to locks or forget them, depending on {@code lock} argument.
         *
         * @param lock Whether to lock.
         * @return Count of completed lock intentions.
         */
        private int completeIntentions(boolean lock) {
            int completed = 0;

            for (int i = 0; i < lockCounts.length; i++) {
                short c = lockCounts[i];

                if (isIntention(c) && getLockCount(c) > 0) {
                    if (lock) {
                        lockCounts[i] = setIntention(c, false);
                    } else {
                        lockCounts[i] = 0;
                    }

                    completed++;
                }
            }

            recalculateSuprema();

            return completed;
        }

        private boolean hasLocks(boolean intention) {
            for (short c : lockCounts) {
                if (isIntention(c) == intention && getLockCount(c) > 0) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public boolean locked() {
            return lockedSupremum != null;
        }

        public boolean waiting() {
            return intentionSupremum != null && intentionSupremum != lockedSupremum;
        }

        @Override
        public LockMode lockMode() {
            return lockedSupremum;
        }

        public LockMode intentionLockMode() {
            return intentionSupremum;
        }

        /**
         * Fails the lock waiter.
         *
         * @param e Lock exception.
         */
        public void fail(LockException e) {
            assert fut != null;

            ex = e;
        }

        public CompletableFuture<Void> fut() {
            return fut;
        }

        /** {@inheritDoc} */
        @Override
        public UUID txId() {
            return txId;
        }

        /** {@inheritDoc} */
        @Override
        public int compareTo(@NotNull Waiter o) {
            return txId.compareTo(o.txId());
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

    /**
     * Waiter notification type denotes the type of notification the waiter is going to receive.
     */
    private enum WaiterNotificationType {
        /** No notification. */
        NONE,

        /** Waiter's lock intentions should be turned into actual locks. */
        LOCK,

        /** Waiter should complete it's lock intentions and lock future with exception. */
        FAIL;
    }

    /**
     * Waiter notification.
     */
    private static class WaiterNotification {
        static final WaiterNotification NO_NOTIFICATION = new WaiterNotification(WaiterNotificationType.NONE);
        static final WaiterNotification LOCK_NOTIFICATION = new WaiterNotification(WaiterNotificationType.LOCK);

        /** Waiter notification type. */
        final WaiterNotificationType waiterNotificationType;

        /** Conflicting waiter, in case the notification type is equal to {@link WaiterNotificationType#FAIL}. */
        final WaiterImpl conflictingWaiter;

        WaiterNotification(WaiterNotificationType waiterNotificationType) {
            this(waiterNotificationType, null);
        }

        WaiterNotification(WaiterNotificationType waiterNotificationType, WaiterImpl conflictingWaiter) {
            this.waiterNotificationType = waiterNotificationType;

            if (conflictingWaiter != null) {
                assert waiterNotificationType == WaiterNotificationType.FAIL;
            }

            this.conflictingWaiter = conflictingWaiter;
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean isEmpty() {
        return locks.isEmpty();
    }
}
