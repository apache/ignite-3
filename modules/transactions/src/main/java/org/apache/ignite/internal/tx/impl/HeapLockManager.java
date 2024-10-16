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

import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.tx.event.LockEvent.LOCK_CONFLICT;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Transactions.ACQUIRE_LOCK_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.ACQUIRE_LOCK_TIMEOUT_ERR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.DeadlockPreventionPolicy;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.Waiter;
import org.apache.ignite.internal.tx.event.LockEvent;
import org.apache.ignite.internal.tx.event.LockEventParameters;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * A {@link LockManager} implementation which stores lock queues in the heap.
 *
 * <p>Lock waiters are placed in the queue, ordered according to comparator provided by {@link HeapLockManager#deadlockPreventionPolicy}.
 * When a new waiter is placed in the queue, it's validated against current lock owner: if there is an owner with a higher priority (as
 * defined by comparator) lock request is denied.
 *
 * <p>Read lock can be upgraded to write lock (only available for the lowest read-locked entry of
 * the queue).
 *
 * <p>Additionally limits the lock map size.
 */
public class HeapLockManager extends AbstractEventProducer<LockEvent, LockEventParameters> implements LockManager {
    /**
     * Table size. TODO make it configurable IGNITE-20694
     */
    public static final int SLOTS = 131072;

    /**
     * Empty slots.
     */
    private final ConcurrentLinkedQueue<LockState> empty = new ConcurrentLinkedQueue<>();

    /**
     * Mapped slots.
     */
    private final ConcurrentHashMap<LockKey, LockState> locks;

    /**
     * Raw slots.
     */
    private final LockState[] slots;

    /**
     * The policy.
     */
    private final DeadlockPreventionPolicy deadlockPreventionPolicy;

    /**
     * Executor that is used to fail waiters after timeout.
     */
    private final Executor delayedExecutor;

    /**
     * Enlisted transactions.
     */
    private final ConcurrentHashMap<UUID, ConcurrentLinkedQueue<Releasable>> txMap = new ConcurrentHashMap<>(1024);

    /**
     * Coarse locks.
     */
    private final ConcurrentHashMap<Object, CoarseLockState> coarseMap = new ConcurrentHashMap<>();

    /**
     * Constructor.
     */
    public HeapLockManager() {
        this(new WaitDieDeadlockPreventionPolicy(), SLOTS, SLOTS);
    }

    /**
     * Constructor.
     */
    public HeapLockManager(DeadlockPreventionPolicy deadlockPreventionPolicy) {
        this(deadlockPreventionPolicy, SLOTS, SLOTS);
    }

    /**
     * Constructor.
     *
     * @param deadlockPreventionPolicy Deadlock prevention policy.
     * @param maxSize Raw slots size.
     * @param mapSize Lock map size.
     */
    public HeapLockManager(DeadlockPreventionPolicy deadlockPreventionPolicy, int maxSize, int mapSize) {
        if (mapSize > maxSize) {
            throw new IllegalArgumentException("maxSize=" + maxSize + " < mapSize=" + mapSize);
        }

        this.deadlockPreventionPolicy = deadlockPreventionPolicy;
        this.delayedExecutor = deadlockPreventionPolicy.waitTimeout() > 0
                ? CompletableFuture.delayedExecutor(deadlockPreventionPolicy.waitTimeout(), TimeUnit.MILLISECONDS)
                : null;

        locks = new ConcurrentHashMap<>(mapSize);

        LockState[] tmp = new LockState[maxSize];
        for (int i = 0; i < tmp.length; i++) {
            LockState lockState = new LockState();
            if (i < mapSize) {
                empty.add(lockState);
            }
            tmp[i] = lockState;
        }

        slots = tmp; // Atomic init.
    }

    @Override
    public CompletableFuture<Lock> acquire(UUID txId, LockKey lockKey, LockMode lockMode) {
        if (lockKey.contextId() == null) { // Treat this lock as a hierarchy(coarse) lock.
            // Try fast path.
            CoarseLockState state = coarseMap.get(lockKey);

            if (state == null) {
                // Atomically init state.
                CoarseLockState s = new CoarseLockState(lockKey);
                CoarseLockState cur = coarseMap.putIfAbsent(lockKey, s);
                state = (cur == null ? s : cur);
            }

            return state.acquire(txId, lockKey, lockMode);
        }

        while (true) {
            LockState state = lockState(lockKey);

            IgniteBiTuple<CompletableFuture<Void>, LockMode> futureTuple = state.tryAcquire(txId, lockMode);

            if (futureTuple.get1() == null) {
                continue; // State is marked for remove, need retry.
            }

            LockMode newLockMode = futureTuple.get2();

            return futureTuple.get1().thenApply(res -> new Lock(lockKey, newLockMode, txId));
        }
    }

    @Override
    @TestOnly
    public void release(Lock lock) {
        if (lock.lockKey().contextId() == null) {
            CoarseLockState lockState2 = coarseMap.get(lock.lockKey());
            if (lockState2 != null) {
                lockState2.release(lock);
            }
            return;
        }

        LockState state = lockState(lock.lockKey());

        if (state.tryRelease(lock.txId())) {
            locks.compute(lock.lockKey(), (k, v) -> adjustLockState(state, v));
        }
    }

    @Override
    public void release(UUID txId, LockKey lockKey, LockMode lockMode) {
        if (lockKey.contextId() == null) {
            throw new IllegalArgumentException("Coarse locks don't support downgrading");
        }

        LockState state = lockState(lockKey);

        if (state.tryRelease(txId, lockMode)) {
            locks.compute(lockKey, (k, v) -> adjustLockState(state, v));
        }
    }

    @Override
    public void releaseAll(UUID txId) {
        ConcurrentLinkedQueue<Releasable> states = this.txMap.remove(txId);

        if (states != null) {
            // Default size corresponds to average number of entities used by transaction. Estimate it to 5.
            List<Releasable> delayed = new ArrayList<>(4);
            for (Releasable state : states) {
                if (state.coarse()) {
                    delayed.add(state); // Delay release.
                    continue;
                }

                if (state.tryRelease(txId)) {
                    LockKey key = state.key(); // State may be already invalidated.
                    if (key != null) {
                        locks.compute(key, (k, v) -> adjustLockState((LockState) state, v));
                    }
                }
            }

            // Unlock coarse locks after all.
            for (Releasable state : delayed) {
                state.tryRelease(txId);
            }
        }
    }

    @Override
    public Iterator<Lock> locks(UUID txId) {
        ConcurrentLinkedQueue<Releasable> lockStates = txMap.get(txId);

        List<Lock> result = new ArrayList<>();

        if (lockStates != null) {
            for (Releasable lockState : lockStates) {
                Lock lock = lockState.lock(txId);
                if (lock != null) {
                    result.add(lock);
                }
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
        int h = spread(key.hashCode());
        int index = h & (slots.length - 1);

        LockState[] res = new LockState[1];

        locks.compute(key, (k, v) -> {
            if (v == null) {
                v = empty.poll();
                if (v == null) {
                    res[0] = slots[index];
                    assert !res[0].markedForRemove;
                } else {
                    v.markedForRemove = false;
                    v.key = k;
                    res[0] = v;
                }
            } else {
                res[0] = v;
            }

            return v;
        });

        return res[0];
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

    /** {@inheritDoc} */
    @Override
    public boolean isEmpty() {
        for (LockState slot : slots) {
            if (slot.waitersCount() != 0) {
                return false;
            }
        }

        for (CoarseLockState value : coarseMap.values()) {
            if (!value.sLocksOwners.isEmpty()) {
                return false;
            }

            if (!value.ixLocksOwners.isEmpty()) {
                return false;
            }
        }

        return true;
    }

    @Nullable
    private LockState adjustLockState(LockState state, LockState v) {
        // Mapping may already change.
        if (v != state) {
            return v;
        }

        synchronized (v.waiters) {
            if (v.waiters.isEmpty()) {
                v.markedForRemove = true;
                v.key = null;
                empty.add(v);
                return null;
            } else {
                return v;
            }
        }
    }

    private void track(UUID txId, Releasable val) {
        txMap.compute(txId, (k, v) -> {
            if (v == null) {
                v = new ConcurrentLinkedQueue<>();
            }

            v.add(val);

            return v;
        });
    }

    /**
     * Create lock exception with given parameters.
     *
     * @param locker Locker.
     * @param holder Lock holder.
     * @return Lock exception.
     */
    private static LockException lockException(UUID locker, UUID holder) {
        return new LockException(ACQUIRE_LOCK_ERR,
                "Failed to acquire a lock due to a possible deadlock [locker=" + locker + ", holder=" + holder + ']');
    }

    /**
     * Create lock exception when lock holder is believed to be missing.
     *
     * @param locker Locker.
     * @param holder Lock holder.
     * @return Lock exception.
     */
    private static LockException abandonedLockException(UUID locker, UUID holder) {
        return new LockException(ACQUIRE_LOCK_ERR,
                "Failed to acquire an abandoned lock due to a possible deadlock [locker=" + locker + ", holder=" + holder + ']');
    }

    public interface Releasable {
        /**
         * Tries to release a lock.
         *
         * @param txId Tx id.
         * @return {@code True} if lock state requires cleanup after release.
         */
        boolean tryRelease(UUID txId);

        /**
         * Gets associated lock key.
         *
         * @return Lock key.
         */
        LockKey key();

        /**
         * Returns the lock which is held by given tx.
         *
         * @param txId Tx id.
         * @return The lock or null if no lock exist.
         */
        @Nullable Lock lock(UUID txId);

        /**
         * Returns lock type.
         *
         * @return The type.
         */
        boolean coarse();
    }

    public class CoarseLockState implements Releasable {
        private final ReentrantReadWriteLock[] stripes =
                new ReentrantReadWriteLock[Math.max(1, Runtime.getRuntime().availableProcessors() / 2)];

        {
            for (int i = 0; i < stripes.length; i++) {
                stripes[i] = new ReentrantReadWriteLock();
            }
        }

        private final ConcurrentHashMap<UUID, Lock> ixLocksOwners = new ConcurrentHashMap<>();
        private final Map<UUID, IgniteBiTuple<Lock, CompletableFuture<Lock>>> sLocksWaiters = new HashMap<>();
        private final ConcurrentHashMap<UUID, Lock> sLocksOwners = new ConcurrentHashMap<>();

        private final LockKey lockKey;

        CoarseLockState(LockKey lockKey) {
            this.lockKey = lockKey;
        }

        @Override
        public boolean tryRelease(UUID txId) {
            Lock lock = lock(txId);

            release(lock);

            return false;
        }

        @Override
        public LockKey key() {
            return lockKey;
        }

        @Override
        public Lock lock(UUID txId) {
            Lock lock = ixLocksOwners.get(txId);

            if (lock != null) {
                return lock;
            }

            int idx = Math.abs(txId.hashCode()) % stripes.length;

            stripes[idx].readLock().lock();

            try {
                return sLocksOwners.get(txId);
            } finally {
                stripes[idx].readLock().unlock();
            }
        }

        @Override
        public boolean coarse() {
            return true;
        }

        public CompletableFuture<Lock> acquire(UUID txId, LockKey lockKey, LockMode lockMode) {
            switch (lockMode) {
                case S:
                    for (ReentrantReadWriteLock lock : stripes) {
                        lock.writeLock().lock();
                    }

                    try {
                        if (!ixLocksOwners.isEmpty()) {
                            if (ixLocksOwners.containsKey(txId)) {
                                // IX-locks can't be modified under the striped lock.
                                if (ixLocksOwners.size() == 1) {
                                    // Safe to upgrade.
                                    track(txId, this); // Double track.
                                    Lock lock = new Lock(lockKey, lockMode, txId);
                                    sLocksOwners.putIfAbsent(txId, lock);
                                    return completedFuture(lock);
                                } else {
                                    // Attempt to upgrade to SIX in the presence of concurrent transactions. Deny lock attempt.
                                    for (Lock lock : ixLocksOwners.values()) {
                                        if (!lock.txId().equals(txId)) {
                                            return failedFuture(lockException(txId, lock.txId()));
                                        }
                                    }
                                }

                                assert false : "Should not reach here";
                            }

                            UUID holderTx = ixLocksOwners.keySet().iterator().next();
                            CompletableFuture<Void> res = fireEvent(LOCK_CONFLICT, new LockEventParameters(txId, holderTx));
                            if (res.isCompletedExceptionally()) {
                                return failedFuture(abandonedLockException(txId, holderTx));
                            }

                            track(txId, this);

                            CompletableFuture<Lock> fut = new CompletableFuture<>();
                            IgniteBiTuple<Lock, CompletableFuture<Lock>> prev = sLocksWaiters.putIfAbsent(txId,
                                    new IgniteBiTuple<>(new Lock(lockKey, lockMode, txId), fut));
                            return prev == null ? fut : prev.get2();
                        } else {
                            Lock lock = new Lock(lockKey, lockMode, txId);
                            Lock prev = sLocksOwners.putIfAbsent(txId, lock);

                            if (prev == null) {
                                track(txId, this); // Do not track on reenter.
                            }

                            return completedFuture(lock);
                        }
                    } finally {
                        for (ReentrantReadWriteLock lock : stripes) {
                            lock.writeLock().unlock();
                        }
                    }

                case IX:
                    int idx = Math.abs(txId.hashCode()) % stripes.length;

                    while(true) {
                        // This will prevent IX to block S, giving deadlock avoidance.
                        boolean locked = stripes[idx].readLock().tryLock();

                        // Striped lock is held for short time. Can spin wait here.
                        if (!locked) {
                            Thread.onSpinWait();
                        } else {
                            break;
                        }
                    }

                    try {
                        if (!sLocksOwners.isEmpty()) {
                            if (sLocksOwners.containsKey(txId)) {
                                // S-locks can't be modified under the striped lock.
                                if (sLocksOwners.size() == 1) {
                                    // Safe to upgrade.
                                    track(txId, this); // Double track.
                                    Lock lock = new Lock(lockKey, lockMode, txId);
                                    ixLocksOwners.putIfAbsent(txId, lock);
                                    return completedFuture(lock);
                                } else {
                                    // Attempt to upgrade to SIX in the presence of concurrent transactions. Deny lock attempt.
                                    for (Lock lock : sLocksOwners.values()) {
                                        if (!lock.txId().equals(txId)) {
                                            return failedFuture(lockException(txId, lock.txId()));
                                        }
                                    }
                                }

                                assert false : "Should not reach here";
                            }

                            UUID holderTx = sLocksOwners.keySet().iterator().next();
                            CompletableFuture<Void> eventResult = fireEvent(LOCK_CONFLICT, new LockEventParameters(txId, holderTx));
                            if (eventResult.isCompletedExceptionally()) {
                                return failedFuture(abandonedLockException(txId, holderTx));
                            } else {
                                return failedFuture(lockException(txId, holderTx));  // IX locks never wait.
                            }
                        } else {
                            Lock lock = new Lock(lockKey, lockMode, txId);
                            Lock prev = ixLocksOwners.putIfAbsent(txId, lock);// Avoid overwrite existing lock.

                            if (prev == null) {
                                track(txId, this); // Do not track on reenter.
                            }

                            return completedFuture(lock);
                        }
                    } finally {
                        stripes[idx].readLock().unlock();
                    }

                default:
                    assert false : "Unsupported coarse lock mode: " + lockMode;
            }

            return null; // Should not be here.
        }

        /**
         * Releases the lock. Should be called from {@link #releaseAll(UUID)}.
         *
         * @param lock The lock.
         */
        public void release(@Nullable Lock lock) {
            if (lock == null) {
                return;
            }

            switch (lock.lockMode()) {
                case S:
                    for (ReentrantReadWriteLock l : stripes) {
                        l.writeLock().lock();
                    }

                    try {
                        Lock removed = sLocksOwners.remove(lock.txId());

                        assert removed != null: "Attempt to release not acquired lock: " + lock.txId();
                    } finally {
                        for (ReentrantReadWriteLock l : stripes) {
                            l.writeLock().unlock();
                        }
                    }

                    break;
                case IX:
                    int idx = Math.abs(lock.txId().hashCode()) % stripes.length;

                    Map<UUID, IgniteBiTuple<Lock, CompletableFuture<Lock>>> wakeups;

                    stripes[idx].readLock().lock();

                    try {
                        var removed = ixLocksOwners.remove(lock.txId());

                        assert removed != null: "Attempt to release not acquired lock: " + lock.txId();

                        if (sLocksWaiters.isEmpty()) {
                            return; // Nothing to do.
                        }

                        if (!ixLocksOwners.isEmpty()) {
                            assert sLocksOwners.isEmpty() || sLocksOwners.containsKey(lock.txId());

                            return; // Nothing to do.
                        }

                        // No race here because no new locks can be acquired after releaseAll due to 2-phase locking protocol.

                        // Promote waiters to owners.
                        wakeups = new HashMap<>(sLocksWaiters);

                        sLocksWaiters.clear();

                        for (IgniteBiTuple<Lock, CompletableFuture<Lock>> value : wakeups.values()) {
                            sLocksOwners.put(value.getKey().txId(), value.getKey());
                        }
                    } finally {
                        stripes[idx].readLock().unlock();
                    }

                    for (Entry<UUID, IgniteBiTuple<Lock, CompletableFuture<Lock>>> entry : wakeups.entrySet()) {
                        entry.getValue().get2().complete(entry.getValue().get1());
                    }

                    break;
                default:
                    assert false : "Unsupported coarse unlock mode: " + lock.lockMode();
            }
        }
    }

    /**
     * A lock state.
     */
    public class LockState implements Releasable {
        /** Waiters. */
        private final TreeMap<UUID, WaiterImpl> waiters;

        /** Marked for removal flag. */
        private volatile boolean markedForRemove = false;

        /** Lock key. */
        private volatile LockKey key;

        LockState() {
            Comparator<UUID> txComparator =
                    deadlockPreventionPolicy.txIdComparator() != null ? deadlockPreventionPolicy.txIdComparator() : UUID::compareTo;

            this.waiters = new TreeMap<>(txComparator);
        }

        @Override
        public LockKey key() {
            return key;
        }

        @Override
        public Lock lock(UUID txId) {
            Waiter waiter = waiters.get(txId);

            if (waiter != null) {
                return new Lock(key, waiter.lockMode(), txId);
            }

            return null;
        }

        @Override
        public boolean coarse() {
            return false;
        }

        /**
         * Attempts to acquire a lock for the specified {@code key} in specified lock mode.
         *
         * @param txId Transaction id.
         * @param lockMode Lock mode.
         * @return The future or null if state is marked for removal and acquired lock mode.
         */
        @Nullable IgniteBiTuple<CompletableFuture<Void>, LockMode> tryAcquire(UUID txId, LockMode lockMode) {
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

                        return new IgniteBiTuple(nullCompletedFuture(), prev.lockMode());
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

                    // Put to wait queue, track.
                    if (prev == null) {
                        track(waiter.txId, this);
                    }

                    return new IgniteBiTuple<>(waiter.fut, waiter.lockMode());
                }

                if (!waiter.locked()) {
                    waiters.remove(waiter.txId());
                } else if (waiter.hasLockIntent()) {
                    waiter.refuseIntent(); // Restore old lock.
                } else {
                    // Lock granted, track.
                    if (prev == null) {
                        track(waiter.txId, this);
                    }
                }
            }

            // Notify outside the monitor.
            waiter.notifyLocked();

            return new IgniteBiTuple<>(waiter.fut, waiter.lockMode());
        }

        /**
         * Returns waiters count.
         *
         * @return waiters count.
         */
        public int waitersCount() {
            synchronized (waiters) {
                return waiters.size();
            }
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
                    if (conflictFound(waiter.txId(), tmp.txId())) {
                        waiter.fail(abandonedLockException(waiter.txId, tmp.txId));

                        return true;
                    } else if (!deadlockPreventionPolicy.usePriority() && deadlockPreventionPolicy.waitTimeout() == 0) {
                        waiter.fail(lockException(waiter.txId, tmp.txId));

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
                    } else if (conflictFound(waiter.txId(), tmp.txId())) {
                        waiter.fail(abandonedLockException(waiter.txId, tmp.txId));

                        return true;
                    } else if (deadlockPreventionPolicy.waitTimeout() == 0) {
                        waiter.fail(lockException(waiter.txId, tmp.txId));

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
         * Attempts to release a lock for the specified {@code key} in exclusive mode.
         *
         * @param txId Transaction id.
         * @return {@code True} if the queue is empty.
         */
        @Override
        public boolean tryRelease(UUID txId) {
            Collection<WaiterImpl> toNotify;

            synchronized (waiters) {
                toNotify = release(txId);
            }

            // Notify outside the monitor.
            for (WaiterImpl waiter : toNotify) {
                waiter.notifyLocked();
            }

            return key != null && waitersCount() == 0;
        }

        /**
         * Releases a specific lock of the key, if a key is locked in multiple modes by the same locker.
         *
         * @param txId Transaction id.
         * @param lockMode Lock mode.
         * @return If the value is true, no one waits of any lock of the key, false otherwise.
         */
        boolean tryRelease(UUID txId, LockMode lockMode) {
            List<WaiterImpl> toNotify = emptyList();
            synchronized (waiters) {
                WaiterImpl waiter = waiters.get(txId);

                if (waiter != null) {
                    assert LockMode.supremum(lockMode, waiter.lockMode()) == waiter.lockMode() :
                            "The lock is not locked in specified mode [mode=" + lockMode + ", locked=" + waiter.lockMode() + ']';

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

            return key != null && waitersCount() == 0;
        }

        /**
         * Releases all locks are held by a specific transaction. This method should be invoked synchronously.
         *
         * @param txId Transaction id.
         * @return List of waiters to notify.
         */
        private List<WaiterImpl> release(UUID txId) {
            waiters.remove(txId);

            if (waiters.isEmpty()) {
                return emptyList();
            }

            return unlockCompatibleWaiters();
        }

        /**
         * Unlock compatible waiters.
         *
         * @return List of waiters to notify.
         */
        private List<WaiterImpl> unlockCompatibleWaiters() {
            if (!deadlockPreventionPolicy.usePriority() && deadlockPreventionPolicy.waitTimeout() == 0) {
                return emptyList();
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

        /**
         * Notifies about the lock conflict found between transactions.
         *
         * @param acquirerTx Transaction which tries to acquire the lock.
         * @param holderTx Transaction which holds the lock.
         */
        private boolean conflictFound(UUID acquirerTx, UUID holderTx) {
            CompletableFuture<Void> eventResult = fireEvent(LOCK_CONFLICT, new LockEventParameters(acquirerTx, holderTx));
            // No async handling is expected.
            // TODO: https://issues.apache.org/jira/browse/IGNITE-21153
            assert eventResult.isDone() : "Async lock conflict handling is not supported";

            return eventResult.isCompletedExceptionally();
        }
    }

    /**
     * A waiter implementation.
     */
    private static class WaiterImpl implements Comparable<WaiterImpl>, Waiter {
        /**
         * Holding locks by type.
         */
        private final Map<LockMode, Integer> locks = new EnumMap<>(LockMode.class);

        /**
         * Lock modes are marked as intended, but have not taken yet. This is NOT specific to intention lock modes, such as IS and IX.
         */
        private final Set<LockMode> intendedLocks = EnumSet.noneOf(LockMode.class);

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
         * @return True if the lock is not locked in the passed mode, false otherwise.
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

                // TODO FIXME https://issues.apache.org/jira/browse/IGNITE-20985
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
            return S.toString(WaiterImpl.class, this, "granted", fut.isDone() && !fut.isCompletedExceptionally());
        }
    }

    private static int spread(int h) {
        return (h ^ (h >>> 16)) & 0x7fffffff;
    }

    @TestOnly
    public LockState[] getSlots() {
        return slots;
    }

    public int available() {
        return empty.size();
    }
}
