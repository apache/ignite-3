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
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.SystemPropertyView;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.AcquireLockTimeoutException;
import org.apache.ignite.internal.tx.DeadlockPreventionPolicy;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.LockTableOverflowException;
import org.apache.ignite.internal.tx.PossibleDeadlockOnLockAcquireException;
import org.apache.ignite.internal.tx.Waiter;
import org.apache.ignite.internal.tx.event.LockEvent;
import org.apache.ignite.internal.tx.event.LockEventParameters;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.IgniteStripedReadWriteLock;
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
    /** Table size. */
    public static final int DEFAULT_SLOTS = 1_048_576;

    public static final String LOCK_MAP_SIZE_PROPERTY_NAME = "lockMapSize";

    /** Striped lock concurrency. */
    private static final int CONCURRENCY = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);

    private final LongAdder lockTableSize = new LongAdder();

    /** An unused state to avoid concurrent allocation. */
    private LockState removedLockState;

    /** Lock map size. */
    private final int lockMapSize;

    /** Mapped slots. */
    private ConcurrentHashMap<LockKey, LockState> locks;

    /** The policy. */
    private DeadlockPreventionPolicy deadlockPreventionPolicy;

    /** Executor that is used to fail waiters after timeout. */
    private Executor delayedExecutor;

    /** Enlisted transactions. */
    private final ConcurrentHashMap<UUID, ConcurrentLinkedQueue<Releasable>> txMap = new ConcurrentHashMap<>(1024);

    /** Coarse locks. */
    private final ConcurrentHashMap<Object, CoarseLockState> coarseMap = new ConcurrentHashMap<>();

    /** Tx state required to present tx labels in logs and exceptions. */
    private final VolatileTxStateMetaStorage txStateVolatileStorage;

    /**
     * Creates an instance of {@link HeapLockManager} with a few slots eligible for tests which don't stress the lock manager too much.
     * Such a small instance is started way faster than a full-blown production ready instance with a lot of slots.
     */
    @TestOnly
    public static HeapLockManager smallInstance() {
        VolatileTxStateMetaStorage storage = new VolatileTxStateMetaStorage();
        storage.start();
        return new HeapLockManager(1024, storage);
    }

    /** Constructor. */
    public HeapLockManager(SystemLocalConfiguration systemProperties, VolatileTxStateMetaStorage txStateVolatileStorage) {
        this(intProperty(systemProperties, LOCK_MAP_SIZE_PROPERTY_NAME, DEFAULT_SLOTS), txStateVolatileStorage);
    }

    /**
     * Constructor.
     *
     * @param lockMapSize Lock map size.
     */
    public HeapLockManager(int lockMapSize, VolatileTxStateMetaStorage txStateVolatileStorage) {
        this.lockMapSize = lockMapSize;
        this.txStateVolatileStorage = txStateVolatileStorage;
    }

    private static int intProperty(SystemLocalConfiguration systemProperties, String name, int defaultValue) {
        SystemPropertyView property = systemProperties.properties().value().get(name);

        return property == null ? defaultValue : Integer.parseInt(property.propertyValue());
    }

    @Override
    public void start(DeadlockPreventionPolicy deadlockPreventionPolicy) {
        this.deadlockPreventionPolicy = deadlockPreventionPolicy;
        this.removedLockState = new LockState();

        this.delayedExecutor = deadlockPreventionPolicy.waitTimeout() > 0
                ? CompletableFuture.delayedExecutor(deadlockPreventionPolicy.waitTimeout(), TimeUnit.MILLISECONDS)
                : null;

        locks = new ConcurrentHashMap<>(lockMapSize);
    }

    @Override
    public CompletableFuture<Lock> acquire(UUID txId, LockKey lockKey, LockMode lockMode) {
        assert lockMode != null : "Lock mode is null";

        if (lockKey.contextId() == null) { // Treat this lock as a hierarchy(coarse) lock.
            CoarseLockState state = coarseMap.computeIfAbsent(lockKey, key -> new CoarseLockState(lockKey));

            return state.acquire(txId, lockMode);
        }

        while (true) {
            LockState state = acquireLockState(lockKey);

            if (state == null) {
                return failedFuture(new LockTableOverflowException(txId, lockMapSize, txStateVolatileStorage));
            }

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
        assert lockMode != null : "Lock mode is null";

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
    public Iterator<Lock> locks() {
        return txMap.entrySet().stream()
                .flatMap(e -> collectLocksFromStates(e.getKey(), e.getValue()).stream())
                .iterator();
    }

    @Override
    public Iterator<Lock> locks(UUID txId) {
        ConcurrentLinkedQueue<Releasable> lockStates = txMap.get(txId);

        return collectLocksFromStates(txId, lockStates).iterator();
    }

    /**
     * Gets a lock state. Use this method where sure this lock state was already acquired.
     *
     * @param key A lock key.
     * @return A state matched with the key or unused state.
     */
    private LockState lockState(LockKey key) {
        return locks.getOrDefault(key, removedLockState);
    }

    /**
     * Returns the lock state for the key.
     *
     * @param key The key.
     * @return A state matched with the key.
     */
    private @Nullable LockState acquireLockState(LockKey key) {
        return locks.computeIfAbsent(key, (k) -> {
            int acquiredLocks = lockTableSize.intValue();

            if (acquiredLocks < lockMapSize) {
                lockTableSize.increment();

                LockState v = new LockState();
                v.key = k;

                return v;
            } else {
                return null;
            }

        });
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
        if (lockTableSize.sum() != 0) {
            return false;
        }

        for (CoarseLockState value : coarseMap.values()) {
            if (!value.slockOwners.isEmpty()) {
                return false;
            }

            if (!value.ixlockOwners.isEmpty()) {
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
                v.key = null;

                lockTableSize.decrement();

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

    private static List<Lock> collectLocksFromStates(UUID txId, ConcurrentLinkedQueue<Releasable> lockStates) {
        List<Lock> result = new ArrayList<>();

        if (lockStates != null) {
            for (Releasable lockState : lockStates) {
                Lock lock = lockState.lock(txId);
                if (lock != null && lock.lockMode() != null) {
                    result.add(lock);
                }
            }
        }

        return result;
    }

    /**
     * Common interface for releasing transaction locks.
     */
    interface Releasable {
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
         * Returns the lock which is requested by given tx.
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

    /**
     * Coarse lock.
     */
    public class CoarseLockState implements Releasable {
        private final IgniteStripedReadWriteLock stripedLock = new IgniteStripedReadWriteLock(CONCURRENCY);
        private final ConcurrentHashMap<UUID, Lock> ixlockOwners = new ConcurrentHashMap<>();
        private final Map<UUID, IgniteBiTuple<Lock, CompletableFuture<Lock>>> slockWaiters = new HashMap<>();
        private final ConcurrentHashMap<UUID, Lock> slockOwners = new ConcurrentHashMap<>();
        private final LockKey lockKey;
        private final Comparator<UUID> txComparator;

        CoarseLockState(LockKey lockKey) {
            this.lockKey = lockKey;
            txComparator =
                    deadlockPreventionPolicy.txIdComparator() != null ? deadlockPreventionPolicy.txIdComparator() : UUID::compareTo;
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
            Lock lock = ixlockOwners.get(txId);

            if (lock != null) {
                return lock;
            }

            int idx = Math.floorMod(spread(txId.hashCode()), CONCURRENCY);

            stripedLock.readLock(idx).lock();

            try {
                lock = slockOwners.get(txId);

                if (lock != null) {
                    return lock;
                }

                IgniteBiTuple<Lock, CompletableFuture<Lock>> tuple = slockWaiters.get(txId);

                if (tuple != null) {
                    return tuple.get1();
                }
            } finally {
                stripedLock.readLock(idx).unlock();
            }

            return null;
        }

        @Override
        public boolean coarse() {
            return true;
        }

        /**
         * Acquires a lock.
         *
         * @param txId Tx id.
         * @param lockMode Lock mode.
         * @return The future.
         */
        public CompletableFuture<Lock> acquire(UUID txId, LockMode lockMode) {
            switch (lockMode) {
                case S:
                    stripedLock.writeLock().lock();

                    try {
                        // IX-locks can't be modified under the striped write lock.
                        if (!ixlockOwners.isEmpty()) {
                            if (ixlockOwners.containsKey(txId)) {
                                if (ixlockOwners.size() == 1) {
                                    // Safe to upgrade.
                                    track(txId, this); // Double track.
                                    Lock lock = new Lock(lockKey, lockMode, txId);
                                    slockOwners.putIfAbsent(txId, lock);
                                    return completedFuture(lock);
                                } else {
                                    // Attempt to upgrade to SIX in the presence of concurrent transactions. Deny lock attempt.
                                    for (Lock lock : ixlockOwners.values()) {
                                        if (!lock.txId().equals(txId)) {
                                            return notifyAndFail(txId, lock.txId(), lockMode, lock.lockMode());
                                        }
                                    }
                                }

                                assert false : "Should not reach here";
                            }

                            // Validate reordering with IX locks if prevention is enabled.
                            if (deadlockPreventionPolicy.usePriority()) {
                                for (Lock lock : ixlockOwners.values()) {
                                    // Allow only high priority transactions to wait.
                                    if (txComparator.compare(lock.txId(), txId) < 0) {
                                        return notifyAndFail(txId, lock.txId(), lockMode, lock.lockMode());
                                    }
                                }
                            }

                            track(txId, this);

                            CompletableFuture<Lock> fut = new CompletableFuture<>();
                            IgniteBiTuple<Lock, CompletableFuture<Lock>> prev = slockWaiters.putIfAbsent(txId,
                                    new IgniteBiTuple<>(new Lock(lockKey, lockMode, txId), fut));
                            return prev == null ? fut : prev.get2();
                        } else {
                            Lock lock = new Lock(lockKey, lockMode, txId);
                            Lock prev = slockOwners.putIfAbsent(txId, lock);

                            if (prev == null) {
                                track(txId, this); // Do not track on reenter.
                            }

                            return completedFuture(lock);
                        }
                    } finally {
                        stripedLock.writeLock().unlock();
                    }

                case IX:
                    int idx = Math.floorMod(spread(txId.hashCode()), CONCURRENCY);

                    stripedLock.readLock(idx).lock();

                    try {
                        // S-locks can't be modified under the striped read lock.
                        if (!slockOwners.isEmpty()) {
                            if (slockOwners.containsKey(txId)) {
                                if (slockOwners.size() == 1) {
                                    // Safe to upgrade.
                                    track(txId, this); // Double track.
                                    Lock lock = new Lock(lockKey, lockMode, txId);
                                    ixlockOwners.putIfAbsent(txId, lock);
                                    return completedFuture(lock);
                                } else {
                                    // Attempt to upgrade to SIX in the presence of concurrent transactions. Deny lock attempt.
                                    for (Lock lock : slockOwners.values()) {
                                        if (!lock.txId().equals(txId)) {
                                            return notifyAndFail(txId, lock.txId(), lockMode, lock.lockMode());
                                        }
                                    }
                                }

                                assert false : "Should not reach here";
                            }

                            // IX locks never allowed to wait.
                            Entry<UUID, Lock> holderEntry = slockOwners.entrySet().iterator().next();
                            return notifyAndFail(txId, holderEntry.getKey(), lockMode, holderEntry.getValue().lockMode());
                        } else {
                            Lock lock = new Lock(lockKey, lockMode, txId);
                            Lock prev = ixlockOwners.putIfAbsent(txId, lock); // Avoid overwrite existing lock.

                            if (prev == null) {
                                track(txId, this); // Do not track on reenter.
                            }

                            return completedFuture(lock);
                        }
                    } finally {
                        stripedLock.readLock(idx).unlock();
                    }

                default:
                    assert false : "Unsupported coarse lock mode: " + lockMode;

                    return null; // Should not be here.
            }
        }

        private Set<UUID> allLockHolderTxs() {
            return CollectionUtils.union(ixlockOwners.keySet(), slockOwners.keySet());
        }

        /**
         * Triggers event and fails.
         *
         * @param failedToAcquireLockTxId UUID of a transaction that tried to acquire lock, but failed.
         * @param currentLockHolderTxId UUID of a transaction that currently holds the lock.
         * @param attemptedLockModeToAcquireWith {@link LockMode} that was tried to acquire the lock with but failed the attempt.
         * @param currentlyAcquiredLockMode {@link LockMode} of the lock that is already acquired with.
         * @return Failed future.
         */
        CompletableFuture<Lock> notifyAndFail(
                UUID failedToAcquireLockTxId,
                UUID currentLockHolderTxId,
                LockMode attemptedLockModeToAcquireWith,
                LockMode currentlyAcquiredLockMode
        ) {
            CompletableFuture<Lock> failedFuture = new CompletableFuture<>();

            fireEvent(LOCK_CONFLICT, new LockEventParameters(failedToAcquireLockTxId, allLockHolderTxs())).whenComplete((v, ex) -> {
                boolean abandonedLock = ex != null;
                failedFuture.completeExceptionally(new PossibleDeadlockOnLockAcquireException(
                        failedToAcquireLockTxId,
                        currentLockHolderTxId,
                        attemptedLockModeToAcquireWith,
                        currentlyAcquiredLockMode,
                        abandonedLock,
                        txStateVolatileStorage
                ));
            });

            // TODO: https://issues.apache.org/jira/browse/IGNITE-21153
            return failedFuture;
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
                    IgniteBiTuple<Lock, CompletableFuture<Lock>> waiter = null;

                    stripedLock.writeLock().lock();

                    try {
                        Lock removed = slockOwners.remove(lock.txId());

                        if (removed == null) {
                            waiter = slockWaiters.remove(lock.txId());

                            if (waiter != null) {
                                removed = waiter.get1();
                            }
                        }

                        assert removed != null : "Attempt to release not requested lock: " + lock.txId();
                    } finally {
                        stripedLock.writeLock().unlock();
                    }

                    if (waiter != null) {
                        waiter.get2().complete(waiter.get1());
                    }

                    break;
                case IX:
                    int idx = Math.floorMod(spread(lock.txId().hashCode()), CONCURRENCY);

                    Map<UUID, IgniteBiTuple<Lock, CompletableFuture<Lock>>> wakeups;

                    stripedLock.readLock(idx).lock();

                    try {
                        var removed = ixlockOwners.remove(lock.txId());

                        assert removed != null : "Attempt to release not acquired lock: " + lock.txId();

                        if (slockWaiters.isEmpty()) {
                            return; // Nothing to do.
                        }

                        if (!ixlockOwners.isEmpty()) {
                            assert slockOwners.isEmpty() || slockOwners.containsKey(lock.txId());

                            return; // Nothing to do.
                        }

                        // No race here because no new locks can be acquired after releaseAll due to 2-phase locking protocol.

                        // Promote waiters to owners.
                        wakeups = new HashMap<>(slockWaiters);

                        slockWaiters.clear();

                        for (IgniteBiTuple<Lock, CompletableFuture<Lock>> value : wakeups.values()) {
                            slockOwners.put(value.getKey().txId(), value.getKey());
                        }
                    } finally {
                        stripedLock.readLock(idx).unlock();
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
     * Key lock.
     */
    public class LockState implements Releasable {
        /** Waiters. */
        private final TreeMap<UUID, WaiterImpl> waiters;

        /** Lock key. */
        private volatile LockKey key;

        LockState() {
            Comparator<UUID> txComparator =
                    deadlockPreventionPolicy.txIdComparator() != null ? deadlockPreventionPolicy.txIdComparator() : UUID::compareTo;

            this.waiters = new TreeMap<>(txComparator);
        }

        /**
         * Checks if lock state is used to hold a lock.
         *
         * @return True if the state is used, otherwise false.
         */
        boolean isUsed() {
            return key != null;
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
        IgniteBiTuple<CompletableFuture<Void>, LockMode> tryAcquire(UUID txId, LockMode lockMode) {
            assert lockMode != null : "Lock mode is null";

            WaiterImpl waiter = new WaiterImpl(txId, lockMode);

            synchronized (waiters) {
                if (!isUsed()) {
                    return new IgniteBiTuple<>(null, lockMode);
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

                        return new IgniteBiTuple<>(nullCompletedFuture(), prev.lockMode());
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
            LockMode intendedLockMode = waiter.intendedLockMode();

            assert intendedLockMode != null : "Intended lock mode is null";

            for (Entry<UUID, WaiterImpl> entry : waiters.tailMap(waiter.txId(), false).entrySet()) {
                WaiterImpl tmp = entry.getValue();
                LockMode currentlyAcquiredLockMode = tmp.lockMode;

                if (currentlyAcquiredLockMode != null && !currentlyAcquiredLockMode.isCompatible(intendedLockMode)) {
                    if (conflictFound(waiter.txId())) {
                        // We treat the current lock as the abandoned one.
                        waiter.fail(new PossibleDeadlockOnLockAcquireException(
                                waiter.txId,
                                tmp.txId,
                                intendedLockMode,
                                currentlyAcquiredLockMode,
                                true,
                                txStateVolatileStorage
                        ));

                        return true;
                    } else if (!deadlockPreventionPolicy.usePriority() && deadlockPreventionPolicy.waitTimeout() == 0) {
                        waiter.fail(new PossibleDeadlockOnLockAcquireException(
                                waiter.txId,
                                tmp.txId,
                                intendedLockMode,
                                currentlyAcquiredLockMode,
                                false,
                                txStateVolatileStorage
                        ));

                        return true;
                    }

                    return false;
                }
            }

            for (Entry<UUID, WaiterImpl> entry : waiters.headMap(waiter.txId()).entrySet()) {
                WaiterImpl tmp = entry.getValue();
                LockMode currentlyAcquiredLockMode = tmp.lockMode;

                if (currentlyAcquiredLockMode != null && !currentlyAcquiredLockMode.isCompatible(intendedLockMode)) {
                    if (skipFail) {
                        return false;
                    } else if (conflictFound(waiter.txId())) {
                        // We treat the current lock as the abandoned one.
                        waiter.fail(new PossibleDeadlockOnLockAcquireException(
                                waiter.txId,
                                tmp.txId,
                                intendedLockMode,
                                currentlyAcquiredLockMode,
                                true,
                                txStateVolatileStorage
                        ));
                        return true;
                    } else if (deadlockPreventionPolicy.waitTimeout() == 0) {
                        waiter.fail(new PossibleDeadlockOnLockAcquireException(
                                waiter.txId,
                                tmp.txId,
                                intendedLockMode,
                                currentlyAcquiredLockMode,
                                false,
                                txStateVolatileStorage
                        ));

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
            assert lockMode != null : "Lock mode is null";

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

            for (Entry<UUID, WaiterImpl> entry : waiters.entrySet()) {
                WaiterImpl tmp = entry.getValue();

                if (tmp.hasLockIntent() && isWaiterReadyToNotify(tmp, true)) {
                    assert !tmp.hasLockIntent() : "This waiter in not locked for notification [waiter=" + tmp + ']';

                    toNotify.add(tmp);
                }
            }

            if (deadlockPreventionPolicy.usePriority() && deadlockPreventionPolicy.waitTimeout() >= 0) {
                for (Entry<UUID, WaiterImpl> entry : waiters.entrySet()) {
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
                    waiter.fut.completeExceptionally(
                            new AcquireLockTimeoutException(waiter, deadlockPreventionPolicy.waitTimeout(), txStateVolatileStorage));
                }
            });
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
         * @return True if the conflict connected with an abandoned transaction, false in the other case.
         */
        private boolean conflictFound(UUID acquirerTx) {
            CompletableFuture<Void> eventResult = fireEvent(LOCK_CONFLICT, new LockEventParameters(acquirerTx, allLockHolderTxs()));
            // No async handling is expected.
            // TODO: https://issues.apache.org/jira/browse/IGNITE-21153
            assert eventResult.isDone() : "Async lock conflict handling is not supported";

            return eventResult.isCompletedExceptionally();
        }

        private Set<UUID> allLockHolderTxs() {
            return waiters.keySet();
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
        private @Nullable LockMode intendedLockMode;

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
                assert mode != null : "Lock mode is null";
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

            other.locks.forEach(this::addLock);

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
                assert lockMode != null : "Lock mode is null";

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
        boolean hasLockIntent() {
            return this.intendedLockMode != null;
        }

        /** {@inheritDoc} */
        @Override
        public LockMode lockMode() {
            return lockMode;
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable LockMode intendedLockMode() {
            return intendedLockMode;
        }

        /** Grant a lock. */
        private void lock() {
            assert intendedLockMode != null : "Intended lock mode is null";

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
            return o instanceof WaiterImpl && compareTo((WaiterImpl) o) == 0;
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
        return locks.values().toArray(new LockState[]{});
    }

    public int available() {
        return Math.max(lockMapSize - lockTableSize.intValue(), 0);
    }
}
