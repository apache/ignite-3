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

package org.apache.ignite.internal.tx;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.tx.LockMode.IS;
import static org.apache.ignite.internal.tx.LockMode.IX;
import static org.apache.ignite.internal.tx.LockMode.S;
import static org.apache.ignite.internal.tx.LockMode.SIX;
import static org.apache.ignite.internal.tx.LockMode.X;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests a LockManager implementation.
 */
@ExtendWith(ConfigurationExtension.class)
public abstract class AbstractLockManagerTest extends IgniteAbstractTest {
    @InjectConfiguration
    private SystemLocalConfiguration systemLocalConfiguration;

    protected LockManager lockManager;

    @BeforeEach
    public void before() {
        lockManager = newInstance(systemLocalConfiguration);
    }

    protected abstract LockManager newInstance(SystemLocalConfiguration systemLocalConfiguration);

    protected abstract LockKey lockKey();

    @Test
    public void testSingleKeyWrite() {
        UUID txId1 = TestTransactionIds.newTransactionId();

        LockKey key = lockKey();

        CompletableFuture<Lock> fut0 = lockManager.acquire(txId1, key, X);

        assertTrue(fut0.isDone());

        Collection<UUID> queue = lockManager.queue(key);

        assertTrue(queue.size() == 1 && queue.iterator().next().equals(txId1));

        Waiter waiter = lockManager.waiter(key, txId1);

        assertTrue(waiter.locked());

        lockManager.release(fut0.join());
    }

    @Test
    public void testSingleKeyWriteLock() {
        UUID txId1 = TestTransactionIds.newTransactionId();
        UUID txId2 = TestTransactionIds.newTransactionId();

        LockKey key = lockKey();

        CompletableFuture<Lock> fut0 = lockManager.acquire(txId2, key, X);

        assertTrue(fut0.isDone());

        assertTrue(txId2.compareTo(txId1) > 0);

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, key, X);

        assertFalse(fut1.isDone());

        assertTrue(lockManager.waiter(key, txId2).locked());
        assertFalse(lockManager.waiter(key, txId1).locked());

        lockManager.release(fut0.join());

        assertTrue(fut1.isDone());

        assertNull(lockManager.waiter(key, txId2));
        assertTrue(lockManager.waiter(key, txId1).locked());

        lockManager.release(fut1.join());

        assertNull(lockManager.waiter(key, txId2));
        assertNull(lockManager.waiter(key, txId1));
    }

    @Test
    public void downgradeLockOutOfTurnTest() {
        UUID txId0 = TestTransactionIds.newTransactionId();
        UUID txId1 = TestTransactionIds.newTransactionId();
        UUID txId2 = TestTransactionIds.newTransactionId();

        LockKey key = lockKey();

        lockManager.acquire(txId0, key, S).join();
        Lock lock = lockManager.acquire(txId2, key, S).join();

        CompletableFuture<Lock> fut0 = lockManager.acquire(txId0, key, X);
        assertFalse(fut0.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId2, key, X);
        expectConflict(fut2);

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, key, S);
        fut1.join();

        assertFalse(fut0.isDone());

        lockManager.release(lock);
        fut0.thenAccept(l -> lockManager.release(l));
    }

    @Test
    public void upgradeLockImmediatelyTest() {
        UUID txId0 = TestTransactionIds.newTransactionId();
        UUID txId1 = TestTransactionIds.newTransactionId();
        UUID txId2 = TestTransactionIds.newTransactionId();

        LockKey key = lockKey();

        CompletableFuture<Lock> fut = lockManager.acquire(txId0, key, IS);
        assertTrue(fut.isDone());

        CompletableFuture<Lock> fut0 = lockManager.acquire(txId1, key, IS);
        assertTrue(fut0.isDone());

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId2, key, IS);
        assertTrue(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId1, key, IX);
        assertTrue(fut2.isDone());

        lockManager.release(fut1.join());
    }

    @Test
    public void testSingleKeyReadWriteLock() {
        UUID txId0 = TestTransactionIds.newTransactionId();
        UUID txId1 = TestTransactionIds.newTransactionId();
        UUID txId2 = TestTransactionIds.newTransactionId();
        UUID txId3 = TestTransactionIds.newTransactionId();
        assertTrue(txId3.compareTo(txId2) > 0);
        assertTrue(txId2.compareTo(txId1) > 0);
        assertTrue(txId1.compareTo(txId0) > 0);
        LockKey key = lockKey();

        CompletableFuture<Lock> fut3 = lockManager.acquire(txId3, key, S);
        assertTrue(fut3.isDone());

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, key, S);
        assertTrue(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId2, key, S);
        assertTrue(fut2.isDone());

        CompletableFuture<Lock> fut0 = lockManager.acquire(txId0, key, X);
        assertFalse(fut0.isDone());

        assertTrue(lockManager.waiter(key, txId3).locked());
        assertTrue(lockManager.waiter(key, txId2).locked());
        assertTrue(lockManager.waiter(key, txId1).locked());
        assertFalse(lockManager.waiter(key, txId0).locked());

        lockManager.release(fut1.join());

        assertTrue(lockManager.waiter(key, txId3).locked());
        assertTrue(lockManager.waiter(key, txId2).locked());
        assertNull(lockManager.waiter(key, txId1));
        assertFalse(lockManager.waiter(key, txId0).locked());

        lockManager.release(fut3.join());

        assertNull(lockManager.waiter(key, txId3));
        assertTrue(lockManager.waiter(key, txId2).locked());
        assertNull(lockManager.waiter(key, txId1));
        assertFalse(lockManager.waiter(key, txId0).locked());

        lockManager.release(fut2.join());

        assertNull(lockManager.waiter(key, txId3));
        assertNull(lockManager.waiter(key, txId2));
        assertNull(lockManager.waiter(key, txId1));
        assertTrue(lockManager.waiter(key, txId0).locked());
    }

    @Test
    public void testSingleKeyReadWriteConflict() {
        UUID txId0 = TestTransactionIds.newTransactionId();
        UUID txId1 = TestTransactionIds.newTransactionId();
        LockKey key = lockKey();

        // Lock in order
        CompletableFuture<Lock> fut0 = lockManager.acquire(txId1, key, S);
        assertTrue(fut0.isDone());

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId0, key, X);
        assertFalse(fut1.isDone());

        lockManager.release(fut0.join());
        assertTrue(fut1.isDone());

        lockManager.release(fut1.join());

        assertTrue(lockManager.queue(key).isEmpty());

        // Lock not in order
        fut0 = lockManager.acquire(txId0, key, S);
        assertTrue(fut0.isDone());

        try {
            lockManager.acquire(txId1, key, X).join();

            fail();
        } catch (CompletionException e) {
            // Expected.
        }
    }

    @Test
    public void testSingleKeyReadWriteConflict2() {
        UUID[] txId = generate(3);
        LockKey key = lockKey();

        // Lock in order
        CompletableFuture<Lock> fut0 = lockManager.acquire(txId[1], key, S);
        assertTrue(fut0.isDone());

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId[0], key, X);
        assertFalse(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId[2], key, S);
        assertTrue(fut2.isDone());

        lockManager.release(fut0.join());
        lockManager.release(fut2.join());

        assertTrue(fut1.isDone());
    }

    @Test
    public void testSingleKeyReadWriteConflict3() {
        UUID txId0 = TestTransactionIds.newTransactionId();
        UUID txId1 = TestTransactionIds.newTransactionId();
        UUID txId2 = TestTransactionIds.newTransactionId();
        LockKey key = lockKey();

        // Lock in order
        CompletableFuture<Lock> fut0 = lockManager.acquire(txId1, key, S);
        assertTrue(fut0.isDone());

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId0, key, X);
        assertFalse(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId2, key, S);
        assertTrue(fut2.isDone());

        assertFalse(lockManager.waiter(key, txId0).locked());

        lockManager.release(fut2.join());
        lockManager.release(fut0.join());

        assertTrue(fut1.isDone());
    }

    @Test
    public void testSingleKeyReadWriteConflict4() {
        UUID txId1 = TestTransactionIds.newTransactionId();
        final UUID txId2 = TestTransactionIds.newTransactionId();
        UUID txId3 = TestTransactionIds.newTransactionId();
        UUID txId4 = TestTransactionIds.newTransactionId();
        LockKey key = lockKey();

        CompletableFuture<Lock> fut4 = lockManager.acquire(txId4, key, S);
        assertTrue(fut4.isDone());

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, key, X);
        assertFalse(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId3, key, X);
        assertFalse(fut2.isDone());

        CompletableFuture<Lock> fut3 = lockManager.acquire(txId2, key, X);
        assertFalse(fut3.isDone());
    }

    @Test
    public void testSingleKeyReadWriteConflict5() {
        UUID txId0 = TestTransactionIds.newTransactionId();
        UUID txId1 = TestTransactionIds.newTransactionId();
        LockKey key = lockKey();

        lockManager.acquire(txId0, key, X).join();

        expectConflict(lockManager.acquire(txId1, key, X));
    }

    @Test
    public void testConflicts() {
        UUID txId0 = TestTransactionIds.newTransactionId();
        UUID txId1 = TestTransactionIds.newTransactionId();

        LockKey key = lockKey();

        List<IgniteBiTuple<LockMode, LockMode>> lockModes = new ArrayList<>();

        lockModes.add(new IgniteBiTuple<>(IS, X));
        lockModes.add(new IgniteBiTuple<>(IX, X));
        lockModes.add(new IgniteBiTuple<>(S, X));
        lockModes.add(new IgniteBiTuple<>(SIX, X));
        lockModes.add(new IgniteBiTuple<>(X, X));

        lockModes.add(new IgniteBiTuple<>(IX, SIX));
        lockModes.add(new IgniteBiTuple<>(S, SIX));
        lockModes.add(new IgniteBiTuple<>(SIX, SIX));
        lockModes.add(new IgniteBiTuple<>(X, SIX));

        lockModes.add(new IgniteBiTuple<>(IX, S));
        lockModes.add(new IgniteBiTuple<>(SIX, S));
        lockModes.add(new IgniteBiTuple<>(X, S));

        lockModes.add(new IgniteBiTuple<>(S, IX));
        lockModes.add(new IgniteBiTuple<>(SIX, IX));
        lockModes.add(new IgniteBiTuple<>(X, IX));

        lockModes.add(new IgniteBiTuple<>(X, IS));

        for (IgniteBiTuple<LockMode, LockMode> lockModePair : lockModes) {
            CompletableFuture<Lock> fut0 = lockManager.acquire(txId0, key, lockModePair.get2());
            CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, key, lockModePair.get1());

            assertTrue(fut0.isDone());
            expectConflict(fut1);

            lockManager.release(fut0.join());

            assertTrue(lockManager.queue(key).isEmpty());
        }
    }

    @Test
    public void testSingleKeyWriteWriteConflict() {
        UUID txId0 = TestTransactionIds.newTransactionId();
        UUID txId1 = TestTransactionIds.newTransactionId();
        UUID txId2 = TestTransactionIds.newTransactionId();
        LockKey key = lockKey();

        // Lock in order
        CompletableFuture<Lock> fut0 = lockManager.acquire(txId1, key, X);
        assertTrue(fut0.isDone());

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId0, key, X);
        assertFalse(fut1.isDone());

        try {
            lockManager.acquire(txId2, key, X).join();

            fail();
        } catch (CompletionException e) {
            // Expected.
        }
    }

    @Test
    public void testSingleKeyWriteWriteConflict2() {
        UUID txId0 = TestTransactionIds.newTransactionId();
        UUID txId1 = TestTransactionIds.newTransactionId();
        UUID txId2 = TestTransactionIds.newTransactionId();
        LockKey key = lockKey();

        // Lock in order
        CompletableFuture<Lock> fut2 = lockManager.acquire(txId2, key, X);
        assertTrue(fut2.isDone());

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, key, X);
        assertFalse(fut1.isDone());

        CompletableFuture<Lock> fut0 = lockManager.acquire(txId0, key, X);
        assertFalse(fut0.isDone());
    }

    @Test
    public void testSingleKeyMultithreadedRead() throws InterruptedException {
        LongAdder readLocks = new LongAdder();
        LongAdder writeLocks = new LongAdder();
        LongAdder failedLocks = new LongAdder();

        doTestSingleKeyMultithreaded(5_000, readLocks, writeLocks, failedLocks, 0);

        assertEquals(0, writeLocks.sum());
        assertEquals(0, failedLocks.sum());
    }

    @Test
    public void testSingleKeyMultithreadedWrite() throws InterruptedException {
        LongAdder readLocks = new LongAdder();
        LongAdder writeLocks = new LongAdder();
        LongAdder failedLocks = new LongAdder();

        doTestSingleKeyMultithreaded(5_000, readLocks, writeLocks, failedLocks, 1);

        assertEquals(0, readLocks.sum());
    }

    @Test
    public void testSingleKeyMultithreadedRandom() throws InterruptedException {
        LongAdder readLocks = new LongAdder();
        LongAdder writeLocks = new LongAdder();
        LongAdder failedLocks = new LongAdder();

        doTestSingleKeyMultithreaded(5_000, readLocks, writeLocks, failedLocks, 2);
    }

    @Test
    public void testLockUpgrade() {
        UUID txId0 = TestTransactionIds.newTransactionId();
        UUID txId1 = TestTransactionIds.newTransactionId();
        LockKey key = lockKey();

        lockManager.acquire(txId0, key, S).join();

        Lock lock = lockManager.acquire(txId1, key, S).join();

        CompletableFuture<Lock> fut = lockManager.acquire(txId0, key, X);
        assertFalse(fut.isDone());

        lockManager.release(lock);

        fut.join();

        lockManager.release(fut.join());

        assertTrue(lockManager.queue(key).isEmpty());
    }

    @Test
    public void testLockUpgrade2() {
        UUID txId0 = TestTransactionIds.newTransactionId();
        UUID txId1 = TestTransactionIds.newTransactionId();
        LockKey key = lockKey();

        lockManager.acquire(txId0, key, S).join();

        lockManager.acquire(txId1, key, S).join();

        expectConflict(lockManager.acquire(txId1, key, X));
    }

    @Test
    public void testLockUpgrade3() {
        UUID txId0 = TestTransactionIds.newTransactionId();
        UUID txId1 = TestTransactionIds.newTransactionId();
        UUID txId2 = TestTransactionIds.newTransactionId();
        LockKey key = lockKey();

        lockManager.acquire(txId1, key, S).join();

        lockManager.acquire(txId0, key, S).join();

        Lock lock2 = lockManager.acquire(txId2, key, S).join();

        CompletableFuture<?> fut1 = lockManager.acquire(txId1, key, X);

        assertFalse(fut1.isDone());

        lockManager.release(lock2);
        assertTrue(fut1.isDone());
        assertTrue(fut1.isCompletedExceptionally());
    }

    @Test
    public void testLockUpgrade4() {
        UUID txId0 = TestTransactionIds.newTransactionId();
        UUID txId1 = TestTransactionIds.newTransactionId();
        LockKey key = lockKey();

        lockManager.acquire(txId0, key, S).join();

        Lock lock = lockManager.acquire(txId1, key, S).join();

        CompletableFuture<Lock> fut = lockManager.acquire(txId0, key, X);

        assertFalse(fut.isDone());

        lockManager.release(lock);

        fut.join();

        assertThat(lockManager.queue(key), hasSize(1));
    }

    @Test
    public void testLockUpgrade5() {
        UUID txId0 = TestTransactionIds.newTransactionId();

        LockKey key = lockKey();

        for (LockMode lockMode : List.of(IS, IX, SIX, X)) {
            lockManager.acquire(txId0, key, lockMode).join();

            assertEquals(lockMode, lockManager.waiter(key, txId0).lockMode());
        }

        assertThat(lockManager.queue(key), hasSize(1));

        lockManager.release(new Lock(key, X, txId0));

        assertTrue(lockManager.queue(key).isEmpty());

        List<List<LockMode>> lockModes = new ArrayList<>();

        lockModes.add(List.of(IX, S, SIX));
        lockModes.add(List.of(S, IX, SIX));

        for (List<LockMode> lockModes0 : lockModes) {
            lockManager.acquire(txId0, key, lockModes0.get(0)).join();
            lockManager.acquire(txId0, key, lockModes0.get(1)).join();

            assertEquals(lockModes0.get(2), lockManager.waiter(key, txId0).lockMode());

            lockManager.release(new Lock(key, lockModes0.get(1), txId0));

            assertTrue(lockManager.queue(key).isEmpty());
        }
    }

    @Test
    public void testReenter() {
        UUID txId = TestTransactionIds.newTransactionId();
        LockKey key = lockKey();

        CompletableFuture<Lock> fut = lockManager.acquire(txId, key, X);
        assertTrue(fut.isDone());

        fut = lockManager.acquire(txId, key, X);
        assertTrue(fut.isDone());

        assertThat(lockManager.queue(key), hasSize(1));

        lockManager.release(fut.join());

        assertTrue(lockManager.queue(key).isEmpty());

        fut = lockManager.acquire(txId, key, S);
        assertTrue(fut.isDone());

        fut = lockManager.acquire(txId, key, S);
        assertTrue(fut.isDone());

        assertThat(lockManager.queue(key), hasSize(1));

        lockManager.release(fut.join());

        assertTrue(lockManager.queue(key).isEmpty());
    }

    @Test
    public void testAcquireReleasedLock() {
        UUID txId0 = TestTransactionIds.newTransactionId();
        UUID txId1 = TestTransactionIds.newTransactionId();

        LockKey key = lockKey();

        List<List<LockMode>> lockModes = new ArrayList<>();

        lockModes.add(List.of(IS, X));
        lockModes.add(List.of(IX, X));
        lockModes.add(List.of(S, X));
        lockModes.add(List.of(SIX, X));
        lockModes.add(List.of(X, X));

        lockModes.add(List.of(IX, SIX));
        lockModes.add(List.of(S, SIX));
        lockModes.add(List.of(SIX, SIX));
        lockModes.add(List.of(X, SIX));

        lockModes.add(List.of(IX, S));
        lockModes.add(List.of(SIX, S));
        lockModes.add(List.of(X, S));

        lockModes.add(List.of(S, IX));
        lockModes.add(List.of(SIX, IX));
        lockModes.add(List.of(X, IX));

        lockModes.add(List.of(X, IS));

        for (List<LockMode> lockModes0 : lockModes) {
            CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, key, lockModes0.get(1));
            CompletableFuture<Lock> fut0 = lockManager.acquire(txId0, key, lockModes0.get(0));

            assertTrue(fut1.isDone());
            assertFalse(fut0.isDone());

            lockManager.release(fut1.join());

            assertTrue(fut0.isDone());

            lockManager.release(fut0.join());
        }
    }

    @Test
    public void testCompatibleLockModes() {
        UUID txId0 = TestTransactionIds.newTransactionId();
        UUID txId1 = TestTransactionIds.newTransactionId();

        LockKey key = lockKey();

        List<List<LockMode>> lockModes = new ArrayList<>();

        lockModes.add(List.of(IS, IS));
        lockModes.add(List.of(IS, IX));
        lockModes.add(List.of(IS, S));
        lockModes.add(List.of(IS, SIX));

        lockModes.add(List.of(IX, IS));
        lockModes.add(List.of(IX, IX));

        lockModes.add(List.of(S, IS));
        lockModes.add(List.of(S, S));

        lockModes.add(List.of(SIX, IS));

        for (List<LockMode> lockModes0 : lockModes) {
            CompletableFuture<Lock> fut0 = lockManager.acquire(txId0, key, lockModes0.get(0));
            CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, key, lockModes0.get(1));

            assertTrue(fut0.isDone());
            assertTrue(fut1.isDone());

            lockManager.release(fut0.join());
            lockManager.release(fut1.join());

            assertTrue(lockManager.queue(key).isEmpty());
        }
    }

    @Test
    public void testPossibleDowngradeLockModes() {
        UUID txId0 = TestTransactionIds.newTransactionId();

        LockKey key = lockKey();

        for (LockMode lockMode : List.of(SIX, S, IS, IX)) {
            CompletableFuture<Lock> fut0 = lockManager.acquire(txId0, key, X);

            assertEquals(X, fut0.join().lockMode());

            var lockFut = lockManager.acquire(txId0, key, lockMode);

            Waiter waiter = lockManager.waiter(fut0.join().lockKey(), txId0);

            assertEquals(LockMode.supremum(lockMode, X), waiter.lockMode());

            lockManager.release(txId0, key, X);

            assertThat(lockManager.queue(key), hasSize(1));

            waiter = lockManager.waiter(fut0.join().lockKey(), txId0);

            assertEquals(lockMode, waiter.lockMode());

            assertThat(lockManager.queue(key), hasSize(1));

            lockManager.release(lockFut.join());
        }
    }

    @Test
    public void testAcquireRelease() {
        UUID txId = TestTransactionIds.newTransactionId();
        LockKey key = lockKey();

        for (LockMode lockMode : LockMode.values()) {
            lockManager.acquire(txId, key, lockMode);
            lockManager.release(txId, key, lockMode);

            assertFalse(lockManager.locks(txId).hasNext());
        }

        assertTrue(lockManager.isEmpty());
    }

    @Test
    public void testAcquireReleaseWhenHoldOther() {
        UUID txId = TestTransactionIds.newTransactionId();
        LockKey key = lockKey();

        for (LockMode holdLockMode : LockMode.values()) {
            lockManager.acquire(txId, key, holdLockMode);

            assertTrue(lockManager.locks(txId).hasNext());
            assertSame(holdLockMode, lockManager.locks(txId).next().lockMode());

            for (LockMode lockMode : LockMode.values()) {
                lockManager.acquire(txId, key, lockMode);
                lockManager.release(txId, key, lockMode);
            }

            assertTrue(lockManager.locks(txId).hasNext());
            assertSame(holdLockMode, lockManager.locks(txId).next().lockMode());

            lockManager.release(txId, key, holdLockMode);

            assertFalse(lockManager.locks(txId).hasNext());
        }

        assertTrue(lockManager.isEmpty());
    }

    @Test
    public void testReleaseThenReleaseWeakerInHierarchy() {
        LockKey key = lockKey();

        UUID txId1 = TestTransactionIds.newTransactionId();
        UUID txId2 = TestTransactionIds.newTransactionId();

        var tx1SharedLock = lockManager.acquire(txId2, key, S);

        assertTrue(tx1SharedLock.isDone());

        var tx1ExclusiveLock = lockManager.acquire(txId2, key, X);

        assertTrue(tx1ExclusiveLock.isDone());

        var tx2SharedLock = lockManager.acquire(txId1, key, S);

        assertFalse(tx2SharedLock.isDone());

        lockManager.release(txId2, key, X);

        assertTrue(lockManager.locks(txId2).hasNext());

        var lock = lockManager.locks(txId2).next();

        assertSame(S, lock.lockMode());

        assertTrue(tx2SharedLock.isDone());
    }

    @Test
    public void testReleaseThenNoReleaseWeakerInHierarchy() {
        LockKey key = lockKey();

        UUID txId1 = TestTransactionIds.newTransactionId();
        UUID txId2 = TestTransactionIds.newTransactionId();

        var tx1SharedLock = lockManager.acquire(txId2, key, S);

        assertTrue(tx1SharedLock.isDone());

        var tx1ExclusiveLock = lockManager.acquire(txId2, key, X);

        assertTrue(tx1ExclusiveLock.isDone());

        var tx2SharedLock = lockManager.acquire(txId1, key, S);

        assertFalse(tx2SharedLock.isDone());

        lockManager.release(txId2, key, S);

        assertTrue(lockManager.locks(txId2).hasNext());

        var lock = lockManager.locks(txId2).next();

        assertSame(X, lock.lockMode());

        assertFalse(tx2SharedLock.isDone());
    }

    @Test
    public void testLockingOverloadAndUpgrade() {
        LockKey key = lockKey();

        UUID tx1 = TestTransactionIds.newTransactionId();
        UUID tx2 = TestTransactionIds.newTransactionId();

        var tx1Lock = lockManager.acquire(tx2, key, X);

        assertTrue(tx1Lock.isDone());

        var tx2sLock = lockManager.acquire(tx1, key, S);

        assertFalse(tx2sLock.isDone());

        var tx2xLock = lockManager.acquire(tx1, key, X);

        assertFalse(tx2xLock.isDone());

        lockManager.release(tx1Lock.join());

        assertThat(tx2sLock, willSucceedFast());
        assertThat(tx2xLock, willSucceedFast());
    }

    @Test
    public void testLockingOverload() {
        LockKey key = lockKey();

        UUID tx1 = TestTransactionIds.newTransactionId();
        UUID tx2 = TestTransactionIds.newTransactionId();

        var tx1Lock = lockManager.acquire(tx2, key, X);

        assertTrue(tx1Lock.isDone());

        var tx2xLock = lockManager.acquire(tx1, key, X);

        assertFalse(tx2xLock.isDone());

        var tx2s1Lock = lockManager.acquire(tx1, key, S);
        var tx2s2Lock = lockManager.acquire(tx1, key, S);

        assertFalse(tx2s1Lock.isDone());
        assertFalse(tx2s2Lock.isDone());

        lockManager.release(tx1Lock.join());

        assertThat(tx2xLock, willSucceedFast());
        assertThat(tx2s1Lock, willSucceedFast());
        assertThat(tx2s2Lock, willSucceedFast());
    }

    @Test
    public void testFailUpgrade() {
        LockKey key = lockKey();

        UUID tx1 = TestTransactionIds.newTransactionId();
        UUID tx2 = TestTransactionIds.newTransactionId();
        UUID tx3 = TestTransactionIds.newTransactionId();

        var tx1Lock = lockManager.acquire(tx1, key, S);
        var tx2Lock = lockManager.acquire(tx2, key, S);
        var tx3Lock = lockManager.acquire(tx3, key, S);

        assertTrue(tx1Lock.isDone());
        assertTrue(tx2Lock.isDone());
        assertTrue(tx3Lock.isDone());

        var tx1xLock = lockManager.acquire(tx1, key, X);
        var tx2xLock = lockManager.acquire(tx2, key, X);

        assertFalse(tx1xLock.isDone());
        assertFalse(tx2xLock.isDone());

        lockManager.release(tx3Lock.join());

        expectConflict(tx2xLock);
        assertFalse(tx1xLock.isDone());

        lockManager.release(tx2Lock.join());

        assertTrue(tx1xLock.isDone());

        lockManager.release(tx1xLock.join());
    }

    @Test
    public void testDowngradeTargetLock() {
        LockKey key = lockKey();

        UUID tx1 = TestTransactionIds.newTransactionId();
        UUID tx2 = TestTransactionIds.newTransactionId();

        var tx1Lock = lockManager.acquire(tx1, key, S);
        var tx2Lock = lockManager.acquire(tx2, key, S);

        assertThat(tx1Lock, willSucceedFast());
        assertThat(tx2Lock, willSucceedFast());

        var tx1IxLock = lockManager.acquire(tx1, key, IX);

        assertFalse(tx1IxLock.isDone());

        assertEquals(SIX, lockManager.waiter(key, tx1).intendedLockMode());

        lockManager.release(tx1, key, S);

        assertFalse(tx1IxLock.isDone());
        assertEquals(IX, lockManager.waiter(key, tx1).intendedLockMode());

        lockManager.release(tx2, key, S);

        assertThat(tx1IxLock, willSucceedFast());
    }

    @Test
    public void testFailWait() {
        LockKey key = lockKey();

        UUID tx1 = TestTransactionIds.newTransactionId();
        UUID tx2 = TestTransactionIds.newTransactionId();
        UUID tx3 = TestTransactionIds.newTransactionId();

        var tx3Lock = lockManager.acquire(tx3, key, S);

        assertThat(tx3Lock, willSucceedFast());

        var tx2Lock = lockManager.acquire(tx2, key, X);

        assertFalse(tx2Lock.isDone());

        var tx1Lock = lockManager.acquire(tx1, key, X);

        assertFalse(tx1Lock.isDone());

        lockManager.release(tx3, key, S);

        expectConflict(tx2Lock);

        assertThat(tx1Lock, willSucceedFast());
    }

    @Test
    public void testWaitInOrder() {
        LockKey key = lockKey();

        UUID tx1 = TestTransactionIds.newTransactionId();
        UUID tx2 = TestTransactionIds.newTransactionId();
        UUID tx3 = TestTransactionIds.newTransactionId();

        var tx3IxLock = lockManager.acquire(tx3, key, IX);
        var tx3Lock = lockManager.acquire(tx3, key, S);

        assertThat(tx3IxLock, willSucceedFast());
        assertThat(tx3Lock, willSucceedFast());

        var tx2Lock = lockManager.acquire(tx2, key, IX);

        assertFalse(tx2Lock.isDone());

        var tx1Lock = lockManager.acquire(tx1, key, X);

        assertFalse(tx1Lock.isDone());

        lockManager.release(tx3, key, S);

        assertThat(tx2Lock, willSucceedFast());

        lockManager.release(tx3, key, IX);
        lockManager.release(tx2, key, IX);

        assertThat(tx3Lock, willSucceedFast());
    }

    @Test
    public void testWaitNotInOrder() {
        LockKey key = lockKey();

        UUID tx1 = TestTransactionIds.newTransactionId();
        UUID tx2 = TestTransactionIds.newTransactionId();
        UUID tx3 = TestTransactionIds.newTransactionId();

        var tx3IxLock = lockManager.acquire(tx3, key, IX);
        var tx3Lock = lockManager.acquire(tx3, key, S);

        assertThat(tx3IxLock, willSucceedFast());
        assertThat(tx3Lock, willSucceedFast());

        var tx2Lock = lockManager.acquire(tx2, key, X);

        assertFalse(tx2Lock.isDone());

        var tx1Lock = lockManager.acquire(tx1, key, IX);

        assertFalse(tx1Lock.isDone());

        lockManager.release(tx3, key, S);

        assertThat(tx1Lock, willSucceedFast());

        lockManager.release(tx1, key, IX);
        lockManager.release(tx3, key, IX);

        assertThat(tx2Lock, willSucceedFast());
    }

    @Test
    public void testWaitFailNotInOrder() {
        LockKey key = lockKey();

        UUID tx1 = TestTransactionIds.newTransactionId();
        UUID tx2 = TestTransactionIds.newTransactionId();
        UUID tx3 = TestTransactionIds.newTransactionId();

        var tx3IxLock = lockManager.acquire(tx3, key, IX);
        var tx3Lock = lockManager.acquire(tx3, key, S);

        assertThat(tx3IxLock, willSucceedFast());
        assertThat(tx3Lock, willSucceedFast());

        var tx2Lock = lockManager.acquire(tx2, key, X);

        assertFalse(tx2Lock.isDone());

        var tx1Lock = lockManager.acquire(tx1, key, IX);

        assertFalse(tx1Lock.isDone());

        lockManager.release(tx3, key, S);

        assertThat(tx1Lock, willSucceedFast());

        lockManager.release(tx3, key, IX);
        lockManager.release(tx1, key, IX);

        expectConflict(tx2Lock);
    }

    @Test
    public void testLocksInIterator() {
        UUID txId1 = TestTransactionIds.newTransactionId();

        LockKey key = new LockKey(0);

        lockManager.acquire(txId1, key, S).join();

        assertTrue(lockManager.locks(txId1).hasNext());

        LockKey key2 = new LockKey(1, 1);

        lockManager.acquire(txId1, key2, S).join();

        AtomicInteger counter = new AtomicInteger();

        lockManager.locks(txId1).forEachRemaining(lock -> counter.incrementAndGet());

        assertEquals(2, counter.get());
    }

    @Test
    public void testLockIsReleased() {
        LockKey key = lockKey();

        UUID txId1 = TestTransactionIds.newTransactionId();

        lockManager.acquire(txId1, key, X).join();

        assertFalse(lockManager.isEmpty());

        lockManager.release(txId1, key, X);

        assertTrue(lockManager.isEmpty());

        UUID txId2 = TestTransactionIds.newTransactionId();

        lockManager.acquire(txId2, key, X).join();

        lockManager.release(txId2, key, X);

        assertTrue(lockManager.isEmpty());
    }

    @Test
    public void testAcquireLockAfterFail() {
        UUID tx1 = TestTransactionIds.newTransactionId();
        UUID tx2 = TestTransactionIds.newTransactionId();
        UUID tx3 = TestTransactionIds.newTransactionId();

        var key = lockKey();

        assertThat(lockManager.acquire(tx1, key, S), willCompleteSuccessfully());
        assertThat(lockManager.acquire(tx2, key, IS), willCompleteSuccessfully());

        assertThat(lockManager.acquire(tx2, key, X), willThrow(LockException.class));

        assertThat(lockManager.acquire(tx2, key, S), willCompleteSuccessfully());

        assertThat(lockManager.acquire(tx3, key, S), willCompleteSuccessfully());

        lockManager.releaseAll(tx1);

        CompletableFuture<?> f = lockManager.acquire(tx2, key, X);
        assertFalse(f.isDone());

        lockManager.releaseAll(tx3);

        assertThat(f, willCompleteSuccessfully());
    }

    @Test
    public void testFailWaiter() {
        UUID older = TestTransactionIds.newTransactionId();
        UUID newer = TestTransactionIds.newTransactionId();

        CompletableFuture<Lock> fut1 = lockManager.acquire(newer, lockKey(), X);
        assertTrue(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(older, lockKey(), S);
        assertFalse(fut2.isDone());

        // Should do nothing then called on owner.
        lockManager.failAllWaiters(newer, new Exception());
        assertFalse(fut2.isDone());

        lockManager.failAllWaiters(older, new Exception("test"));
        assertThat(fut2, willThrowWithCauseOrSuppressed(Exception.class, "test"));

        lockManager.releaseAll(older);
        lockManager.releaseAll(newer);
    }

    @Test
    public void testFailWaiter2() {
        UUID tx1 = TestTransactionIds.newTransactionId();
        UUID tx2 = TestTransactionIds.newTransactionId();
        UUID tx3 = TestTransactionIds.newTransactionId();

        CompletableFuture<Lock> fut1 = lockManager.acquire(tx1, lockKey(), S);
        assertTrue(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(tx2, lockKey(), S);
        assertTrue(fut2.isDone());

        CompletableFuture<Lock> fut3 = lockManager.acquire(tx3, lockKey(), S);
        assertTrue(fut3.isDone());

        CompletableFuture<Lock> fut4 = lockManager.acquire(tx2, lockKey(), X);
        assertFalse(fut4.isDone());

        lockManager.releaseAll(tx3);

        assertThat(fut4, willThrowWithCauseOrSuppressed(PossibleDeadlockOnLockAcquireException.class));
        // Failing already invalidated waiter should do nothing.
        lockManager.failAllWaiters(tx2, new Exception());

        lockManager.releaseAll(tx2);
        lockManager.releaseAll(tx1);
    }

    @Test
    public void testFailWaiter3() {
        UUID tx1 = TestTransactionIds.newTransactionId();
        UUID tx2 = TestTransactionIds.newTransactionId();
        UUID tx3 = TestTransactionIds.newTransactionId();

        CompletableFuture<Lock> fut3 = lockManager.acquire(tx3, lockKey(), S);
        assertTrue(fut3.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(tx2, lockKey(), X);
        assertFalse(fut2.isDone());

        CompletableFuture<Lock> fut1 = lockManager.acquire(tx1, lockKey(), S);
        assertTrue(fut1.isDone());

        lockManager.releaseAll(tx3);

        assertThat(fut2, willThrowWithCauseOrSuppressed(PossibleDeadlockOnLockAcquireException.class));
        // Failing already invalidated waiter should do nothing.
        lockManager.failAllWaiters(tx2, new Exception());

        lockManager.releaseAll(tx2);
        lockManager.releaseAll(tx1);
    }

    @Test
    public void testFailWaiter4() {
        UUID tx1 = TestTransactionIds.newTransactionId();
        UUID tx2 = TestTransactionIds.newTransactionId();
        UUID tx3 = TestTransactionIds.newTransactionId();

        CompletableFuture<Lock> fut3 = lockManager.acquire(tx3, lockKey(), S);
        assertTrue(fut3.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(tx2, lockKey(), X);
        assertFalse(fut2.isDone());

        CompletableFuture<Lock> fut1 = lockManager.acquire(tx1, lockKey(), X);
        assertFalse(fut1.isDone());

        lockManager.failAllWaiters(tx2, new Exception("test"));
        assertThat(fut2, willThrowWithCauseOrSuppressed(Exception.class, "test"));
        assertFalse(fut1.isDone());

        lockManager.releaseAll(tx3);
        assertThat(fut1, willCompleteSuccessfully());

        lockManager.releaseAll(tx1);
    }

    /**
     * Do test single key multithreaded.
     *
     * @param duration The duration.
     * @param readLocks Read lock accumulator.
     * @param writeLocks Write lock accumulator.
     * @param failedLocks Failed lock accumulator.
     * @param mode Mode: 0 - read only, 1 - write only, 2 - mixed random.
     * @throws InterruptedException If interrupted while waiting.
     */
    private void doTestSingleKeyMultithreaded(
            long duration,
            LongAdder readLocks,
            LongAdder writeLocks,
            LongAdder failedLocks,
            int mode
    ) throws InterruptedException {
        LockKey key = lockKey();

        Thread[] threads = new Thread[Runtime.getRuntime().availableProcessors() * 2];

        CyclicBarrier startBar = new CyclicBarrier(threads.length, () -> log.info("Before test"));

        AtomicBoolean stop = new AtomicBoolean();

        Random r = new Random();

        AtomicReference<Throwable> firstErr = new AtomicReference<>();

        try {
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        startBar.await();
                    } catch (Exception e) {
                        fail();
                    }

                    while (!stop.get() && firstErr.get() == null) {
                        UUID txId = TestTransactionIds.newTransactionId();

                        if (mode == 0 ? false : mode == 1 ? true : r.nextBoolean()) {
                            Lock lock;
                            try {
                                lock = lockManager.acquire(txId, key, X).join();

                                writeLocks.increment();
                            } catch (CompletionException e) {
                                failedLocks.increment();
                                continue;
                            }

                            lockManager.release(lock);
                        } else {
                            Lock lock;
                            try {
                                lock = lockManager.acquire(txId, key, S).join();

                                readLocks.increment();
                            } catch (CompletionException e) {
                                if (mode == 0) {
                                    fail("Unexpected exception for read only locking mode");
                                }

                                failedLocks.increment();

                                continue;
                            }

                            lockManager.release(lock);
                        }
                    }
                });

                threads[i].setName("Worker" + i);

                threads[i].setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        firstErr.compareAndExchange(null, e);
                    }
                });

                threads[i].start();
            }

            Thread.sleep(duration);

            stop.set(true);
        } finally {
            for (Thread thread : threads) {
                thread.join();
            }
        }

        if (firstErr.get() != null) {
            throw new IgniteException(INTERNAL_ERR, firstErr.get());
        }

        log.info("After test readLocks={} writeLocks={} failedLocks={}", readLocks.sum(), writeLocks.sum(),
                failedLocks.sum());

        assertTrue(lockManager.queue(key).isEmpty());
    }

    private UUID[] generate(int num) {
        UUID[] tmp = new UUID[num];

        for (int i = 0; i < tmp.length; i++) {
            tmp[i] = TestTransactionIds.newTransactionId();
        }

        for (int i = 1; i < tmp.length; i++) {
            assertTrue(tmp[i - 1].compareTo(tmp[i]) < 0);
        }

        return tmp;
    }

    private void expectConflict(CompletableFuture<Lock> fut) {
        try {
            fut.join();

            fail();
        } catch (CompletionException e) {
            assertTrue(IgniteTestUtils.hasCause(e, LockException.class, null),
                    "Wrong exception type, expecting LockException");
        }
    }
}
