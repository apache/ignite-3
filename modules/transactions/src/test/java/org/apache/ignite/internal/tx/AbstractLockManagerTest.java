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

import static org.apache.ignite.internal.tx.LockMode.IS;
import static org.apache.ignite.internal.tx.LockMode.IX;
import static org.apache.ignite.internal.tx.LockMode.S;
import static org.apache.ignite.internal.tx.LockMode.SIX;
import static org.apache.ignite.internal.tx.LockMode.X;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests a LockManager implementation.
 */
public abstract class AbstractLockManagerTest extends IgniteAbstractTest {
    private LockManager lockManager;

    @BeforeEach
    public void before() {
        lockManager = newInstance();
    }

    protected abstract LockManager newInstance();

    @Test
    public void testSingleKeyWrite() throws LockException {
        UUID txId1 = Timestamp.nextVersion().toUuid();

        LockKey key = new LockKey("test");

        CompletableFuture<Lock> fut0 = lockManager.acquire(txId1, key, X);

        assertTrue(fut0.isDone());

        Collection<UUID> queue = lockManager.queue(key);

        assertTrue(queue.size() == 1 && queue.iterator().next().equals(txId1));

        Waiter waiter = lockManager.waiter(key, txId1);

        assertTrue(waiter.locked());

        lockManager.release(fut0.join());
    }

    @Test
    public void testSingleKeyWriteLock() throws LockException {
        UUID txId1 = Timestamp.nextVersion().toUuid();

        LockKey key = new LockKey("test");

        CompletableFuture<Lock> fut0 = lockManager.acquire(txId1, key, X);

        assertTrue(fut0.isDone());

        UUID txId2 = Timestamp.nextVersion().toUuid();

        assertTrue(txId1.compareTo(txId2) < 0);

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId2, key, X);

        assertFalse(fut1.isDone());

        assertTrue(lockManager.waiter(key, txId1).locked());
        assertFalse(lockManager.waiter(key, txId2).locked());

        lockManager.release(fut0.join());

        assertTrue(fut1.isDone());

        assertNull(lockManager.waiter(key, txId1));
        assertTrue(lockManager.waiter(key, txId2).locked());

        lockManager.release(fut1.join());

        assertNull(lockManager.waiter(key, txId1));
        assertNull(lockManager.waiter(key, txId2));
    }

    @Test
    public void downgradeLockOutOfTurnTest() throws Exception {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        UUID txId2 = Timestamp.nextVersion().toUuid();

        LockKey key = new LockKey("test");

        lockManager.acquire(txId0, key, S).join();
        lockManager.acquire(txId2, key, S).join();

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId2, key, X);
        assertFalse(fut2.isDone());

        CompletableFuture<Lock> fut0 = lockManager.acquire(txId0, key, X);
        fut0.get(10, TimeUnit.SECONDS);

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, key, S);
        assertFalse(fut1.isDone());

        assertFalse(fut2.isDone());
        fut0.thenAccept(lock -> lockManager.release(lock));

        fut1.join();
        expectConflict(fut2);
    }

    @Test
    public void upgradeLockImmediatelyTest() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        UUID txId2 = Timestamp.nextVersion().toUuid();

        LockKey key = new LockKey("test");

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
    public void testSingleKeyReadWriteLock() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        UUID txId2 = Timestamp.nextVersion().toUuid();
        UUID txId3 = Timestamp.nextVersion().toUuid();
        assertTrue(txId0.compareTo(txId1) < 0);
        assertTrue(txId1.compareTo(txId2) < 0);
        assertTrue(txId2.compareTo(txId3) < 0);
        LockKey key = new LockKey("test");

        CompletableFuture<Lock> fut0 = lockManager.acquire(txId0, key, S);
        assertTrue(fut0.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId2, key, S);
        assertTrue(fut2.isDone());

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, key, S);
        assertTrue(fut1.isDone());

        CompletableFuture<Lock> fut3 = lockManager.acquire(txId3, key, X);
        assertFalse(fut3.isDone());

        assertTrue(lockManager.waiter(key, txId0).locked());
        assertTrue(lockManager.waiter(key, txId1).locked());
        assertTrue(lockManager.waiter(key, txId2).locked());
        assertFalse(lockManager.waiter(key, txId3).locked());

        lockManager.release(fut2.join());

        assertTrue(lockManager.waiter(key, txId0).locked());
        assertTrue(lockManager.waiter(key, txId1).locked());
        assertNull(lockManager.waiter(key, txId2));
        assertFalse(lockManager.waiter(key, txId3).locked());

        lockManager.release(fut0.join());

        assertNull(lockManager.waiter(key, txId0));
        assertTrue(lockManager.waiter(key, txId1).locked());
        assertNull(lockManager.waiter(key, txId2));
        assertFalse(lockManager.waiter(key, txId3).locked());

        lockManager.release(fut1.join());

        assertNull(lockManager.waiter(key, txId0));
        assertNull(lockManager.waiter(key, txId1));
        assertNull(lockManager.waiter(key, txId2));
        assertTrue(lockManager.waiter(key, txId3).locked());
    }

    @Test
    public void testSingleKeyReadWriteConflict() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        LockKey key = new LockKey("test");

        // Lock in order
        CompletableFuture<Lock> fut0 = lockManager.acquire(txId0, key, S);
        assertTrue(fut0.isDone());

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, key, X);
        assertFalse(fut1.isDone());

        lockManager.release(fut0.join());
        assertTrue(fut1.isDone());

        lockManager.release(fut1.join());

        assertTrue(lockManager.queue(key).isEmpty());

        // Lock not in order
        fut0 = lockManager.acquire(txId1, key, S);
        assertTrue(fut0.isDone());

        try {
            lockManager.acquire(txId0, key, X).join();

            fail();
        } catch (CompletionException e) {
            // Expected.
        }
    }

    @Test
    public void testSingleKeyReadWriteConflict2() throws LockException {
        UUID[] txId = generate(3);
        LockKey key = new LockKey("test");

        // Lock in order
        CompletableFuture<Lock> fut0 = lockManager.acquire(txId[1], key, S);
        assertTrue(fut0.isDone());

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId[2], key, X);
        assertFalse(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId[0], key, S);
        assertTrue(fut2.isDone());

        lockManager.release(fut0.join());
        lockManager.release(fut2.join());

        assertTrue(fut1.isDone());
    }

    @Test
    public void testSingleKeyReadWriteConflict3() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        UUID txId2 = Timestamp.nextVersion().toUuid();
        LockKey key = new LockKey("test");

        // Lock in order
        CompletableFuture<Lock> fut0 = lockManager.acquire(txId0, key, S);
        assertTrue(fut0.isDone());

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId2, key, X);
        assertFalse(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId1, key, S);
        assertTrue(fut2.isDone());

        assertFalse(lockManager.waiter(key, txId2).locked());

        lockManager.release(fut2.join());
        lockManager.release(fut0.join());

        assertTrue(fut1.isDone());
    }

    @Test
    public void testSingleKeyReadWriteConflict4() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        final UUID txId2 = Timestamp.nextVersion().toUuid();
        UUID txId3 = Timestamp.nextVersion().toUuid();
        LockKey key = new LockKey("test");

        CompletableFuture<Lock> fut0 = lockManager.acquire(txId0, key, S);
        assertTrue(fut0.isDone());

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, key, X);
        assertFalse(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId3, key, X);
        assertFalse(fut2.isDone());

        CompletableFuture<Lock> fut3 = lockManager.acquire(txId2, key, X);
        assertFalse(fut3.isDone());
    }

    @Test
    public void testSingleKeyReadWriteConflict5() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        LockKey key = new LockKey("test");

        lockManager.acquire(txId1, key, X).join();

        expectConflict(lockManager.acquire(txId0, key, X));
    }

    @Test
    public void testSingleKeyReadWriteConflict6() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        LockKey key = new LockKey("test");

        lockManager.acquire(txId0, key, S).join();

        lockManager.acquire(txId1, key, S).join();

        CompletableFuture<Lock> fut = lockManager.acquire(txId1, key, X);
        assertFalse(fut.isDone());

        Lock lock0 = lockManager.acquire(txId0, key, X).join();

        lockManager.release(lock0);

        expectConflict(fut);
    }

    @Test
    public void testConflicts() throws Exception {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();

        LockKey key = new LockKey("test");

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
            CompletableFuture<Lock> fut0 = lockManager.acquire(txId1, key, lockModePair.get1());
            CompletableFuture<Lock> fut1 = lockManager.acquire(txId0, key, lockModePair.get2());

            assertTrue(fut0.isDone());
            expectConflict(fut1);

            lockManager.release(fut0.join());

            assertTrue(lockManager.queue(key).isEmpty());
        }
    }

    @Test
    public void testSingleKeyWriteWriteConflict() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        UUID txId2 = Timestamp.nextVersion().toUuid();
        LockKey key = new LockKey("test");

        // Lock in order
        CompletableFuture<Lock> fut0 = lockManager.acquire(txId1, key, X);
        assertTrue(fut0.isDone());

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId2, key, X);
        assertFalse(fut1.isDone());

        try {
            lockManager.acquire(txId0, key, X).join();

            fail();
        } catch (CompletionException e) {
            // Expected.
        }
    }

    @Test
    public void testSingleKeyWriteWriteConflict2() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        UUID txId2 = Timestamp.nextVersion().toUuid();
        LockKey key = new LockKey("test");

        // Lock in order
        CompletableFuture<Lock> fut0 = lockManager.acquire(txId0, key, X);
        assertTrue(fut0.isDone());

        CompletableFuture<Lock> fut1 = lockManager.acquire(txId2, key, X);
        assertFalse(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId1, key, X);
        assertFalse(fut2.isDone());
    }

    @Test
    public void testSingleKeyMultithreadedRead() throws InterruptedException {
        LongAdder readLocks = new LongAdder();
        LongAdder writeLocks = new LongAdder();
        LongAdder failedLocks = new LongAdder();

        doTestSingleKeyMultithreaded(5_000, readLocks, writeLocks, failedLocks, 0);

        assertTrue(writeLocks.sum() == 0);
        assertTrue(failedLocks.sum() == 0);
    }

    @Test
    public void testSingleKeyMultithreadedWrite() throws InterruptedException {
        LongAdder readLocks = new LongAdder();
        LongAdder writeLocks = new LongAdder();
        LongAdder failedLocks = new LongAdder();

        doTestSingleKeyMultithreaded(5_000, readLocks, writeLocks, failedLocks, 1);

        assertTrue(readLocks.sum() == 0);
    }

    @Test
    public void testSingleKeyMultithreadedRandom() throws InterruptedException {
        LongAdder readLocks = new LongAdder();
        LongAdder writeLocks = new LongAdder();
        LongAdder failedLocks = new LongAdder();

        doTestSingleKeyMultithreaded(5_000, readLocks, writeLocks, failedLocks, 2);
    }

    @Test
    public void testLockUpgrade() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        LockKey key = new LockKey("test");

        lockManager.acquire(txId1, key, S).join();

        Lock lock = lockManager.acquire(txId0, key, S).join();

        CompletableFuture<Lock> fut = lockManager.acquire(txId1, key, X);
        assertFalse(fut.isDone());

        lockManager.release(lock);

        fut.join();

        lockManager.release(fut.join());

        assertTrue(lockManager.queue(key).isEmpty());
    }

    @Test
    public void testLockUpgrade2() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        LockKey key = new LockKey("test");

        lockManager.acquire(txId0, key, S).join();

        lockManager.acquire(txId1, key, S).join();

        expectConflict(lockManager.acquire(txId0, key, X));
    }

    @Test
    public void testLockUpgrade3() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        UUID txId2 = Timestamp.nextVersion().toUuid();
        LockKey key = new LockKey("test");

        lockManager.acquire(txId1, key, S).join();

        lockManager.acquire(txId0, key, S).join();

        lockManager.acquire(txId2, key, S).join();

        try {
            lockManager.acquire(txId1, key, X).join();
        } catch (CompletionException e) {
            // Expected.
        }
    }

    @Test
    public void testLockUpgrade4() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        LockKey key = new LockKey("test");

        lockManager.acquire(txId1, key, S).join();

        Lock lock = lockManager.acquire(txId0, key, S).join();

        CompletableFuture<Lock> fut = lockManager.acquire(txId1, key, X);

        assertFalse(fut.isDone());

        lockManager.release(lock);

        fut.join();

        assertTrue(lockManager.queue(key).size() == 1);
    }

    @Test
    public void testLockUpgrade5() {
        UUID txId0 = Timestamp.nextVersion().toUuid();

        LockKey key = new LockKey("test");

        for (LockMode lockMode : List.of(IS, IX, SIX, X)) {
            lockManager.acquire(txId0, key, lockMode).join();

            assertEquals(lockMode, lockManager.waiter(key, txId0).lockMode());
        }

        assertTrue(lockManager.queue(key).size() == 1);

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
    public void testReenter() throws LockException {
        UUID txId = Timestamp.nextVersion().toUuid();
        LockKey key = new LockKey("test");

        CompletableFuture<Lock> fut = lockManager.acquire(txId, key, X);
        assertTrue(fut.isDone());

        fut = lockManager.acquire(txId, key, X);
        assertTrue(fut.isDone());

        assertTrue(lockManager.queue(key).size() == 1);

        lockManager.release(fut.join());

        assertTrue(lockManager.queue(key).isEmpty());

        fut = lockManager.acquire(txId, key, S);
        assertTrue(fut.isDone());

        fut = lockManager.acquire(txId, key, S);
        assertTrue(fut.isDone());

        assertTrue(lockManager.queue(key).size() == 1);

        lockManager.release(fut.join());

        assertTrue(lockManager.queue(key).isEmpty());
    }

    @Test
    public void testAcquireReleasedLock() {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();

        LockKey key = new LockKey("test");

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
            CompletableFuture<Lock> fut0 = lockManager.acquire(txId0, key, lockModes0.get(1));
            CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, key, lockModes0.get(0));

            assertTrue(fut0.isDone());
            assertFalse(fut1.isDone());

            lockManager.release(fut0.join());

            assertTrue(fut1.isDone());

            lockManager.release(fut1.join());
        }
    }

    @Test
    public void testCompatibleLockModes() {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();

        LockKey key = new LockKey("test");

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
    public void testPossibleDowngradeLockModes() throws Exception {
        UUID txId0 = Timestamp.nextVersion().toUuid();

        LockKey key = new LockKey("test");

        CompletableFuture<Lock> fut0 = lockManager.acquire(txId0, key, X);

        assertEquals(X, fut0.join().lockMode());

        List<LockMode> lockModes = List.of(SIX, S, IS);

        LockMode lastLockMode;

        for (LockMode lockMode : lockModes) {
            Waiter waiter = lockManager.waiter(fut0.join().lockKey(), txId0);

            lastLockMode = waiter.lockMode();

            assertEquals(lastLockMode, waiter.lockMode());

            lockManager.downgrade(fut0.join(), lockMode);

            assertTrue(lockManager.queue(key).size() == 1);

            waiter = lockManager.waiter(fut0.join().lockKey(), txId0);

            assertEquals(lockMode, waiter.lockMode());

            assertTrue(lockManager.queue(key).size() == 1);
        }

        lockManager.release(fut0.join());


        fut0 = lockManager.acquire(txId0, key, X);

        assertEquals(X, fut0.join().lockMode());

        lockModes = List.of(SIX, IX, IS);

        for (LockMode lockMode : lockModes) {
            Waiter waiter = lockManager.waiter(fut0.join().lockKey(), txId0);

            lastLockMode = waiter.lockMode();

            assertEquals(lastLockMode, waiter.lockMode());

            lockManager.downgrade(fut0.join(), lockMode);

            assertTrue(lockManager.queue(key).size() == 1);

            waiter = lockManager.waiter(fut0.join().lockKey(), txId0);

            assertEquals(lockMode, waiter.lockMode());

            assertTrue(lockManager.queue(key).size() == 1);
        }

        lockManager.release(fut0.join());
    }

    @Test
    public void testImpossibleDowngradeLockModes1() {
        UUID txId0 = Timestamp.nextVersion().toUuid();

        LockKey key = new LockKey("test");

        List<List<LockMode>> lockModes = new ArrayList<>();

        lockModes.add(List.of(S, IX));
        lockModes.add(List.of(IX, S));

        for (List<LockMode> lockModes0 : lockModes) {
            CompletableFuture<Lock> fut = lockManager.acquire(txId0, key, lockModes0.get(0));

            try {
                lockManager.downgrade(fut.join(), lockModes0.get(1));

                fail();
            } catch (LockException e) {
                // Expected.
            }

            assertEquals(1, lockManager.queue(key).size());
            assertEquals(lockModes0.get(0), lockManager.waiter(fut.join().lockKey(), txId0).lockMode());

            lockManager.release(fut.join());
        }

    }

    @Test
    public void testImpossibleDowngradeLockModes2() {
        UUID txId0 = Timestamp.nextVersion().toUuid();

        LockKey key = new LockKey("test");

        CompletableFuture<Lock> fut = lockManager.acquire(txId0, key, IS);

        try {
            lockManager.downgrade(fut.join(), X);

            fail();
        } catch (LockException e) {
            // Expected.
        }

        assertEquals(1, lockManager.queue(key).size());

        lockManager.release(fut.join());
    }

    @Test
    public void testImpossibleDowngradeLockModes3() {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();

        LockKey key = new LockKey("test");

        CompletableFuture<Lock> fut0 = lockManager.acquire(txId0, key, S);
        lockManager.acquire(txId1, key, S);

        try {
            lockManager.downgrade(fut0.join(), IX);

            fail();
        } catch (LockException e) {
            // Expected.
        }

        assertEquals(2, lockManager.queue(key).size());
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
        LockKey key = new LockKey("test");

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
                        UUID txId = Timestamp.nextVersion().toUuid();

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
            throw new IgniteException(firstErr.get());
        }

        log.info("After test readLocks={} writeLocks={} failedLocks={}", readLocks.sum(), writeLocks.sum(),
                failedLocks.sum());

        assertTrue(lockManager.queue(key).isEmpty());
    }

    private UUID[] generate(int num) {
        UUID[] tmp = new UUID[num];

        for (int i = 0; i < tmp.length; i++) {
            tmp[i] = Timestamp.nextVersion().toUuid();
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
