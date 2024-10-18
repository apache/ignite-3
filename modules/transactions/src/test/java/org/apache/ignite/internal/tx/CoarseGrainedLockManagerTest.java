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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests coarse lock modes. It allows IX, S locks and upgrade from S to SIX (S, then IX).
 */
public class CoarseGrainedLockManagerTest {
    private final HeapLockManager lockManager = new HeapLockManager(new WaitDieDeadlockPreventionPolicy());

    @AfterEach
    void after() {
        assertTrue(lockManager.isEmpty());
    }

    @Test
    public void testSimple() {
        UUID txId1 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, lockKey(), LockMode.IX);
        assertTrue(fut1.isDone());

        UUID txId2 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut2 = lockManager.acquire(txId2, lockKey(), LockMode.S);
        assertFalse(fut2.isDone());

        lockManager.releaseAll(txId1);
        fut2.join();

        lockManager.releaseAll(txId2);
    }

    @Test
    public void testSimpleInverse() {
        UUID txId1 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, lockKey(), LockMode.S);
        assertTrue(fut1.isDone());

        UUID txId2 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut2 = lockManager.acquire(txId2, lockKey(), LockMode.IX);

        assertThrowsWithCause(fut2::join, LockException.class);

        lockManager.releaseAll(txId1);

        fut2 = lockManager.acquire(txId2, lockKey(), LockMode.IX);
        assertTrue(fut2.isDone());

        lockManager.releaseAll(txId2);
    }

    @Test
    public void testComplex() {
        UUID txId1 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, lockKey(), LockMode.IX);
        assertTrue(fut1.isDone());

        UUID txId2 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut2 = lockManager.acquire(txId2, lockKey(), LockMode.IX);
        assertTrue(fut2.isDone());

        UUID txId3 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut3 = lockManager.acquire(txId3, lockKey(), LockMode.IX);
        assertTrue(fut3.isDone());

        UUID txId4 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut4 = lockManager.acquire(txId4, lockKey(), LockMode.S);
        assertFalse(fut4.isDone());

        UUID txId5 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut5 = lockManager.acquire(txId5, lockKey(), LockMode.S);
        assertFalse(fut5.isDone());

        lockManager.releaseAll(txId1);
        assertFalse(fut4.isDone());
        assertFalse(fut5.isDone());

        lockManager.releaseAll(txId2);
        assertFalse(fut4.isDone());
        assertFalse(fut5.isDone());

        lockManager.releaseAll(txId3);
        fut4.join();
        fut5.join();

        lockManager.releaseAll(txId4);
        lockManager.releaseAll(txId5);
    }

    @Test
    public void testComplexInverse() {
        UUID txId1 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, lockKey(), LockMode.S);
        assertTrue(fut1.isDone());

        UUID txId2 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut2 = lockManager.acquire(txId2, lockKey(), LockMode.S);
        assertTrue(fut2.isDone());

        UUID txId3 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut3 = lockManager.acquire(txId3, lockKey(), LockMode.S);
        assertTrue(fut3.isDone());

        UUID txId4 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut4 = lockManager.acquire(txId4, lockKey(), LockMode.IX);
        assertThrowsWithCause(fut4::join, LockException.class);

        UUID txId5 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut5 = lockManager.acquire(txId5, lockKey(), LockMode.IX);
        assertThrowsWithCause(fut5::join, LockException.class);

        lockManager.releaseAll(txId1);
        lockManager.releaseAll(txId2);
        lockManager.releaseAll(txId3);

        fut4 = lockManager.acquire(txId4, lockKey(), LockMode.IX);
        fut4.join();

        fut5 = lockManager.acquire(txId5, lockKey(), LockMode.IX);
        fut5.join();

        lockManager.releaseAll(txId4);
        lockManager.releaseAll(txId5);
    }

    @Test
    public void testUpgrade() {
        UUID txId1 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, lockKey(), LockMode.IX);
        assertTrue(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId1, lockKey(), LockMode.S);
        assertTrue(fut2.isDone());

        lockManager.releaseAll(txId1);
    }

    @Test
    public void testUpgradeReverse() {
        UUID txId1 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, lockKey(), LockMode.S);
        assertTrue(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId1, lockKey(), LockMode.IX);
        assertTrue(fut2.isDone());

        lockManager.releaseAll(txId1);
    }

    @Test
    public void testUpgradeMulti() {
        UUID txId1 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, lockKey(), LockMode.IX);
        assertTrue(fut1.isDone());

        UUID txId2 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut2 = lockManager.acquire(txId2, lockKey(), LockMode.IX);
        assertTrue(fut2.isDone());

        CompletableFuture<Lock> fut3 = lockManager.acquire(txId1, lockKey(), LockMode.S);
        assertThrowsWithCause(fut3::join, LockException.class);

        lockManager.releaseAll(txId1);
        lockManager.releaseAll(txId2);
    }

    @Test
    public void testUpgradeReverseMulti() {
        UUID txId1 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, lockKey(), LockMode.S);
        assertTrue(fut1.isDone());

        UUID txId2 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut2 = lockManager.acquire(txId2, lockKey(), LockMode.S);
        assertTrue(fut2.isDone());

        CompletableFuture<Lock> fut3 = lockManager.acquire(txId1, lockKey(), LockMode.IX);
        assertThrowsWithCause(fut3::join, LockException.class);

        lockManager.releaseAll(txId1);
        lockManager.releaseAll(txId2);
    }

    @Test
    public void testReenter() {
        UUID txId1 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, lockKey(), LockMode.IX);
        assertTrue(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId1, lockKey(), LockMode.IX);
        assertTrue(fut2.isDone());

        lockManager.releaseAll(txId1);
    }

    @Test
    public void testReenter2() {
        UUID txId1 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, lockKey(), LockMode.S);
        assertTrue(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId1, lockKey(), LockMode.S);
        assertTrue(fut2.isDone());

        lockManager.releaseAll(txId1);
    }

    @Test
    public void testUpgradeAndLockRequest() {
        UUID txId1 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, lockKey(), LockMode.IX);
        assertTrue(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId1, lockKey(), LockMode.S);
        assertTrue(fut2.isDone());

        UUID txId2 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut3 = lockManager.acquire(txId2, lockKey(), LockMode.S);
        assertFalse(fut3.isDone());

        lockManager.releaseAll(txId1);

        fut3.join();

        lockManager.releaseAll(txId2);
    }

    @Test
    public void testUpgradeAndLockRequestReverse() {
        UUID txId1 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, lockKey(), LockMode.S);
        assertTrue(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId1, lockKey(), LockMode.IX);
        assertTrue(fut2.isDone());

        UUID txId2 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut3 = lockManager.acquire(txId2, lockKey(), LockMode.S);
        assertFalse(fut3.isDone());

        lockManager.releaseAll(txId1);

        fut3.join();

        lockManager.releaseAll(txId2);
    }

    @Test
    public void testUpgradeAndLockRequest2() {
        UUID txId1 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, lockKey(), LockMode.IX);
        assertTrue(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId1, lockKey(), LockMode.S);
        assertTrue(fut2.isDone());

        UUID txId2 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut3 = lockManager.acquire(txId2, lockKey(), LockMode.IX);
        assertThrowsWithCause(fut3::join, LockException.class);

        lockManager.releaseAll(txId1);
    }

    @Test
    public void testUpgradeAndLockRequestReverse2() {
        UUID txId1 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut1 = lockManager.acquire(txId1, lockKey(), LockMode.S);
        assertTrue(fut1.isDone());

        CompletableFuture<Lock> fut2 = lockManager.acquire(txId1, lockKey(), LockMode.IX);
        assertTrue(fut2.isDone());

        UUID txId2 = TestTransactionIds.newTransactionId();
        CompletableFuture<Lock> fut3 = lockManager.acquire(txId2, lockKey(), LockMode.IX);
        assertThrowsWithCause(fut3::join, LockException.class);

        lockManager.releaseAll(txId1);
    }

    private static LockKey lockKey() {
        return new LockKey("test");
    }
}
