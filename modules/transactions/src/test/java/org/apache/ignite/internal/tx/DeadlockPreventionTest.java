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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.hasCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.LockMode.S;
import static org.apache.ignite.internal.tx.LockMode.X;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.junit.jupiter.api.Test;

/**
 * Tests for deadlock prevention scenarios.
 */
public class DeadlockPreventionTest {
    private LockManager lockManager = new HeapLockManager();
    private Map<UUID, List<CompletableFuture<Lock>>> locks = new HashMap<>();

    @Test
    public void testWaitDie0() {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var key1 = key("test");

        assertThat(xlock(tx1, key1), willCompleteSuccessfully());

        assertThrowsLockException(() -> xlock(tx2, key1));
    }

    @Test
    public void testWaitDie1() {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var key1 = key("test");

        assertThat(xlock(tx2, key1), willSucceedFast());

        CompletableFuture<?> xlockFutTx1 = xlock(tx1, key1);
        assertFalse(xlockFutTx1.isDone());

        commitTx(tx2);
        assertThat(xlockFutTx1, willSucceedFast());
    }

    @Test
    public void testWaitDieSlocks1() {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var key1 = key("test");

        assertThat(slock(tx1, key1), willSucceedFast());
        assertThat(slock(tx2, key1), willSucceedFast());

        assertThrowsLockException(() -> xlock(tx2, key1));
    }

    @Test
    public void testWaitDieSlocks2() {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var key1 = key("test");

        assertThat(slock(tx1, key1), willSucceedFast());
        assertThat(slock(tx2, key1), willSucceedFast());

        CompletableFuture<?> xlockTx1 = xlock(tx1, key1);
        assertFalse(xlockTx1.isDone());

        assertThrowsLockException(() -> xlock(tx2, key1));

        rollbackTx(tx2);

        assertThat(xlockTx1, willSucceedFast());
    }

    @Test
    public void testNonFair() {
        var tx1 = beginTx();
        var tx2 = beginTx();
        var tx3 = beginTx();

        var k = key("test");

        assertThat(slock(tx3, k), willSucceedFast());

        CompletableFuture<?> futTx2 = xlock(tx2, k);
        assertFalse(futTx2.isDone());

        CompletableFuture<?> futTx1 = xlock(tx1, k);
        assertFalse(futTx1.isDone());

        commitTx(tx3);

        // TODO correctness
        assertThat(futTx1, willSucceedFast());

        assertThrowsLockException(() -> futTx2);
    }

    @Test
    public void testReenterWithConflict() {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var k = key("test");

        assertThat(slock(tx2, k), willSucceedFast());
        assertThat(slock(tx1, k), willSucceedFast());

        CompletableFuture<?> futTx1 = xlock(tx1, k);
        assertFalse(futTx1.isDone());

        commitTx(tx2);

        assertThat(futTx1, willSucceedFast());
    }

    @Test
    public void testReenterWithConflictAndAbort() {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var k = key("test");

        assertThat(slock(tx2, k), willSucceedFast());
        assertThat(slock(tx1, k), willSucceedFast());

        assertThrowsLockException(() -> xlock(tx2, k));
    }

    @Test
    public void testReenterAllowed() {
        var tx1 = beginTx();

        var k = key("test");

        assertThat(slock(tx1, k), willSucceedFast());
        assertThat(xlock(tx1, k), willSucceedFast());
    }

    @Test
    public void testNonFairConflictWithAlreadyWaiting() {
        var tx1 = beginTx();
        var tx2 = beginTx();
        var tx3 = beginTx();

        var k = key("test");

        assertThat(slock(tx2, k), willSucceedFast());

        CompletableFuture<?> futTx1 = xlock(tx1, k);
        assertFalse(futTx1.isDone());

        assertThat(slock(tx3, k), willSucceedFast());

        assertFalse(futTx1.isDone());
    }

    @Test
    public void testNonFairConflictWithAlreadyWaitingWithAbort() {
        var tx1 = beginTx();
        var tx2 = beginTx();
        var tx3 = beginTx();

        var k = key("test");

        assertThat(slock(tx3, k), willSucceedFast());

        CompletableFuture<?> futTx2 = xlock(tx2, k);
        assertFalse(futTx2.isDone());

        assertThat(slock(tx1, k), willSucceedFast());

        commitTx(tx3);

        assertThrowsLockException(() -> futTx2);
    }

    @Test
    public void testNonFairTakeFirstCompatible() {
        var tx1 = beginTx();
        var tx2 = beginTx();
        var tx3 = beginTx();
        var tx4 = beginTx();

        var k = key("test");

        assertThat(slock(tx4, k), willSucceedFast());

        CompletableFuture<?> futTx2 = xlock(tx2, k);
        assertFalse(futTx2.isDone());

        assertThat(slock(tx1, k), willSucceedFast());
        assertThat(slock(tx3, k), willSucceedFast());

        assertFalse(futTx2.isDone());

        commitTx(tx1);
        commitTx(tx3);
        commitTx(tx4);

        futTx2.join();
    }

    @Test
    public void testLockOrderAfterRelease() {
        var tx1 = beginTx();
        var tx2 = beginTx();
        var tx3 = beginTx();
        var tx4 = beginTx();

        var k = key("test");

        assertThat(xlock(tx4, k), willSucceedFast());

        CompletableFuture<?> futTx3 = slock(tx3, k);
        assertFalse(futTx3.isDone());

        CompletableFuture<?> futTx2 = xlock(tx2, k);
        assertFalse(futTx2.isDone());

        CompletableFuture<?> futTx1 = slock(tx1, k);
        assertFalse(futTx1.isDone());

        commitTx(tx4);

        assertThat(futTx3, willSucceedFast());
        assertThat(futTx1, willSucceedFast());
        assertFalse(futTx2.isDone());

        commitTx(tx1);
        commitTx(tx3);

        assertThat(futTx2, willSucceedFast());
    }

    @Test
    public void testMultipleCompatibleLocksAcquiredAfterIncompatibleReleased() {
        var tx1 = beginTx();
        var tx2 = beginTx();
        var tx3 = beginTx();

        var k = key("test");

        assertThat(xlock(tx3, k), willSucceedFast());

        CompletableFuture<?> futTx2 = slock(tx2, k);
        assertFalse(futTx2.isDone());

        CompletableFuture<?> futTx1 = slock(tx1, k);
        assertFalse(futTx1.isDone());

        commitTx(tx3);

        assertThat(futTx2, willSucceedFast());
        assertThat(futTx1, willSucceedFast());
    }

    private UUID beginTx() {
        return Timestamp.nextVersion().toUuid();
    }

    private LockKey key(Object key) {
        ByteBuffer b = ByteBuffer.allocate(Integer.BYTES);
        b.putInt(key.hashCode());

        return new LockKey(b);
    }

    private CompletableFuture<?> xlock(UUID tx, LockKey key) {
        return acquire(tx, key, X);
    }

    private CompletableFuture<?> slock(UUID tx, LockKey key) {
        return acquire(tx, key, S);
    }

    private CompletableFuture<?> acquire(UUID tx, LockKey key, LockMode mode) {
        CompletableFuture<Lock> fut = lockManager.acquire(tx, key, mode);

        locks.compute(tx, (k, v) -> {
            if (v == null) {
                v = new ArrayList<>();
            }

            v.add(fut);

            return v;
        });

        return fut;
    }

    private void commitTx(UUID tx) {
        finishTx(tx);
    }

    private void rollbackTx(UUID tx) {
        finishTx(tx);
    }

    private void finishTx(UUID tx) {
        List<CompletableFuture<Lock>> txLocks = locks.remove(tx);
        assertNotNull(txLocks);

        for (CompletableFuture<Lock> fut : txLocks) {
            assertTrue(fut.isDone());

            if (!fut.isCompletedExceptionally()) {
                Lock lock = fut.join();

                lockManager.release(lock);
            }
        }
    }

    private static void assertCompletedExceptionally(CompletableFuture<?> fut) {
        assertTrue(fut.isDone());
        assertThrows(CompletionException.class, fut::join);
    }

    private static void assertThrowsLockException(Supplier<CompletableFuture<?>> s) {
        try {
            CompletableFuture<?> f = s.get();

            assertTrue(f.isDone());
            f.join();

            fail();
        } catch (Exception e) {
            if (!hasCause(e, LockException.class, null)) {
                fail();
            }
        }
    }
}
