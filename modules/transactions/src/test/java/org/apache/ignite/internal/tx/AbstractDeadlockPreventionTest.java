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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * Abstract class containing some tests for deadlock prevention that check common scenarios for different policies.
 */
public abstract class AbstractDeadlockPreventionTest extends AbstractLockingTest {
    protected abstract DeadlockPreventionPolicy deadlockPreventionPolicy();

    @Override
    protected LockManager lockManager() {
        return lockManager(deadlockPreventionPolicy());
    }

    @Test
    public void testSimpleConflict0() {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var key1 = key("test");

        assertThat(xlock(tx1, key1), willCompleteSuccessfully());

        assertFutureFailsOrWaitsForTimeout(() -> xlock(tx2, key1));
    }

    @Test
    public void testSimpleConflict1() {
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
    public void testSimpleConflictSlocks1() {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var key1 = key("test");

        assertThat(slock(tx1, key1), willSucceedFast());
        assertThat(slock(tx2, key1), willSucceedFast());

        assertFutureFailsOrWaitsForTimeout(() -> xlock(tx2, key1));
    }

    @Test
    public void testSimpleConflictSlocks2() {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var key1 = key("test");

        assertThat(slock(tx1, key1), willSucceedFast());
        assertThat(slock(tx2, key1), willSucceedFast());

        CompletableFuture<?> xlockTx1 = xlock(tx1, key1);
        assertFalse(xlockTx1.isDone());

        CompletableFuture<?> xlockTx2 = xlock(tx2, key1);

        assertFutureFailsOrWaitsForTimeout(() -> xlockTx2);

        if (xlockTx2.isDone()) {
            rollbackTx(tx2);

            assertThat(xlockTx1, willSucceedFast());
        }
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

        assertThat(futTx1, willSucceedFast());

        assertFutureFailsOrWaitsForTimeout(() -> futTx2);
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

        assertFutureFailsOrWaitsForTimeout(() -> xlock(tx2, k));
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

        assertFutureFailsOrWaitsForTimeout(() -> futTx2);
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

    @Test
    public void testIncompatibleLockRetry() {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var k = key("test");

        assertThat(slock(tx1, k), willSucceedFast());
        assertThat(slock(tx2, k), willSucceedFast());

        assertFutureFailsOrWaitsForTimeout(() -> xlock(tx2, k));

        commitTx(tx1);

        assertThat(xlock(tx2, k), willSucceedFast());
    }

    @Test
    public void testDeadlockRecovery() {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var k = key("test");

        assertThat(slock(tx2, k), willSucceedFast());
        assertThat(slock(tx1, k), willSucceedFast());

        assertFutureFailsOrWaitsForTimeout(() ->  xlock(tx2, k));

        // Failed lock will be rolled to S state. We need to release it manually.
        release(tx2, k, LockMode.S);

        assertThat(xlock(tx1, k), willSucceedFast());
    }

    /**
     * This method checks lock future of conflicting transaction provided by supplier, in a way depending on deadlock prevention policy.
     * If the policy does not allow wait on conflict (wait timeout is equal to {@code 0}) then the future must be failed with
     * {@link LockException}. Otherwise, it must be not completed. This method is only suitable for checking lock futures of lower priority
     * transactions, if transaction priority is applicable.
     *
     * @param s Supplier of lock future.
     */
    protected void assertFutureFailsOrWaitsForTimeout(Supplier<CompletableFuture<?>> s) {
        try {
            CompletableFuture<?> f = s.get();

            if (deadlockPreventionPolicy().waitTimeout() == 0) {
                assertTrue(f.isDone());
                f.join();

                fail();
            } else {
                assertFalse(f.isDone());
            }
        } catch (Exception e) {
            if (!hasCause(e, LockException.class, null)) {
                fail();
            }
        }
    }
}
