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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.ensureFutureNotCompleted;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.hasCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.tx.LockMode.X;
import static org.apache.ignite.internal.tx.test.LockConflictMatcher.conflictsWith;
import static org.apache.ignite.internal.tx.test.LockFutureMatcher.isGranted;
import static org.apache.ignite.internal.tx.test.LockWaiterMatcher.waitsFor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

/**
 * Abstract class containing some tests for deadlock prevention that check common scenarios for different policies.
 * TODO move all single keys tests to heap lm test as they cant produce deadlock.
 */
public abstract class AbstractDeadlockPreventionTest extends AbstractLockingTest {
    protected abstract Matcher<CompletableFuture<Lock>> conflictMatcher(UUID txId);

    @Test
    public void testSimpleConflict0() {
        var tx1 = tx1();
        var tx2 = tx2();

        var key = lockKey("test");

        assertThat(xlock(tx1, key), isGranted(key, X, tx1));
        assertThat(xlock(tx2, key), conflictMatcher(tx1));
    }

    @Test
    public void testSimpleWait0() {
        var tx1 = tx1();
        var tx2 = tx2();

        var key = lockKey("test");

        assertThat(xlock(tx2, key), isGranted(key, X, tx2));
        assertThat(xlock(tx1, key), waitsFor(tx2));
    }

    @Test
    public void testSimpleConflict1() {
        var tx1 = tx1();
        var tx2 = tx2();

        var key1 = lockKey("test");

        assertThat(xlock(tx2, key1), willSucceedFast());

        var xlockFutTx1 = xlock(tx1, key1);
        assertThat(xlockFutTx1, waitsFor(tx2));

        commitTx(tx2);
        assertThat(xlockFutTx1, willSucceedFast());
    }

    @Test
    public void testSimpleConflictSlocks1() {
        var tx1 = tx1();
        var tx2 = tx2();

        var key = lockKey("test");

        assertThat(slock(tx1, key), willSucceedFast());
        assertThat(slock(tx2, key), willSucceedFast());

        assertThat(xlock(tx2, key), conflictMatcher(tx1));
    }

    @Test
    public void testSimpleConflictSlocks2() {
        var tx1 = tx1();
        var tx2 = tx2();

        var key1 = lockKey("test");

        assertThat(slock(tx1, key1), willSucceedFast());
        assertThat(slock(tx2, key1), willSucceedFast());

        var xlockTx1 = xlock(tx1, key1);
        assertThat(xlockTx1, waitsFor(tx2));

        var xlockTx2 = xlock(tx2, key1);
        assertThat(xlockTx2, conflictMatcher(tx1));

        if (xlockTx2.isDone()) {
            rollbackTx(tx2);

            assertThat(xlockTx1, willSucceedFast());
        }
    }

    @Test
    public void testNonFair() {
        var tx1 = tx1();
        var tx2 = tx2();
        var tx3 = tx3();

        var k = lockKey("test");

        assertThat(slock(tx3, k), willSucceedFast());

        var futTx2 = xlock(tx2, k);
        assertThat(futTx2, waitsFor(tx3));

        var futTx1 = xlock(tx1, k);
        assertThat(futTx1, waitsFor(tx3));

        commitTx(tx3);

        // An oldest txn should be locked first.
        if (tx2.compareTo(tx1) < 0) {
            assertThat(futTx2, willSucceedFast());
            assertThat(futTx1, conflictMatcher(tx2));
        } else {
            assertThat(futTx1, willSucceedFast());
            assertThat(futTx2, conflictMatcher(tx1));
        }

    }

    @Test
    public void testReenterWithConflict() {
        var tx1 = tx1();
        var tx2 = tx2();

        var k = lockKey("test");

        assertThat(slock(tx2, k), willSucceedFast());
        assertThat(slock(tx1, k), willSucceedFast());

        var futTx1 = xlock(tx1, k);
        assertThat(futTx1, waitsFor(tx2));

        commitTx(tx2);

        assertThat(futTx1, willSucceedFast());
    }

    @Test
    public void testReenterWithConflictAndAbort() {
        var tx1 = tx1();
        var tx2 = tx2();

        var k = lockKey("test");

        assertThat(slock(tx2, k), willSucceedFast());
        assertThat(slock(tx1, k), willSucceedFast());

        assertThat(xlock(tx2, k), conflictMatcher(tx1));
    }

    @Test
    public void testReenterAllowed() {
        var tx1 = tx1();

        var k = lockKey("test");

        assertThat(slock(tx1, k), willSucceedFast());
        assertThat(xlock(tx1, k), willSucceedFast());
    }

    @Test
    public void testNonFairConflictWithAlreadyWaiting() {
        var tx1 = tx1();
        var tx2 = tx2();
        var tx3 = tx3();

        var k = lockKey("test");

        assertThat(slock(tx2, k), willSucceedFast());

        var futTx1 = xlock(tx1, k);
        assertThat(futTx1, waitsFor(tx2));

        assertThat(slock(tx3, k), willSucceedFast());

        assertFalse(futTx1.isDone());
    }

    @Test
    public void testNonFairConflictWithAlreadyWaitingWithAbort() {
        var tx1 = tx1();
        var tx2 = tx2();
        var tx3 = tx3();

        var k = lockKey("test");

        assertThat(slock(tx3, k), willSucceedFast());

        var futTx2 = xlock(tx2, k);
        assertThat(futTx2, waitsFor(tx3));

        assertThat(slock(tx1, k), willSucceedFast());

        commitTx(tx3);

        assertThat(futTx2, conflictMatcher(tx1));
    }

    @Test
    public void testNonFairTakeFirstCompatible() {
        var tx1 = tx1();
        var tx2 = tx2();
        var tx3 = tx3();
        var tx4 = tx4();

        var k = lockKey("test");

        assertThat(slock(tx4, k), willSucceedFast());

        var futTx2 = xlock(tx2, k);
        assertThat(futTx2, waitsFor(tx4));

        assertThat(slock(tx1, k), willSucceedFast());
        assertThat(slock(tx3, k), willSucceedFast());

        assertThat(futTx2, waitsFor(tx4));

        commitTx(tx1);
        commitTx(tx3);
        commitTx(tx4);

        assertThat(futTx2, willSucceedFast());
    }

    @Test
    public void testLockOrderAfterRelease() {
        var tx1 = tx1();
        var tx2 = tx2();
        var tx3 = tx3();
        var tx4 = tx4();

        var k = lockKey("test");

        assertThat(xlock(tx4, k), willSucceedFast());

        var futTx3 = slock(tx3, k);
        assertThat(futTx3, waitsFor(tx4));

        var futTx2 = xlock(tx2, k);
        assertThat(futTx2, waitsFor(tx4));

        var futTx1 = slock(tx1, k);
        assertThat(futTx1, waitsFor(tx4));

        commitTx(tx4);

        assertThat(futTx3, willSucceedFast());
        assertThat(futTx1, willSucceedFast());
        assertThat(futTx2, waitsFor(tx4));

        commitTx(tx1);
        commitTx(tx3);

        assertThat(futTx2, willSucceedFast());
    }

    @Test
    public void testMultipleCompatibleLocksAcquiredAfterIncompatibleReleased() {
        var tx1 = tx1();
        var tx2 = tx2();
        var tx3 = tx3();

        var k = lockKey("test");

        assertThat(xlock(tx3, k), willSucceedFast());

        var futTx2 = slock(tx2, k);
        assertThat(futTx2, waitsFor(tx3));

        var futTx1 = slock(tx1, k);
        assertThat(futTx1, waitsFor(tx3));

        commitTx(tx3);

        assertThat(futTx2, willSucceedFast());
        assertThat(futTx1, willSucceedFast());
    }

    @Test
    public void testIncompatibleLockRetry() {
        var tx1 = tx1();
        var tx2 = tx2();

        var k = lockKey("test");

        assertThat(slock(tx1, k), willSucceedFast());
        assertThat(slock(tx2, k), willSucceedFast());
        assertThat(xlock(tx2, k), conflictMatcher(tx1));

        commitTx(tx1);

        assertThat(xlock(tx2, k), willSucceedFast());
    }

    @Test
    public void testDeadlockRecovery() {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var k = lockKey("test");

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
                ensureFutureNotCompleted(f, 25);
            }
        } catch (Exception e) {
            if (!hasCause(e, LockException.class, null)) {
                fail();
            }
        }
    }
}
