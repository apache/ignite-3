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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.tx.impl.DeadlockPreventionPolicyImpl;
import org.apache.ignite.internal.tx.impl.DeadlockPreventionPolicyImpl.TxIdComparators;
import org.junit.jupiter.api.Test;

/**
 * Test for NO-WAIT deadlock prevention policy, i.e. policy that aborts any transaction that creates a lock request conflicting with
 * another transaction.
 */
public class NoWaitDeadlockPreventionTest extends AbstractLockingTest {
    DeadlockPreventionPolicy deadlockPreventionPolicy() {
        return new DeadlockPreventionPolicyImpl(TxIdComparators.NONE, 0);
    }

    @Override
    protected LockManager lockManager() {
        return lockManager(deadlockPreventionPolicy());
    }

    @Test
    public void noWaitFail() {
        var tx1 = beginTx();
        var tx2 = beginTx();

        addTxLabel(tx1, "tx1");
        addTxLabel(tx2, "tx2");

        var key = key("test");

        for (LockMode m1 : LockMode.values()) {
            for (LockMode m2 : LockMode.values()) {
                assertThat(acquire(tx1, key, m1), willSucceedFast());
                CompletableFuture<?> tx2Fut = acquire(tx2, key, m2);

                if (m1.isCompatible(m2)) {
                    assertThat(tx2Fut, willSucceedFast());
                } else {
                    assertTrue(tx2Fut.isCompletedExceptionally());
                }

                release(tx1, key, m1);
                release(tx2, key, m2);
            }
        }
    }

    @Test
    public void noWaitFailReverseOrder() {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var key = key("test");

        for (LockMode m2 : LockMode.values()) {
            for (LockMode m1 : LockMode.values()) {
                assertThat(acquire(tx2, key, m2), willSucceedFast());
                CompletableFuture<?> tx1Fut = acquire(tx1, key, m1);

                if (m2.isCompatible(m1)) {
                    assertThat(tx1Fut, willSucceedFast());
                } else {
                    assertTrue(tx1Fut.isCompletedExceptionally());
                }

                release(tx2, key, m2);
                release(tx1, key, m1);
            }
        }
    }

    @Test
    public void allowNoWaitOnDeadlockOnOne() {
        var tx0 = beginTx();
        var tx1 = beginTx();

        // Add labels to see them in logs when exceptions occur
        addTxLabel(tx0, "tx0");
        addTxLabel(tx1, "tx1");

        var key = key("test0");

        assertThat(slock(tx0, key), willSucceedFast());
        assertThat(slock(tx1, key), willSucceedFast());

        assertThat(xlock(tx0, key), willThrowFast(LockException.class));
        assertThat(xlock(tx1, key), willThrowFast(LockException.class));
    }

    @Test
    public void allowNoWaitOnDeadlockOnTwoKeys() {
        var tx0 = beginTx();
        var tx1 = beginTx();

        addTxLabel(tx0, "tx0");
        addTxLabel(tx1, "tx1");

        var key0 = key("test0");
        var key1 = key("test1");

        assertThat(xlock(tx0, key0), willSucceedFast());
        assertThat(xlock(tx1, key1), willSucceedFast());

        assertThat(xlock(tx0, key1), willThrowFast(LockException.class));
        assertThat(xlock(tx1, key0), willThrowFast(LockException.class));
    }
}
