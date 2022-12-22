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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willFailFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.junit.jupiter.api.Test;

/**
 * Test for NO-WAIT deadlock prevention policy, i.e. policy that aborts any transaction that creates a lock request conflicting with
 * another transaction.
 */
public class NoWaitDeadlockPreventionTest extends AbstractLockingTest {
    private DeadlockPreventionPolicy deadlockPreventionPolicy() {
        return new DeadlockPreventionPolicy() {
            @Override
            public long waitTimeout() {
                return 0;
            }
        };
    }

    @Override
    protected LockManager lockManager() {
        return new HeapLockManager(deadlockPreventionPolicy());
    }

    @Test
    public void testNoWait() {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var key = key("test");

        CompletableFuture<Lock> tx1lock = (CompletableFuture<Lock>) xlock(tx1, key);
        assertThat(xlock(tx1, key), willSucceedFast());
        CompletableFuture<?> tx2Fut = xlock(tx2, key);
        assertTrue(tx2Fut.isCompletedExceptionally());

        lockManager.release(tx1lock.join());

        CompletableFuture<Lock> tx2lock = (CompletableFuture<Lock>) xlock(tx2, key);
        CompletableFuture<?> tx1Fut = xlock(tx1, key);
        assertTrue(tx1Fut.isCompletedExceptionally());

        lockManager.release(tx2lock.join());
    }

    @Test
    public void noWaitFail() throws InterruptedException {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var key = key("test");

        assertThat(xlock(tx1, key), willSucceedFast());
        CompletableFuture<?> tx2Fut = xlock(tx2, key);

        assertTrue(tx2Fut.isDone());
    }

    @Test
    public void noWaitFailReverseOrder() throws InterruptedException {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var key = key("test");

        assertThat(xlock(tx2, key), willSucceedFast());
        CompletableFuture<?> tx2Fut = xlock(tx1, key);

        assertTrue(tx2Fut.isDone());
    }

    @Test
    public void allowNoWaitOnDeadlockOnOne() {
        var tx0 = beginTx();
        var tx1 = beginTx();

        var key = key("test0");

        assertThat(slock(tx0, key), willSucceedFast());
        assertThat(slock(tx1, key), willSucceedFast());

        assertThat(xlock(tx0, key), willFailFast(LockException.class));
        assertThat(xlock(tx1, key), willFailFast(LockException.class));
    }

    @Test
    public void allowNoWaitOnDeadlockOnTwoKeys() {
        var tx0 = beginTx();
        var tx1 = beginTx();

        var key0 = key("test0");
        var key1 = key("test1");

        assertThat(xlock(tx0, key0), willSucceedFast());
        assertThat(xlock(tx1, key1), willSucceedFast());

        assertThat(xlock(tx0, key1), willFailFast(LockException.class));
        assertThat(xlock(tx1, key0), willFailFast(LockException.class));
    }
}
