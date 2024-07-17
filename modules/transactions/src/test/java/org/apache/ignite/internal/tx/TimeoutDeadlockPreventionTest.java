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
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * Test for the timeout deadlock prevention policy, i.e. policy working in the same way as NO-WAIT but with timeout.
 */
public class TimeoutDeadlockPreventionTest extends AbstractDeadlockPreventionTest {
    @Override
    protected DeadlockPreventionPolicy deadlockPreventionPolicy() {
        return new DeadlockPreventionPolicy() {
            @Override
            public long waitTimeout() {
                return 200;
            }
        };
    }

    @Test
    public void timeoutTest() {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var key = key("test");

        assertThat(xlock(tx1, key), willSucceedFast());
        CompletableFuture<?> tx2Fut = xlock(tx2, key);

        assertFalse(tx2Fut.isDone());

        commitTx(tx1);

        assertThat(tx2Fut, willSucceedFast());
    }

    @Test
    public void timeoutTestReverseOrder() {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var key = key("test");

        assertThat(xlock(tx2, key), willSucceedFast());
        CompletableFuture<?> tx2Fut = xlock(tx1, key);

        assertFalse(tx2Fut.isDone());

        commitTx(tx2);

        assertThat(tx2Fut, willSucceedFast());
    }

    @Test
    public void timeoutFail() throws InterruptedException {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var key = key("test");

        assertThat(xlock(tx1, key), willSucceedFast());
        CompletableFuture<?> tx2Fut = xlock(tx2, key);

        assertFalse(tx2Fut.isDone());

        Thread.sleep(1000);

        commitTx(tx1);

        assertThat(tx2Fut, willThrowFast(LockException.class));
    }

    @Test
    public void timeoutFailReverseOrder() throws InterruptedException {
        var tx1 = beginTx();
        var tx2 = beginTx();

        var key = key("test");

        assertThat(xlock(tx2, key), willSucceedFast());
        CompletableFuture<?> tx2Fut = xlock(tx1, key);

        assertFalse(tx2Fut.isDone());

        Thread.sleep(1000);

        commitTx(tx2);

        assertThat(tx2Fut, willThrowFast(LockException.class));
    }

    @Test
    public void allowDeadlockOnOneKeyWithTimeout() {
        var tx0 = beginTx();
        var tx1 = beginTx();

        var key = key("test0");

        assertThat(slock(tx0, key), willSucceedFast());
        assertThat(slock(tx1, key), willSucceedFast());

        assertThat(xlock(tx0, key), willThrowFast(LockException.class));
        assertThat(xlock(tx1, key), willThrowFast(LockException.class));
    }

    @Test
    public void allowDeadlockOnTwoKeysWithTimeout() {
        var tx0 = beginTx();
        var tx1 = beginTx();

        var key0 = key("test0");
        var key1 = key("test1");

        assertThat(xlock(tx0, key0), willSucceedFast());
        assertThat(xlock(tx1, key1), willSucceedFast());

        assertThat(xlock(tx0, key1), willThrowFast(LockException.class));
        assertThat(xlock(tx1, key0), willThrowFast(LockException.class));
    }
}
