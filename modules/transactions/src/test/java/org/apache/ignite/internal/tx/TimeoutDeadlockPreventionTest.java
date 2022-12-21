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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.tx.LockMode.X;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.junit.jupiter.api.Test;

public class TimeoutDeadlockPreventionTest {
    private final LockManager lockManager = new HeapLockManager(new DeadlockPreventionPolicy() {
        @Override
        public long waitTimeout() {
            return 200;
        }
    });

    @Test
    public void timeoutTest() {
        UUID tx1 = Timestamp.nextVersion().toUuid();
        UUID tx2 = Timestamp.nextVersion().toUuid();

        LockKey key = new LockKey("test");

        Lock tx1lock = lockManager.acquire(tx1, key, X).join();
        CompletableFuture<?> tx2Fut = lockManager.acquire(tx2, key, X);

        assertFalse(tx2Fut.isDone());

        lockManager.release(tx1lock);

        assertThat(tx2Fut, willSucceedFast());
    }

    @Test
    public void timeoutTestReverseOrder() {
        UUID tx1 = Timestamp.nextVersion().toUuid();
        UUID tx2 = Timestamp.nextVersion().toUuid();

        LockKey key = new LockKey("test");

        Lock tx2lock = lockManager.acquire(tx2, key, X).join();
        CompletableFuture<?> tx1Fut = lockManager.acquire(tx1, key, X);

        assertFalse(tx1Fut.isDone());

        lockManager.release(tx2lock);

        assertThat(tx1Fut, willSucceedFast());
    }

    @Test
    public void timeoutFail() throws InterruptedException {
        UUID tx1 = Timestamp.nextVersion().toUuid();
        UUID tx2 = Timestamp.nextVersion().toUuid();

        LockKey key = new LockKey("test");

        Lock tx1lock = lockManager.acquire(tx1, key, X).join();
        CompletableFuture<?> tx2Fut = lockManager.acquire(tx2, key, X);

        assertFalse(tx2Fut.isDone());

        Thread.sleep(350);

        lockManager.release(tx1lock);

        assertTrue(tx2Fut.isCompletedExceptionally());
    }

    @Test
    public void timeoutFailReverseOrder() throws InterruptedException {
        UUID tx1 = Timestamp.nextVersion().toUuid();
        UUID tx2 = Timestamp.nextVersion().toUuid();

        LockKey key = new LockKey("test");

        Lock tx2lock = lockManager.acquire(tx2, key, X).join();
        CompletableFuture<?> tx1Fut = lockManager.acquire(tx1, key, X);

        assertFalse(tx1Fut.isDone());

        Thread.sleep(350);

        lockManager.release(tx2lock);

        assertTrue(tx1Fut.isCompletedExceptionally());
    }
}
