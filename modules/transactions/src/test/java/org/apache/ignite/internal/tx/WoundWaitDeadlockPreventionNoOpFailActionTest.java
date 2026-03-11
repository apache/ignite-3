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
import static org.apache.ignite.internal.tx.test.LockWaiterMatcher.waitsFor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.tx.impl.WoundWaitDeadlockPreventionPolicy;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link WoundWaitDeadlockPreventionPolicy} with no-op fail action.
 */
public class WoundWaitDeadlockPreventionNoOpFailActionTest extends AbstractDeadlockPreventionTest {
    @Override
    protected Matcher<CompletableFuture<Lock>> conflictMatcher(UUID txId) {
        return waitsFor(txId);
    }

    @Override
    protected DeadlockPreventionPolicy deadlockPreventionPolicy() {
        return new WoundWaitDeadlockPreventionPolicy() {
            @Override
            public void failAction(UUID owner) {
                // No-op action causes wound wait to wait on conflict.
            }
        };
    }

    @Test
    @Disabled
    public void testLockOrderAfterRelease2() {
        var tx1 = beginTx();
        var tx2 = beginTx();
        var tx3 = beginTx();
        var tx4 = beginTx();

        var k = lockKey("test");

        assertThat(xlock(tx1, k), willSucceedFast());

        CompletableFuture<?> futTx2 = slock(tx2, k);
        assertFalse(futTx2.isDone());

        CompletableFuture<?> futTx3 = xlock(tx3, k);
        assertFalse(futTx3.isDone());

        CompletableFuture<?> futTx4 = slock(tx4, k);
        assertFalse(futTx4.isDone());

        commitTx(tx1);

        assertThat(futTx2, willSucceedFast());
        assertThat(futTx4, willSucceedFast());
        assertFalse(futTx3.isDone());

        commitTx(tx4);
        commitTx(tx2);

        assertThat(futTx3, willSucceedFast());
    }
}
