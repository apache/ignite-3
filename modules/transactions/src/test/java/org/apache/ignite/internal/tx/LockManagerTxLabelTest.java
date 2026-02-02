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
import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.tx.impl.DeadlockPreventionPolicyImpl;
import org.apache.ignite.internal.tx.impl.DeadlockPreventionPolicyImpl.TxIdComparators;
import org.junit.jupiter.api.Test;

/**
 * Dedicated tests for tx label formatting in lock-manager exception messages.
 */
public class LockManagerTxLabelTest extends AbstractLockingTest {
    @Override
    protected LockManager lockManager() {
        // NO-WAIT: conflicting lock requests fail fast with a deadlock-prevention exception.
        return lockManager(new DeadlockPreventionPolicyImpl(TxIdComparators.NONE, 0));
    }

    @Test
    void txLabelsArePresentInPossibleDeadlockExceptionMessage() {
        UUID lockHolderTx = beginTx();
        UUID failedToAcquireTx = beginTx();

        String lockHolderLabel = "lock-holder";
        String failedToAcquireLabel = "failed-to-acquire";

        txStateVolatileStorage.updateMeta(lockHolderTx, old -> TxStateMeta.builder(PENDING).txLabel(lockHolderLabel).build());
        txStateVolatileStorage.updateMeta(failedToAcquireTx, old -> TxStateMeta.builder(PENDING).txLabel(failedToAcquireLabel).build());

        LockKey key = key("test");

        assertThat(xlock(lockHolderTx, key), willSucceedFast());

        CompletableFuture<?> failedFuture = xlock(failedToAcquireTx, key);

        assertThat(failedFuture, willThrowFast(LockException.class, "txLabel=" + failedToAcquireLabel));
        assertThat(failedFuture, willThrowFast(LockException.class, "txLabel=" + lockHolderLabel));
    }

    @Test
    void emptyTxLabelsAreNotPrintedInPossibleDeadlockExceptionMessage() {
        UUID lockHolderTx = beginTx();
        UUID failedToAcquireTx = beginTx();

        txStateVolatileStorage.updateMeta(lockHolderTx, old -> TxStateMeta.builder(PENDING).txLabel("").build());
        txStateVolatileStorage.updateMeta(failedToAcquireTx, old -> TxStateMeta.builder(PENDING).txLabel("").build());

        LockKey key = key("test");

        assertThat(xlock(lockHolderTx, key), willSucceedFast());

        CompletableFuture<?> failedFuture = xlock(failedToAcquireTx, key);

        assertThat(failedFuture, willThrowFast(LockException.class));

        try {
            failedFuture.join();
            fail("Expected lock acquisition to fail.");
        } catch (Exception e) {
            Throwable unwrapped = unwrapCause(e);
            assertTrue(unwrapped instanceof LockException, "Unexpected exception: " + unwrapped);

            String msg = unwrapped.getMessage();

            assertFalse(msg.contains("txLabel="), msg);
            assertTrue(msg.contains(lockHolderTx.toString()), msg);
            assertTrue(msg.contains(failedToAcquireTx.toString()), msg);
        }
    }
}
