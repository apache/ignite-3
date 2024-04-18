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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.tx.impl.TxIdPriorityComparator;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for WOUND-WAIT deadlock prevention policy.
 */
public class ReversedDeadlockPreventionTest extends AbstractDeadlockPreventionTest {
    private long counter;

    @BeforeEach
    public void before() {
        counter = 0;
    }

    @Override
    protected UUID beginTx() {
        return beginTx(TxPriority.NORMAL);
    }

    @Override
    protected UUID beginTx(TxPriority priority) {
        counter++;
        return TransactionIds.transactionId(Long.MAX_VALUE - counter, 1, priority);
    }

    @Override
    protected DeadlockPreventionPolicy deadlockPreventionPolicy() {
        return new DeadlockPreventionPolicy() {
            @Override
            public @Nullable Comparator<UUID> txIdComparator() {
                return new TxIdPriorityComparator().reversed();
            }

            @Override
            public long waitTimeout() {
                return 0;
            }
        };
    }

    @Test
    public void youngLowTxShouldWaitForOldNormalTx() {
        var oldNormalTx = beginTx(TxPriority.NORMAL);
        var youngLowTx = beginTx(TxPriority.LOW);

        var key1 = key("test");

        assertThat(xlock(oldNormalTx, key1), willSucceedFast());

        CompletableFuture<?> youngLowXlock = xlock(youngLowTx, key1);

        commitTx(oldNormalTx);

        assertThat(youngLowXlock, willCompleteSuccessfully());
    }

    @Test
    public void youngNormalTxShouldBeAborted() {
        var tx1 = beginTx(TxPriority.LOW);
        var tx2 = beginTx(TxPriority.NORMAL);

        var key1 = key("test");

        assertThat(xlock(tx1, key1), willSucceedFast());

        assertThat(xlock(tx2, key1), willThrow(LockException.class));
    }
}
