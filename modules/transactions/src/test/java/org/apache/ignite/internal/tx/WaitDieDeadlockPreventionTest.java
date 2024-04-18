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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link WaitDieDeadlockPreventionPolicy}.
 */
public class WaitDieDeadlockPreventionTest extends AbstractDeadlockPreventionTest {
    @Override
    protected DeadlockPreventionPolicy deadlockPreventionPolicy() {
        return new WaitDieDeadlockPreventionPolicy();
    }

    @Test
    public void youngNormalTxShouldWaitForOldLowTx() {
        var oldLowTx = beginTx(TxPriority.LOW);
        var youngNormalTx = beginTx(TxPriority.NORMAL);

        var key1 = key("test");

        assertThat(xlock(oldLowTx, key1), willSucceedFast());

        CompletableFuture<?> youngNormalXlock = xlock(youngNormalTx, key1);

        commitTx(oldLowTx);

        assertThat(youngNormalXlock, willCompleteSuccessfully());
    }

    @Test
    public void youngLowTxShouldBeAborted() {
        var oldNormalTx = beginTx(TxPriority.NORMAL);
        var youngLowTx = beginTx(TxPriority.LOW);

        var key1 = key("test");

        assertThat(xlock(oldNormalTx, key1), willSucceedFast());

        assertThat(xlock(youngLowTx, key1), willThrow(LockException.class));
    }

    @Test
    public void youngSamePriorityTxShouldBeAborted() {
        var oldNormalTx = beginTx(TxPriority.NORMAL);
        var youngNormalTx = beginTx(TxPriority.NORMAL);

        var key1 = key("test");

        assertThat(xlock(oldNormalTx, key1), willSucceedFast());

        assertThat(xlock(youngNormalTx, key1), willThrow(LockException.class));
    }
}
