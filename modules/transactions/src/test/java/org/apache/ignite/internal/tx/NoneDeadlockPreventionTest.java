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
import static org.apache.ignite.internal.tx.test.LockWaiterMatcher.awaits;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

/**
 * Test for NONE deadlock prevention policy, i.e. policy that doesn't prevent any deadlocks.
 */
public class NoneDeadlockPreventionTest extends AbstractDeadlockPreventionTest {
    @Override
    protected DeadlockPreventionPolicy deadlockPreventionPolicy() {
        return DeadlockPreventionPolicy.NO_OP;
    }

    @Override
    protected Matcher<CompletableFuture<Lock>> conflictMatcher(UUID txId) {
        return awaits();
    }

    @Test
    public void allowDeadlockOnOneKey() {
        var tx0 = beginTx();
        var tx1 = beginTx();

        var key = lockKey("test0");

        assertThat(slock(tx0, key), willSucceedFast());
        assertThat(slock(tx1, key), willSucceedFast());

        assertFalse(xlock(tx0, key).isDone());
        assertFalse(xlock(tx1, key).isDone());
    }

    @Test
    public void allowDeadlockOnTwoKeys() {
        var tx0 = beginTx();
        var tx1 = beginTx();

        var key0 = lockKey("test0");
        var key1 = lockKey("test1");

        assertThat(xlock(tx0, key0), willSucceedFast());
        assertThat(xlock(tx1, key1), willSucceedFast());

        assertFalse(xlock(tx0, key1).isDone());
        assertFalse(xlock(tx1, key0).isDone());
    }
}
