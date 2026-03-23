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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.tx.impl.WoundWaitDeadlockPreventionPolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link WoundWaitDeadlockPreventionPolicy} with rollback fail action.
 */
public class WoundWaitDeadlockPreventionRollbackFailActionTest extends AbstractLockingTest {
    @Override
    protected DeadlockPreventionPolicy deadlockPreventionPolicy() {
        return new WoundWaitDeadlockPreventionPolicy() {
            @Override
            public void failAction(UUID owner) {
                rollbackTx(owner);
            }
        };
    }

    @Test
    public void testInvalidate() {
        var tx1 = beginTx();
        var tx2 = beginTx();
        var tx3 = beginTx();

        var k = lockKey("test");

        assertThat(slock(tx2, k), willSucceedFast());
        assertThat(slock(tx3, k), willSucceedFast());

        // Should invalidate younger owners.
        assertThat(xlock(tx1, k), willSucceedFast());
    }

    @Test
    public void testInvalidate2() {
        var tx1 = beginTx();
        var tx2 = beginTx();
        var tx3 = beginTx();

        var k = lockKey("test");

        assertThat(slock(tx1, k), willSucceedFast());
        assertThat(slock(tx2, k), willSucceedFast());
        assertThat(slock(tx3, k), willSucceedFast());

        // Should invalidate younger owners.
        assertThat(xlock(tx1, k), willSucceedFast());
    }
}
