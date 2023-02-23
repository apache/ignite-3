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

package org.apache.ignite.internal.storage.util;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willTimeoutFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.storage.RowId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Class for testing {@link ReentrantLockByRowId}.
 */
public class ReentrantLockByRowIdTest {
    private ReentrantLockByRowId lockByRowId;

    @BeforeEach
    void setUp() {
        lockByRowId = new ReentrantLockByRowId();
    }

    @AfterEach
    void tearDown() {
        lockByRowId.releaseAllLockByCurrentThread();
    }

    @Test
    void testSimple() {
        RowId rowId = new RowId(0);

        lockByRowId.acquireLock(rowId);
        lockByRowId.releaseLock(rowId);

        assertEquals(1, lockByRowId.inLock(rowId, () -> 1));
    }

    @Test
    void testSimpleReEnter() {
        RowId rowId = new RowId(0);

        lockByRowId.acquireLock(rowId);
        lockByRowId.acquireLock(rowId);

        lockByRowId.inLock(rowId, () -> {
            lockByRowId.acquireLock(rowId);

            lockByRowId.releaseLock(rowId);

            return null;
        });

        lockByRowId.releaseLock(rowId);
        lockByRowId.releaseLock(rowId);
    }

    @Test
    void testReleaseError() {
        assertThrows(IllegalStateException.class, () -> lockByRowId.releaseLock(new RowId(0)));

        RowId rowId = new RowId(0);

        assertThat(runAsync(() -> lockByRowId.acquireLock(rowId)), willCompleteSuccessfully());

        assertThrows(IllegalMonitorStateException.class, () -> lockByRowId.releaseLock(rowId));
    }

    @Test
    void testBlockSimple() {
        RowId rowId = new RowId(0);

        lockByRowId.acquireLock(rowId);
        lockByRowId.acquireLock(rowId);

        CompletableFuture<?> acquireLockFuture = runAsync(() -> {
            lockByRowId.acquireLock(rowId);
            lockByRowId.releaseLock(rowId);
        });

        assertThat(acquireLockFuture, willTimeoutFast());

        lockByRowId.releaseLock(rowId);

        assertThat(acquireLockFuture, willTimeoutFast());

        lockByRowId.releaseLock(rowId);

        assertThat(acquireLockFuture, willCompleteSuccessfully());

        lockByRowId.acquireLock(rowId);
    }

    @Test
    void testBlockSupplier() {
        RowId rowId = new RowId(0);

        lockByRowId.acquireLock(rowId);
        lockByRowId.acquireLock(rowId);

        CompletableFuture<?> acquireLockFuture = runAsync(() -> lockByRowId.inLock(rowId, () -> 1));

        assertThat(acquireLockFuture, willTimeoutFast());

        lockByRowId.releaseLock(rowId);

        assertThat(acquireLockFuture, willTimeoutFast());

        lockByRowId.releaseLock(rowId);

        assertThat(acquireLockFuture, willCompleteSuccessfully());

        lockByRowId.acquireLock(rowId);
    }

    @Test
    void testReleaseAllLocksByCurrentThread() {
        RowId rowId0 = new RowId(0);
        RowId rowId1 = new RowId(0);

        lockByRowId.acquireLock(rowId0);

        lockByRowId.acquireLock(rowId1);
        lockByRowId.acquireLock(rowId1);

        CompletableFuture<?> acquireLockFuture = runAsync(() -> {
            lockByRowId.acquireLock(rowId0);
            lockByRowId.acquireLock(rowId1);

            lockByRowId.releaseAllLockByCurrentThread();
        });

        assertThat(acquireLockFuture, willTimeoutFast());

        lockByRowId.releaseAllLockByCurrentThread();

        assertThat(acquireLockFuture, willCompleteSuccessfully());
    }
}
