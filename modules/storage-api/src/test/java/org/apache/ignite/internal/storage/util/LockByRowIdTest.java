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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.ignite.internal.storage.RowId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Class for testing {@link LockByRowId}.
 */
public class LockByRowIdTest {
    private LockByRowId lockByRowId;

    @BeforeEach
    void setUp() {
        lockByRowId = new LockByRowId();
    }

    @Test
    void testSimple() {
        RowId rowId = new RowId(0);

        lockByRowId.lock(rowId);
        lockByRowId.unlockAll(rowId);

        assertEquals(1, inLock(rowId, () -> 1));
    }

    @Test
    void testSimpleReEnter() {
        RowId rowId = new RowId(0);

        lockByRowId.lock(rowId);
        lockByRowId.lock(rowId);

        lockByRowId.unlockAll(rowId);
    }

    @Test
    void testReleaseError() {
        assertThrows(IllegalStateException.class, () -> lockByRowId.unlockAll(new RowId(0)));

        RowId rowId = new RowId(0);

        assertThat(runAsync(() -> lockByRowId.lock(rowId)), willCompleteSuccessfully());

        assertThrows(IllegalMonitorStateException.class, () -> lockByRowId.unlockAll(rowId));
    }

    @Test
    void testBlockSimple() {
        RowId rowId = new RowId(0);

        lockByRowId.lock(rowId);
        lockByRowId.lock(rowId);

        CompletableFuture<?> acquireLockFuture = runAsync(() -> {
            lockByRowId.lock(rowId);
            lockByRowId.unlockAll(rowId);
        });

        assertThat(acquireLockFuture, willTimeoutFast());

        lockByRowId.unlockAll(rowId);

        assertThat(acquireLockFuture, willCompleteSuccessfully());
    }

    private <T> T inLock(RowId rowId, Supplier<T> action) {
        lockByRowId.lock(rowId);

        try {
            return action.get();
        } finally {
            lockByRowId.unlockAll(rowId);
        }
    }
}
