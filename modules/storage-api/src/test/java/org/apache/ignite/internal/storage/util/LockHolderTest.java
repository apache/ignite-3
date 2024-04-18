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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.concurrent.locks.Lock;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * Class for testing {@link LockHolder}.
 */
public class LockHolderTest extends BaseIgniteAbstractTest {
    @Test
    void testSimple() {
        LockHolder<Lock> lockHolder = createLockHolder();

        lockHolder.incrementHolders();

        assertNotNull(lockHolder.getLock());

        assertTrue(lockHolder.decrementHolders());
    }

    @Test
    void testIncrementHoldersSameThreadTwice() {
        LockHolder<Lock> lockHolder = createLockHolder();

        lockHolder.incrementHolders();
        lockHolder.incrementHolders();

        assertNotNull(lockHolder.getLock());

        assertFalse(lockHolder.decrementHolders());
        assertTrue(lockHolder.decrementHolders());
    }

    @Test
    void testTwoThreadSimple() {
        LockHolder<Lock> lockHolder = createLockHolder();

        lockHolder.incrementHolders();

        assertNotNull(lockHolder.getLock());

        assertThat(runAsync(() -> {
            lockHolder.incrementHolders();

            assertNotNull(lockHolder.getLock());

            assertFalse(lockHolder.decrementHolders());
        }), willCompleteSuccessfully());

        assertNotNull(lockHolder.getLock());

        assertTrue(lockHolder.decrementHolders());
    }

    private static LockHolder<Lock> createLockHolder() {
        return new LockHolder<>(mock(Lock.class));
    }
}
