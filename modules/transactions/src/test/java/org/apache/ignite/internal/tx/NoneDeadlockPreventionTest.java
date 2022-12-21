/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.tx;

import static org.apache.ignite.internal.tx.LockMode.X;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.junit.jupiter.api.Test;

public class NoneDeadlockPreventionTest {
    private final LockManager lockManager = new HeapLockManager(new DeadlockPreventionPolicy() { });

    @Test
    public void testNoWait() {
        UUID tx1 = Timestamp.nextVersion().toUuid();
        UUID tx2 = Timestamp.nextVersion().toUuid();

        LockKey key = new LockKey("test");

        Lock tx1lock = lockManager.acquire(tx1, key, X).join();
        CompletableFuture<?> tx2Fut = lockManager.acquire(tx2, key, X);
        assertTrue(tx2Fut.isCompletedExceptionally());

        lockManager.release(tx1lock);

        Lock tx2Lock = lockManager.acquire(tx2, key, X).join();
        CompletableFuture<?> tx1Fut = lockManager.acquire(tx1, key, X);
        assertTrue(tx1Fut.isCompletedExceptionally());

        lockManager.release(tx2Lock);
    }
}
