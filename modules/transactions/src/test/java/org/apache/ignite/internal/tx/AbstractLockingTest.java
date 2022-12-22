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

import static org.apache.ignite.internal.tx.LockMode.S;
import static org.apache.ignite.internal.tx.LockMode.X;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractLockingTest {
    protected final LockManager lockManager = lockManager();
    private Map<UUID, List<CompletableFuture<Lock>>> locks = new HashMap<>();

    protected abstract LockManager lockManager();

    protected UUID beginTx() {
        return Timestamp.nextVersion().toUuid();
    }

    protected LockKey key(Object key) {
        ByteBuffer b = ByteBuffer.allocate(Integer.BYTES);
        b.putInt(key.hashCode());
        b.position(0);

        return new LockKey(b);
    }

    protected CompletableFuture<?> xlock(UUID tx, LockKey key) {
        return acquire(tx, key, X);
    }

    protected CompletableFuture<?> slock(UUID tx, LockKey key) {
        return acquire(tx, key, S);
    }

    protected CompletableFuture<?> acquire(UUID tx, LockKey key, LockMode mode) {
        CompletableFuture<Lock> fut = lockManager.acquire(tx, key, mode);

        locks.compute(tx, (k, v) -> {
            if (v == null) {
                v = new ArrayList<>();
            }

            v.add(fut);

            return v;
        });

        return fut;
    }

    protected void commitTx(UUID tx) {
        finishTx(tx);
    }

    protected void rollbackTx(UUID tx) {
        finishTx(tx);
    }

    protected void finishTx(UUID tx) {
        List<CompletableFuture<Lock>> txLocks = locks.remove(tx);
        assertNotNull(txLocks);

        for (CompletableFuture<Lock> fut : txLocks) {
            assertTrue(fut.isDone());

            if (!fut.isCompletedExceptionally()) {
                Lock lock = fut.join();

                lockManager.release(lock);
            }
        }
    }
}
