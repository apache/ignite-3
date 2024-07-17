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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.tx.test.TestTransactionIds;

/**
 * Abstract class making lock manager tests more simple.
 */
public abstract class AbstractLockingTest {
    protected final LockManager lockManager = lockManager();
    private Map<UUID, Map<IgniteBiTuple<LockKey, LockMode>, CompletableFuture<Lock>>> locks = new HashMap<>();

    protected abstract LockManager lockManager();

    protected UUID beginTx() {
        return TestTransactionIds.newTransactionId();
    }

    protected UUID beginTx(TxPriority priority) {
        return TestTransactionIds.newTransactionId(priority);
    }

    protected LockKey key(Object key) {
        ByteBuffer b = ByteBuffer.allocate(Integer.BYTES);
        b.putInt(key.hashCode());
        b.position(0);

        return new LockKey(0, b);
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
                v = new HashMap<>();
            }

            assertFalse(v.containsKey(mode));

            v.put(new IgniteBiTuple<>(key, mode), fut);

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
        Map<IgniteBiTuple<LockKey, LockMode>, CompletableFuture<Lock>> txLocks = locks.remove(tx);
        assertNotNull(txLocks);

        for (Map.Entry<IgniteBiTuple<LockKey, LockMode>, CompletableFuture<Lock>> e : txLocks.entrySet()) {
            CompletableFuture<Lock> fut = e.getValue();

            assertTrue(fut.isDone());

            if (!fut.isCompletedExceptionally()) {
                Lock lock = fut.join();

                lockManager.release(lock);
            }
        }
    }

    protected void release(UUID tx, LockKey key, LockMode lockMode) {
        Map<IgniteBiTuple<LockKey, LockMode>, CompletableFuture<Lock>> txLocks = locks.get(tx);
        assertNotNull(txLocks);

        CompletableFuture<Lock> lockFut = txLocks.remove(new IgniteBiTuple<>(key, lockMode));
        assertNotNull(lockFut);
        assertTrue(lockFut.isDone());

        if (!lockFut.isCompletedExceptionally()) {
            Lock lock = lockFut.join();
            lockManager.release(lock);
        }

        if (txLocks.isEmpty()) {
            locks.remove(tx);
        }
    }
}
