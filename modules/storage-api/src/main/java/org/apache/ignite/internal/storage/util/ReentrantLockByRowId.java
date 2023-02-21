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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.ignite.internal.storage.RowId;

/**
 * {@link ReentrantLock} by row ID.
 *
 * <p>Allows synchronization of version chain update operations.
 */
public class ReentrantLockByRowId {
    private final ConcurrentMap<RowId, LockHolder<ReentrantLock>> lockHolderByRowId = new ConcurrentHashMap<>();

    /**
     * Executes the supplier under lock by row ID.
     *
     * @param <T> Return type.
     * @param rowId Row ID.
     * @param supplier Supplier to execute under the lock.
     * @return Value.
     */
    public <T> T inLock(RowId rowId, Supplier<T> supplier) {
        acquireLock(rowId);

        try {
            return supplier.get();
        } finally {
            releaseLock(rowId);
        }
    }

    /**
     * Acquires the lock by row ID.
     *
     * @param rowId Row ID.
     */
    public void acquireLock(RowId rowId) {
        LockHolder<ReentrantLock> lockHolder = lockHolderByRowId.compute(rowId, (rowId1, reentrantLockLockHolder) -> {
            if (reentrantLockLockHolder == null) {
                reentrantLockLockHolder = new LockHolder<>(new ReentrantLock());
            }

            reentrantLockLockHolder.incrementHolders();

            return reentrantLockLockHolder;
        });

        lockHolder.getLock().lock();
    }

    /**
     * Releases the lock by row ID.
     *
     * @param rowId Row ID.
     * @throws IllegalStateException If the lock could not be found by row ID.
     * @throws IllegalMonitorStateException If the current thread does not hold this lock.
     */
    public void releaseLock(RowId rowId) {
        LockHolder<ReentrantLock> lockHolder = lockHolderByRowId.get(rowId);

        if (lockHolder == null) {
            throw new IllegalStateException("Could not find lock by row ID: " + rowId);
        }

        lockHolder.getLock().unlock();

        lockHolderByRowId.compute(rowId, (rowId1, reentrantLockLockHolder) -> {
            assert reentrantLockLockHolder != null;

            return reentrantLockLockHolder.decrementHolders() ? null : reentrantLockLockHolder;
        });
    }
}
