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

import java.util.HashSet;
import java.util.Set;
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
    private final ThreadLocal<Object> lockedRowIds = new ThreadLocal<>();

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
        acquireLock0(rowId);

        try {
            return supplier.get();
        } finally {
            releaseLock0(rowId, false);
        }
    }

    /**
     * Acquires the lock by row ID.
     *
     * @param rowId Row ID.
     */
    public void acquireLock(RowId rowId) {
        acquireLock0(rowId);

        Object lockedRowIds = this.lockedRowIds.get();

        if (lockedRowIds == null) {
            this.lockedRowIds.set(rowId);
        } else if (lockedRowIds.getClass() == RowId.class) {
            RowId lockedRowId = (RowId) lockedRowIds;

            if (!lockedRowId.equals(rowId)) {
                Set<RowId> rowIds = new HashSet<>();

                rowIds.add(lockedRowId);
                rowIds.add(rowId);

                this.lockedRowIds.set(rowIds);
            }
        } else {
            ((Set<RowId>) lockedRowIds).add(rowId);
        }
    }

    /**
     * Releases the lock by row ID.
     *
     * @param rowId Row ID.
     * @throws IllegalStateException If the lock could not be found by row ID.
     * @throws IllegalMonitorStateException If the current thread does not hold this lock.
     */
    public void releaseLock(RowId rowId) {
        releaseLock0(rowId, false);

        LockHolder<ReentrantLock> lockHolder = lockHolderByRowId.get(rowId);

        if (lockHolder != null && lockHolder.getLock().isHeldByCurrentThread()) {
            return;
        }

        Object lockedRowIds = this.lockedRowIds.get();

        assert lockedRowIds != null : rowId;

        if (lockedRowIds.getClass() == RowId.class) {
            RowId lockedRowId = (RowId) lockedRowIds;

            assert lockedRowId.equals(rowId) : "rowId=" + rowId + ", lockedRowId=" + lockedRowId;

            this.lockedRowIds.remove();
        } else {
            Set<RowId> rowIds = ((Set<RowId>) lockedRowIds);

            boolean remove = rowIds.remove(rowId);

            assert remove : "rowId=" + rowId + ", lockedRowIds=" + rowIds;

            if (rowIds.isEmpty()) {
                this.lockedRowIds.remove();
            }
        }
    }

    /**
     * Releases all locks {@link #acquireLock(RowId) acquired} by the current thread if exists.
     *
     * <p>Order of releasing the locks is not defined, each lock will be released with all re-entries.
     */
    public void releaseAllLockByCurrentThread() {
        Object lockedRowIds = this.lockedRowIds.get();

        this.lockedRowIds.remove();

        if (lockedRowIds == null) {
            return;
        } else if (lockedRowIds.getClass() == RowId.class) {
            releaseLock0(((RowId) lockedRowIds), true);
        } else {
            ((Set<RowId>) lockedRowIds).forEach(rowId -> releaseLock0(rowId, true));
        }
    }

    private void acquireLock0(RowId rowId) {
        LockHolder<ReentrantLock> lockHolder = lockHolderByRowId.compute(rowId, (rowId1, reentrantLockLockHolder) -> {
            if (reentrantLockLockHolder == null) {
                reentrantLockLockHolder = new LockHolder<>(new ReentrantLock());
            }

            reentrantLockLockHolder.incrementHolders();

            return reentrantLockLockHolder;
        });

        lockHolder.getLock().lock();
    }

    private void releaseLock0(RowId rowId, boolean untilHoldByCurrentThread) {
        LockHolder<ReentrantLock> lockHolder = lockHolderByRowId.get(rowId);

        if (lockHolder == null) {
            throw new IllegalStateException("Could not find lock by row ID: " + rowId);
        }

        ReentrantLock lock = lockHolder.getLock();

        do {
            lock.unlock();

            lockHolderByRowId.compute(rowId, (rowId1, reentrantLockLockHolder) -> {
                assert reentrantLockLockHolder != null;

                return reentrantLockLockHolder.decrementHolders() ? null : reentrantLockLockHolder;
            });
        } while (untilHoldByCurrentThread && lock.getHoldCount() > 0);
    }
}
