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
import org.apache.ignite.internal.storage.RowId;

/**
 * {@link ReentrantLock} by row ID.
 *
 * <p>Allows synchronization of version chain update operations.
 */
public class LockByRowId {
    private final ConcurrentMap<RowId, LockHolder<ReentrantLock>> lockHolderByRowId = new ConcurrentHashMap<>();

    /**
     * Acquires the lock by row ID.
     *
     * @param rowId Row ID.
     */
    public void lock(RowId rowId) {
        LockHolder<ReentrantLock> lockHolder = lockHolderByRowId.compute(rowId, (id, holder) -> {
            if (holder == null) {
                holder = new LockHolder<>(new ReentrantLock());
            }

            if (!holder.getLock().isHeldByCurrentThread()) {
                // It's ok to perform non-thread-safe operation inside of the "compute" closure.
                holder.incrementHolders();
            }

            return holder;
        });

        if (!lockHolder.getLock().isHeldByCurrentThread()) {
            lockHolder.getLock().lock();
        }
    }

    /**
     * Tries to acquire the lock by row ID.
     *
     * @param rowId Row ID.
     * @return {@code true} if lock has been acquired successfully.
     */
    public boolean tryLock(RowId rowId) {
        boolean[] result = {false};

        lockHolderByRowId.compute(rowId, (id, holder) -> {
            if (holder == null) {
                holder = new LockHolder<>(new ReentrantLock());

                holder.incrementHolders();

                // Locks immediately, because no one else has seen this instance yet.
                holder.getLock().lock();

                result[0] = true;
            } else if (holder.getLock().isHeldByCurrentThread()) {
                // No-need to re-acquire the lock.
                result[0] = true;
            }

            return holder;
        });

        return result[0];
    }

    /**
     * Releases the lock by row ID.
     *
     * @param rowId Row ID.
     * @throws IllegalStateException If the lock could not be found by row ID.
     * @throws IllegalMonitorStateException If the current thread does not hold this lock.
     */
    public void unlockAll(RowId rowId) {
        LockHolder<ReentrantLock> lockHolder = lockHolderByRowId.get(rowId);

        if (lockHolder == null) {
            throw new IllegalStateException("Could not find lock by row ID: " + rowId);
        }

        ReentrantLock lock = lockHolder.getLock();

        lock.unlock();

        assert !lock.isHeldByCurrentThread();

        lockHolderByRowId.compute(rowId, (id, holder) -> {
            assert holder != null;

            // It's ok to perform non-thread-safe operation inside of the "compute" closure.
            return holder.decrementHolders() ? null : holder;
        });
    }
}
