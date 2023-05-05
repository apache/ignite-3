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
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.internal.storage.MvPartitionStorage.Locker;
import org.apache.ignite.internal.storage.RowId;

/**
 * Default {@link Locker} implementation, that collects all locked row IDs in a private, non-thread-local, collection..
 */
public class LocalLocker implements Locker {
    /** {@link LockByRowId} instance, shared between threads. */
    private final LockByRowId locks;

    /** {@code RowId} or {@code Set<RowId>} that describes a set of row IDs, locked by current thread. */
    private Object locked;

    /**
     * Constructor.
     *
     * @param locks Shared instance.
     */
    public LocalLocker(LockByRowId locks) {
        this.locks = locks;
    }

    @Override
    public void lock(RowId rowId) {
        locks.lock(rowId);

        markAsLocked(rowId);
    }

    @Override
    public boolean tryLock(RowId rowId) {
        if (locks.tryLock(rowId)) {
            markAsLocked(rowId);

            return true;
        }

        return false;
    }

    /**
     * Returns {@code true} if passed row ID is currently locked.
     */
    public boolean isLocked(RowId rowId) {
        return Objects.equals(rowId, locked) || (locked instanceof Set) && ((Set<?>) locked).contains(rowId);
    }

    /**
     * Releases all locks, held by the current thread.
     *
     * <p>Order of releasing the locks is not defined, each lock will be released with all re-entries.
     */
    public void unlockAll() {
        if (locked instanceof RowId) {
            locks.unlockAll((RowId) locked);
        } else if (locked != null) {
            for (RowId rowId : (Set<RowId>) locked) {
                locks.unlockAll(rowId);
            }
        }
    }

    private void markAsLocked(RowId rowId) {
        if (locked == null) {
            locked = rowId;
        } else {
            if (locked instanceof RowId) {
                if (locked.equals(rowId)) {
                    return;
                }

                Set<Object> rowIds = new HashSet<>();

                rowIds.add(locked);

                locked = rowIds;
            }

            assert locked instanceof Set;

            ((Set<RowId>) locked).add(rowId);
        }
    }
}
