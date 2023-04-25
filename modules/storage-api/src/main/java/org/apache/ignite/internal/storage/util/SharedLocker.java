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
import org.apache.ignite.internal.storage.MvPartitionStorage.Locker;
import org.apache.ignite.internal.storage.RowId;

public class SharedLocker implements Locker {
    private final LockByRowId locks;

    // Not thread-local!
    private Object locked;

    public SharedLocker(LockByRowId locks) {
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

    public boolean isLocked(RowId rowId) {
        return rowId == locked || (locked instanceof Set) && ((Set<?>) locked).contains(rowId);
    }

    public void releaseAll() {
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
