/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage.pagememory;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.InvokeClosure;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.util.IgniteCursor;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link PartitionStorage} based on a {@link BplusTree} for persistent case.
 */
public class PersistentPageMemoryPartitionStorage extends AbstractPageMemoryPartitionStorage {
    private final CheckpointTimeoutLock checkpointTimeoutLock;

    /**
     * Constructor.
     *
     * @param partId Partition id.
     * @param freeList Table free list.
     * @param tree Table tree.
     * @param checkpointTimeoutLock Checkpoint timeout lock.
     */
    public PersistentPageMemoryPartitionStorage(
            int partId,
            TableFreeList freeList,
            TableTree tree,
            CheckpointTimeoutLock checkpointTimeoutLock
    ) {
        super(partId, freeList, tree);

        this.checkpointTimeoutLock = checkpointTimeoutLock;
    }

    /** {@inheritDoc} */
    @Override
    public void write(DataRow row) throws StorageException {
        checkpointTimeoutLock.checkpointReadLock();

        try {
            super.write(row);
        } finally {
            checkpointTimeoutLock.checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeAll(List<? extends DataRow> rows) throws StorageException {
        checkpointTimeoutLock.checkpointReadLock();

        try {
            super.writeAll(rows);
        } finally {
            checkpointTimeoutLock.checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public Collection<DataRow> insertAll(List<? extends DataRow> rows) throws StorageException {
        checkpointTimeoutLock.checkpointReadLock();

        try {
            return super.insertAll(rows);
        } finally {
            checkpointTimeoutLock.checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void remove(SearchRow key) throws StorageException {
        checkpointTimeoutLock.checkpointReadLock();

        try {
            super.remove(key);
        } finally {
            checkpointTimeoutLock.checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public Collection<SearchRow> removeAll(List<? extends SearchRow> keys) throws StorageException {
        checkpointTimeoutLock.checkpointReadLock();

        try {
            return super.removeAll(keys);
        } finally {
            checkpointTimeoutLock.checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public Collection<DataRow> removeAllExact(List<? extends DataRow> keyValues) throws StorageException {
        checkpointTimeoutLock.checkpointReadLock();

        try {
            return super.removeAllExact(keyValues);
        } finally {
            checkpointTimeoutLock.checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public <T> @Nullable T invoke(SearchRow key, InvokeClosure<T> clo) throws StorageException {
        checkpointTimeoutLock.checkpointReadLock();

        try {
            return super.invoke(key, clo);
        } finally {
            checkpointTimeoutLock.checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() throws StorageException {
        checkpointTimeoutLock.checkpointReadLock();

        try {
            // TODO: IGNITE-17132 Fix partition destruction

            IgniteCursor<TableDataRow> cursor = tree.find(null, null);

            while (cursor.next()) {
                TableDataRow row = cursor.get();

                if (tree.removex(row)) {
                    freeList.removeDataRowByLink(row.link());
                }
            }
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error destroy partition: " + partId, e);
        } finally {
            checkpointTimeoutLock.checkpointReadUnlock();
        }
    }
}
