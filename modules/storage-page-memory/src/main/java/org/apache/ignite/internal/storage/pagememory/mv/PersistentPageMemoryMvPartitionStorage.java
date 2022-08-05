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

package org.apache.ignite.internal.storage.pagememory.mv;

import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;

/**
 * Implementation of {@link MvPartitionStorage} based on a {@link BplusTree} for persistent case.
 */
public class PersistentPageMemoryMvPartitionStorage extends AbstractPageMemoryMvPartitionStorage {
    private final CheckpointTimeoutLock checkpointTimeoutLock;

    /**
     * Constructor.
     *
     * @param partId Partition id.
     * @param tableView Table configuration.
     * @param pageMemory Page memory.
     * @param versionChainFreeList Free list for {@link VersionChain}.
     * @param rowVersionFreeList Free list for {@link RowVersion}.
     * @param versionChainTree Table tree for {@link VersionChain}.
     * @param checkpointTimeoutLock Checkpoint timeout lock.
     */
    public PersistentPageMemoryMvPartitionStorage(
            int partId,
            TableView tableView,
            PersistentPageMemory pageMemory,
            VersionChainFreeList versionChainFreeList,
            RowVersionFreeList rowVersionFreeList,
            VersionChainTree versionChainTree,
            CheckpointTimeoutLock checkpointTimeoutLock
    ) {
        super(partId, tableView, pageMemory, versionChainFreeList, rowVersionFreeList, versionChainTree);

        this.checkpointTimeoutLock = checkpointTimeoutLock;
    }

    /** {@inheritDoc} */
    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        checkpointTimeoutLock.checkpointReadLock();

        try {
            return closure.execute();
        } finally {
            checkpointTimeoutLock.checkpointReadUnlock();
        }
    }
}
