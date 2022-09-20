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

package org.apache.ignite.internal.storage.pagememory.mv;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;

/**
 * Implementation of {@link MvPartitionStorage} based on a {@link BplusTree} for in-memory case.
 */
public class VolatilePageMemoryMvPartitionStorage extends AbstractPageMemoryMvPartitionStorage {
    /** Last applied index value. */
    private volatile long lastAppliedIndex;

    /**
     * Constructor.
     *
     * @param tableStorage Table storage instance.
     * @param partitionId Partition id.
     * @param versionChainTree Table tree for {@link VersionChain}.
     * @param indexMetaTree Tree that contains SQL indexes' metadata.
     */
    public VolatilePageMemoryMvPartitionStorage(
            VolatilePageMemoryTableStorage tableStorage,
            TablesConfiguration tablesCfg,
            int partitionId,
            VersionChainTree versionChainTree,
            IndexMetaTree indexMetaTree
    ) {
        super(
                partitionId,
                tableStorage,
                tableStorage.dataRegion().rowVersionFreeList(),
                tableStorage.dataRegion().indexColumnsFreeList(),
                versionChainTree,
                indexMetaTree,
                tablesCfg
        );
    }

    /** {@inheritDoc} */
    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        return closure.execute();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> flush() {
        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override
    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    /** {@inheritDoc} */
    @Override
    public void lastAppliedIndex(long lastAppliedIndex) throws StorageException {
        this.lastAppliedIndex = lastAppliedIndex;
    }

    /** {@inheritDoc} */
    @Override
    public long persistedIndex() {
        return lastAppliedIndex;
    }
}
