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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.pagememory.inmemory.VolatilePageMemory;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;

/**
 * Implementation of {@link MvPartitionStorage} based on a {@link BplusTree} for in-memory case.
 */
public class VolatilePageMemoryMvPartitionStorage extends AbstractPageMemoryMvPartitionStorage {
    /** Last applied index value. */
    private volatile long lastAppliedIndex;

    /**
     * Constructor.
     *
     * @param partId Partition id.
     * @param tableView Table configuration.
     * @param pageMemory Page memory.
     * @param rowVersionFreeList Free list for {@link RowVersion}.
     * @param versionChainTree Table tree for {@link VersionChain}.
     */
    public VolatilePageMemoryMvPartitionStorage(
            int partId,
            TableView tableView,
            VolatilePageMemory pageMemory,
            RowVersionFreeList rowVersionFreeList,
            VersionChainTree versionChainTree
    ) {
        super(partId, tableView, pageMemory, rowVersionFreeList, versionChainTree);
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
