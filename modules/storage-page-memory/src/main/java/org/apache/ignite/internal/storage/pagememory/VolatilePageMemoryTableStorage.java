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

package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.pagememory.util.GradualTaskExecutor;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.storage.pagememory.mv.VersionChainTree;
import org.apache.ignite.internal.storage.pagememory.mv.VolatilePageMemoryMvPartitionStorage;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Implementation of {@link AbstractPageMemoryTableStorage} for in-memory case.
 */
public class VolatilePageMemoryTableStorage extends AbstractPageMemoryTableStorage {
    private final VolatilePageMemoryDataRegion dataRegion;

    private final GradualTaskExecutor destructionExecutor;

    /**
     * Constructor.
     *
     * @param tableCfg – Table configuration.
     * @param dataRegion – Data region for the table.
     */
    public VolatilePageMemoryTableStorage(
            TableConfiguration tableCfg,
            TablesConfiguration tablesCfg,
            VolatilePageMemoryDataRegion dataRegion,
            GradualTaskExecutor destructionExecutor
    ) {
        super(tableCfg, tablesCfg);

        this.dataRegion = dataRegion;
        this.destructionExecutor = destructionExecutor;
    }

    @Override
    public VolatilePageMemoryDataRegion dataRegion() {
        return dataRegion;
    }

    @Override
    public VolatilePageMemoryMvPartitionStorage createMvPartitionStorage(int partitionId) throws StorageException {
        VersionChainTree versionChainTree = createVersionChainTree(partitionId, tableCfg.value());

        IndexMetaTree indexMetaTree = createIndexMetaTree(partitionId, tableCfg.value());

        return new VolatilePageMemoryMvPartitionStorage(
                this,
                tablesCfg,
                partitionId,
                versionChainTree,
                indexMetaTree,
                destructionExecutor
        );
    }

    private IndexMetaTree createIndexMetaTree(int partitionId, TableView tableCfgView) {
        int grpId = tableCfgView.tableId();

        long metaPageId = dataRegion.pageMemory().allocatePage(grpId, partitionId, FLAG_AUX);

        try {
            return new IndexMetaTree(
                    grpId,
                    tableCfgView.name(),
                    partitionId,
                    dataRegion.pageMemory(),
                    PageLockListenerNoOp.INSTANCE,
                    new AtomicLong(),
                    metaPageId,
                    dataRegion.reuseList(),
                    true
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public boolean isVolatile() {
        return true;
    }

    @Override
    protected void finishDestruction() {
        // No-op.
    }

    /**
     * Returns new {@link VersionChainTree} instance for partition.
     *
     * @param partId Partition ID.
     * @param tableView Table configuration.
     * @throws StorageException If failed.
     */
    private VersionChainTree createVersionChainTree(int partId, TableView tableView) throws StorageException {
        int grpId = tableView.tableId();

        try {
            long metaPageId = dataRegion.pageMemory().allocatePage(grpId, partId, FLAG_AUX);

            return new VersionChainTree(
                    grpId,
                    tableView.name(),
                    partId,
                    dataRegion.pageMemory(),
                    PageLockListenerNoOp.INSTANCE,
                    new AtomicLong(),
                    metaPageId,
                    dataRegion.reuseList(),
                    true
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    String.format("Error creating TableTree [tableName=%s, partitionId=%s]", tableView.name(), partId),
                    e
            );
        }
    }

    @Override
    public CompletableFuture<Void> startRebalancePartition(int partitionId) {
        // TODO: IGNITE-18028 Implement
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> abortRebalancePartition(int partitionId) {
        // TODO: IGNITE-18028 Implement
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> finishRebalancePartition(int partitionId, long lastAppliedIndex, long lastAppliedTerm) {
        // TODO: IGNITE-18028 Implement
        throw new UnsupportedOperationException();
    }

    @Override
    CompletableFuture<Void> destroyMvPartitionStorage(AbstractPageMemoryMvPartitionStorage mvPartitionStorage) {
        mvPartitionStorage.close();

        VolatilePageMemoryMvPartitionStorage volatilePartitionStorage = (VolatilePageMemoryMvPartitionStorage) mvPartitionStorage;

        // We ignore the future returned by destroyStructures() on purpose: the destruction happens in the background,
        // we don't care when it finishes.
        volatilePartitionStorage.destroyStructures();

        return CompletableFuture.completedFuture(null);
    }
}
