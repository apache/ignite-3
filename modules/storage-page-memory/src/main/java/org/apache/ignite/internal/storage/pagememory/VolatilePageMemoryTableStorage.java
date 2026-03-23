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
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.storage.pagememory.mv.VersionChainTree;
import org.apache.ignite.internal.storage.pagememory.mv.VolatilePageMemoryMvPartitionStorage;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcQueue;

/**
 * Implementation of {@link AbstractPageMemoryTableStorage} for in-memory case.
 */
public class VolatilePageMemoryTableStorage extends AbstractPageMemoryTableStorage<VolatilePageMemoryMvPartitionStorage> {
    private final VolatilePageMemoryStorageEngine engine;

    private final VolatilePageMemoryDataRegion dataRegion;

    private final ExecutorService destructionExecutor;

    private final FailureProcessor failureProcessor;

    /**
     * Constructor.
     *
     * @param tableDescriptor Table descriptor.
     * @param indexDescriptorSupplier Index descriptor supplier.
     * @param engine Storage engine instance.
     * @param dataRegion Data region for the table.
     * @param destructionExecutor Executor used to destruct partitions.
     * @param failureProcessor Failure processor.
     */
    VolatilePageMemoryTableStorage(
            StorageTableDescriptor tableDescriptor,
            StorageIndexDescriptorSupplier indexDescriptorSupplier,
            VolatilePageMemoryStorageEngine engine,
            VolatilePageMemoryDataRegion dataRegion,
            ExecutorService destructionExecutor,
            FailureProcessor failureProcessor
    ) {
        super(tableDescriptor, indexDescriptorSupplier);

        this.engine = engine;
        this.dataRegion = dataRegion;
        this.destructionExecutor = destructionExecutor;
        this.failureProcessor = failureProcessor;
    }

    @Override
    public VolatilePageMemoryDataRegion dataRegion() {
        return dataRegion;
    }

    @Override
    public VolatilePageMemoryStorageEngine engine() {
        return engine;
    }

    @Override
    public VolatilePageMemoryMvPartitionStorage createMvPartitionStorage(int partitionId) throws StorageException {
        VersionChainTree versionChainTree = createVersionChainTree(partitionId);

        IndexMetaTree indexMetaTree = createIndexMetaTree(partitionId);

        GcQueue gcQueue = createGarbageCollectionTree(partitionId);

        return new VolatilePageMemoryMvPartitionStorage(
                this,
                partitionId,
                versionChainTree,
                indexMetaTree,
                gcQueue,
                destructionExecutor,
                failureProcessor
        );
    }

    private IndexMetaTree createIndexMetaTree(int partitionId) {
        try {
            long metaPageId = dataRegion.pageMemory().allocatePage(dataRegion.reuseList(), getTableId(), partitionId, FLAG_AUX);

            return new IndexMetaTree(
                    getTableId(),
                    Integer.toString(getTableId()),
                    partitionId,
                    dataRegion.pageMemory(),
                    engine.generateGlobalRemoveId(),
                    metaPageId,
                    dataRegion.reuseList(),
                    true
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(e);
        }
    }

    private GcQueue createGarbageCollectionTree(int partitionId) {
        try {
            long metaPageId = dataRegion.pageMemory().allocatePage(dataRegion().reuseList(), getTableId(), partitionId, FLAG_AUX);

            return new GcQueue(
                    getTableId(),
                    Integer.toString(getTableId()),
                    partitionId,
                    dataRegion.pageMemory(),
                    engine.generateGlobalRemoveId(),
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
     * @throws StorageException If failed.
     */
    private VersionChainTree createVersionChainTree(int partId) throws StorageException {
        try {
            long metaPageId = dataRegion.pageMemory().allocatePage(dataRegion().reuseList(), getTableId(), partId, FLAG_AUX);

            return new VersionChainTree(
                    getTableId(),
                    Integer.toString(getTableId()),
                    partId,
                    dataRegion.pageMemory(),
                    engine.generateGlobalRemoveId(),
                    metaPageId,
                    dataRegion.reuseList(),
                    true
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error creating TableTree: [tableId={}, partitionId={}]", e, getTableId(), partId);
        }
    }

    @Override
    CompletableFuture<Void> clearStorageAndUpdateDataStructures(
            AbstractPageMemoryMvPartitionStorage mvPartitionStorage,
            Runnable afterUpdateStructuresCallback
    ) {
        VolatilePageMemoryMvPartitionStorage volatilePartitionStorage = (VolatilePageMemoryMvPartitionStorage) mvPartitionStorage;

        volatilePartitionStorage.destroyStructures().whenComplete((res, ex) -> {
            if (ex != null) {
                String errorMessage = String.format(
                        "Could not destroy structures: [tableId=%s, partitionId=%s]", getTableId(),
                        volatilePartitionStorage.partitionId()
                );
                failureProcessor.process(new FailureContext(ex, errorMessage));
            }
        });

        int partitionId = mvPartitionStorage.partitionId();

        volatilePartitionStorage.updateDataStructures(
                createVersionChainTree(partitionId),
                createIndexMetaTree(partitionId),
                createGarbageCollectionTree(partitionId)
        );

        afterUpdateStructuresCallback.run();

        return nullCompletedFuture();
    }

    @Override
    CompletableFuture<Void> destroyMvPartitionStorage(AbstractPageMemoryMvPartitionStorage mvPartitionStorage) {
        VolatilePageMemoryMvPartitionStorage volatilePartitionStorage = (VolatilePageMemoryMvPartitionStorage) mvPartitionStorage;

        boolean transitioned = volatilePartitionStorage.transitionToDestroyedState();
        if (!transitioned) {
            // Someone has already started destruction (in which case we don't need to do anything), or closed the storage
            // (which means the node is going down, so the volatile storage does not need to be destroyed).
            return nullCompletedFuture();
        }

        return volatilePartitionStorage.destroyStructures()
                .whenComplete((v, e) -> volatilePartitionStorage.closeResources());
    }
}
