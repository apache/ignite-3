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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionDestructionLockManager;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.WriteDirtyPage;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue;

/**
 * Factory class for checkpoint pages writer.
 *
 * <p>It holds all dependency which is needed for creation of checkpoint writer.
 */
public class CheckpointPagesWriterFactory {
    /** Thread local with buffers for the checkpoint threads. Each buffer represent one page for durable memory. */
    private final ThreadLocal<ByteBuffer> threadBuf;

    /** Writer which writes pages to page store during the checkpoint. */
    private final WriteDirtyPage dirtyPageWriter;

    /** Page IO registry. */
    private final PageIoRegistry ioRegistry;

    /** Partition meta information manager. */
    private final PartitionMetaManager partitionMetaManager;

    private final PartitionDestructionLockManager partitionDestructionLockManager;

    /**
     * Constructor.
     *
     * @param dirtyPageWriter Checkpoint page writer.
     * @param ioRegistry Page IO registry.
     * @param partitionMetaManager Partition meta information manager.
     * @param pageSize Page size in bytes.
     * @param partitionDestructionLockManager Partition Destruction Lock Manager.
     */
    CheckpointPagesWriterFactory(
            WriteDirtyPage dirtyPageWriter,
            PageIoRegistry ioRegistry,
            PartitionMetaManager partitionMetaManager,
            // TODO: IGNITE-17017 Move to common config
            int pageSize,
            PartitionDestructionLockManager partitionDestructionLockManager
    ) {
        this.dirtyPageWriter = dirtyPageWriter;
        this.ioRegistry = ioRegistry;
        this.partitionMetaManager = partitionMetaManager;
        this.partitionDestructionLockManager = partitionDestructionLockManager;

        threadBuf = ThreadLocal.withInitial(() -> {
            ByteBuffer tmpWriteBuf = ByteBuffer.allocateDirect(pageSize);

            tmpWriteBuf.order(ByteOrder.nativeOrder());

            return tmpWriteBuf;
        });
    }

    /**
     * Returns instance of page checkpoint writer.
     *
     * @param tracker Checkpoint metrics tracker.
     * @param dirtyPartitionQueue Checkpoint dirty partition ID queue to write.
     * @param pageMemoryList List of {@link PersistentPageMemory} instances that have dirty partitions in current checkpoint.
     * @param updatedPartitions Updated partitions.
     * @param doneWriteFut Write done future.
     * @param updateHeartbeat Update heartbeat callback.
     * @param checkpointProgress Current checkpoint data.
     * @param shutdownNow Checker of stop operation.
     */
    CheckpointPagesWriter build(
            CheckpointMetricsTracker tracker,
            IgniteConcurrentMultiPairQueue<PersistentPageMemory, GroupPartitionId> dirtyPartitionQueue,
            List<PersistentPageMemory> pageMemoryList,
            ConcurrentMap<GroupPartitionId, PartitionWriteStats> updatedPartitions,
            CompletableFuture<?> doneWriteFut,
            Runnable updateHeartbeat,
            CheckpointProgressImpl checkpointProgress,
            // TODO: IGNITE-16993 Consider a lock replacement
            BooleanSupplier shutdownNow
    ) {
        return new CheckpointPagesWriter(
                tracker,
                dirtyPartitionQueue,
                pageMemoryList,
                updatedPartitions,
                doneWriteFut,
                updateHeartbeat,
                threadBuf,
                checkpointProgress,
                dirtyPageWriter,
                ioRegistry,
                partitionMetaManager,
                shutdownNow,
                partitionDestructionLockManager
        );
    }
}
