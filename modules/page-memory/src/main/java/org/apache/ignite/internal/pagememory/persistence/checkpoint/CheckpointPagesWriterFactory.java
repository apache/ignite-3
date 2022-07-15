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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.pagememory.persistence.store.PageStore;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue;

/**
 * Factory class for checkpoint pages writer.
 *
 * <p>It holds all dependency which is needed for creation of checkpoint writer.
 */
public class CheckpointPagesWriterFactory {
    /** Logger. */
    private final IgniteLogger log;

    /** Thread local with buffers for the checkpoint threads. Each buffer represent one page for durable memory. */
    private final ThreadLocal<ByteBuffer> threadBuf;

    /** Writer which writes pages to page store during the checkpoint. */
    private final CheckpointPageWriter checkpointPageWriter;

    /** Page IO registry. */
    private final PageIoRegistry ioRegistry;

    /** Partition file page store manager. */
    private final FilePageStoreManager filePageStoreManager;

    /**
     * Constructor.
     *
     * @param log Logger.
     * @param checkpointPageWriter Checkpoint page writer.
     * @param ioRegistry Page IO registry.
     * @param filePageStoreManager Partition file page store manager.
     * @param pageSize Page size in bytes.
     */
    CheckpointPagesWriterFactory(
            IgniteLogger log,
            CheckpointPageWriter checkpointPageWriter,
            PageIoRegistry ioRegistry,
            FilePageStoreManager filePageStoreManager,
            // TODO: IGNITE-17017 Move to common config
            int pageSize
    ) {
        this.log = log;
        this.checkpointPageWriter = checkpointPageWriter;
        this.ioRegistry = ioRegistry;
        this.filePageStoreManager = filePageStoreManager;

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
     * @param dirtyPageIdQueue Checkpoint dirty page ID queue to write.
     * @param dirtyPartitionIdQueue Checkpoint dirty partition ID queue to update.
     * @param updStores Updated page store storage.
     * @param doneWriteFut Write done future.
     * @param beforePageWrite Before page write callback.
     * @param checkpointProgress Current checkpoint data.
     * @param shutdownNow Checker of stop operation.
     */
    CheckpointPagesWriter build(
            CheckpointMetricsTracker tracker,
            IgniteConcurrentMultiPairQueue<PersistentPageMemory, FullPageId> dirtyPageIdQueue,
            IgniteConcurrentMultiPairQueue<PersistentPageMemory, GroupPartitionId> dirtyPartitionIdQueue,
            ConcurrentMap<PageStore, LongAdder> updStores,
            CompletableFuture<?> doneWriteFut,
            Runnable beforePageWrite,
            CheckpointProgressImpl checkpointProgress,
            // TODO: IGNITE-16993 Consider a lock replacement
            BooleanSupplier shutdownNow
    ) {
        return new CheckpointPagesWriter(
                log,
                tracker,
                dirtyPageIdQueue,
                dirtyPartitionIdQueue,
                updStores,
                doneWriteFut,
                beforePageWrite,
                threadBuf,
                checkpointProgress,
                checkpointPageWriter,
                ioRegistry,
                filePageStoreManager,
                shutdownNow
        );
    }
}
