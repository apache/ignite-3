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

package org.apache.ignite.internal.pagememory.persistence;

import static org.apache.ignite.internal.pagememory.persistence.PartitionMeta.partitionMetaPageId;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta.PartitionMetaSnapshot;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Partition meta information manager.
 */
// TODO: IGNITE-17132 Do not forget about deleting the partition meta information
public class PartitionMetaManager {
    private static final IgniteLogger LOG = Loggers.forClass(PartitionMetaManager.class);

    private final Map<GroupPartitionId, PartitionMeta> metas = new ConcurrentHashMap<>();

    private final PageIoRegistry ioRegistry;

    private final int pageSize;

    /**
     * Constructor.
     *
     * @param ioRegistry Page IO Registry.
     * @param pageSize Page size in bytes.
     */
    public PartitionMetaManager(PageIoRegistry ioRegistry, int pageSize) {
        this.ioRegistry = ioRegistry;
        this.pageSize = pageSize;
    }

    /**
     * Returns the partition's meta information.
     *
     * @param groupPartitionId Partition of the group.
     */
    public @Nullable PartitionMeta getMeta(GroupPartitionId groupPartitionId) {
        return metas.get(groupPartitionId);
    }

    /**
     * Adds partition meta information.
     *
     * @param groupPartitionId Partition of the group.
     * @param partitionMeta Partition meta information.
     */
    public void addMeta(GroupPartitionId groupPartitionId, PartitionMeta partitionMeta) {
        metas.put(groupPartitionId, partitionMeta);
    }

    /**
     * Reads the partition {@link PartitionMeta meta} from the partition file or creates a new one.
     *
     * <p>If it creates a new one, it writes the meta to the file.
     *
     * @param checkpointId Checkpoint ID.
     * @param groupPartitionId Partition of the group.
     * @param filePageStore Partition file page store.
     */
    public PartitionMeta readOrCreateMeta(
            @Nullable UUID checkpointId,
            GroupPartitionId groupPartitionId,
            FilePageStore filePageStore
    ) throws IgniteInternalCheckedException {
        ByteBuffer buffer = allocateBuffer(pageSize);

        long bufferAddr = bufferAddress(buffer);

        long partitionMetaPageId = partitionMetaPageId(groupPartitionId.getPartitionId());

        try {
            if (containsPartitionMeta(filePageStore)) {
                // Reads the partition meta.
                try {
                    filePageStore.readWithoutPageIdCheck(partitionMetaPageId, buffer, false);

                    return new PartitionMeta(checkpointId, ioRegistry.resolve(bufferAddr), bufferAddr);
                } catch (IgniteInternalDataIntegrityViolationException e) {
                    LOG.info(() -> "Error reading partition meta page, will be recreated: " + groupPartitionId, e);
                }
            }

            // Creates and writes a partition meta.
            PartitionMetaIo io = PartitionMetaIo.VERSIONS.latest();

            io.initNewPage(bufferAddr, partitionMetaPageId, pageSize);

            // Because we will now write this page.
            io.setPageCount(bufferAddr, 1);

            int pageIdx = filePageStore.allocatePage();

            assert pageIdx == 0 : pageIdx;

            filePageStore.write(partitionMetaPageId, buffer.rewind(), true);

            filePageStore.sync();

            return new PartitionMeta(checkpointId, io, bufferAddr);
        } finally {
            freeBuffer(buffer);
        }
    }

    /**
     * Writes the partition meta to the buffer.
     *
     * @param groupPartitionId Partition of the group.
     * @param partitionMeta Snapshot of the partition meta.
     * @param writeToBuffer Direct byte buffer to write partition meta.
     */
    public void writeMetaToBuffer(
            GroupPartitionId groupPartitionId,
            PartitionMetaSnapshot partitionMeta,
            ByteBuffer writeToBuffer
    ) {
        assert writeToBuffer.remaining() == pageSize : writeToBuffer.remaining();

        long partitionMetaPageId = partitionMetaPageId(groupPartitionId.getPartitionId());

        long pageAddr = bufferAddress(writeToBuffer);

        PartitionMetaIo io = PartitionMetaIo.VERSIONS.latest();

        io.initNewPage(pageAddr, partitionMetaPageId, pageSize);

        partitionMeta.writeTo(io, pageAddr);
    }

    private boolean containsPartitionMeta(FilePageStore filePageStore) throws IgniteInternalCheckedException {
        return filePageStore.deltaFileCount() > 0 || filePageStore.size() > filePageStore.headerSize();
    }
}
