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

import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Partition meta information manager.
 */
// TODO: IGNITE-17132 Do not forget about deleting the partition meta information
public class PartitionMetaManager {
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
     * @param groupPartitionId Partition of the group.
     * @param pageMemory Page memory.
     * @param filePageStore Partition file page store.
     */
    public PartitionMeta readOrCreateMeta(
            GroupPartitionId groupPartitionId,
            PersistentPageMemory pageMemory,
            FilePageStore filePageStore
    ) throws IgniteInternalCheckedException {
        ByteBuffer buffer = allocateBuffer(pageSize);

        long bufferAddr = bufferAddress(buffer);

        long partitionMetaPageId = pageMemory.partitionMetaPageId(groupPartitionId.getGroupId(), groupPartitionId.getPartitionId());

        try {
            if (filePageStore.size() > filePageStore.headerSize()) {
                // Reads the partition meta.
                boolean read = filePageStore.read(partitionMetaPageId, buffer, false);

                assert read : filePageStore.filePath();

                return createMeta(ioRegistry.resolve(bufferAddr), bufferAddr);
            } else {
                // Creates and writes a partition meta.
                PartitionMetaIo partitionMetaIo = PartitionMetaIo.VERSIONS.latest();
                partitionMetaIo.initNewPage(bufferAddr, partitionMetaPageId, pageSize);

                // Because we will now write this page.
                partitionMetaIo.setPageCount(bufferAddr, 1);

                filePageStore.pages(1);

                filePageStore.write(partitionMetaPageId, buffer.rewind(), true);

                filePageStore.sync();

                return createMeta(partitionMetaIo, bufferAddr);
            }
        } finally {
            freeBuffer(buffer);
        }
    }

    private PartitionMeta createMeta(PartitionMetaIo metaIo, long pageAddr) {
        return new PartitionMeta(
                metaIo.getTreeRootPageId(pageAddr),
                metaIo.getReuseListRootPageId(pageAddr),
                metaIo.getPageCount(pageAddr)
        );
    }
}
