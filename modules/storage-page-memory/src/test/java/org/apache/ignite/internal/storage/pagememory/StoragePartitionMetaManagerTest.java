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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.pagememory.persistence.PartitionMeta.partitionMetaPageId;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStore.VERSION_1;
import static org.apache.ignite.internal.storage.pagememory.StoragePartitionMeta.FACTORY;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.zeroMemory;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIo;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreHeader;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreIo;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link PartitionMetaManager} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class StoragePartitionMetaManagerTest extends BaseIgniteAbstractTest {
    private static final int PAGE_SIZE = 1024;

    private static PageIoRegistry ioRegistry;

    @BeforeAll
    static void beforeAll() {
        ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();
    }

    @AfterAll
    static void afterAll() {
        ioRegistry = null;
    }

    @Test
    void testReadWritePartitionMeta(@WorkDirectory Path workDir) throws Exception {
        Path testFilePath = workDir.resolve("test");

        PartitionMetaManager manager = new PartitionMetaManager(ioRegistry, PAGE_SIZE, FACTORY);

        GroupPartitionId partId = new GroupPartitionId(0, 0);

        ByteBuffer buffer = allocateBuffer(PAGE_SIZE);

        try {
            long wiHeadLink = Long.MAX_VALUE;
            long lastAppliedIndex = Long.MAX_VALUE - 1;
            long lastAppliedTerm = Long.MAX_VALUE - 2;
            long pageId = Long.MAX_VALUE;
            long versionChainTreeRootPageId = Long.MAX_VALUE - 3;
            long freeListRootPageId = Long.MAX_VALUE - 4;
            long indexTreeMetaPageId = Long.MAX_VALUE - 5;
            long gcQueueMetaPageId = Long.MAX_VALUE - 6;
            long leaseStartTime = Long.MAX_VALUE - 8;

            // Check for an empty file.
            try (FilePageStore filePageStore = createFilePageStore(testFilePath)) {
                StoragePartitionMeta meta = readOrCreateMeta(manager, partId, filePageStore);

                assertEquals(0, meta.lastAppliedIndex());
                assertEquals(0, meta.lastAppliedTerm());
                assertEquals(0, meta.lastReplicationProtocolGroupConfigFirstPageId());
                assertEquals(0, meta.versionChainTreeRootPageId());
                assertEquals(0, meta.gcQueueMetaPageId());
                assertEquals(0, meta.indexTreeMetaPageId());
                assertEquals(0, meta.freeListRootPageId());
                assertEquals(1, meta.pageCount());
                assertEquals(HybridTimestamp.MIN_VALUE.longValue(), meta.leaseStartTime());
                assertEquals(0, meta.estimatedSize());
                assertEquals(0, meta.wiHeadLink());

                // Change the meta and write it to the file.
                meta.lastApplied(null, lastAppliedIndex, lastAppliedTerm);
                meta.lastReplicationProtocolGroupConfigFirstPageId(null, pageId);
                meta.versionChainTreeRootPageId(null, versionChainTreeRootPageId);
                meta.gcQueueMetaPageId(null, gcQueueMetaPageId);
                meta.indexTreeMetaPageId(null, indexTreeMetaPageId);
                meta.freeListRootPageId(null, freeListRootPageId);
                meta.incrementPageCount(null);
                meta.updateLease(null, leaseStartTime);
                meta.incrementEstimatedSize(null);
                meta.updateWiHead(null, wiHeadLink);

                manager.writeMetaToBuffer(partId, meta.metaSnapshot(UUID.randomUUID()), buffer);

                filePageStore.allocatePage();

                filePageStore.write(partitionMetaPageId(partId.getPartitionId()), buffer.rewind());

                filePageStore.sync();
            }

            // Check not empty file.
            try (FilePageStore filePageStore = createFilePageStore(testFilePath)) {
                StoragePartitionMeta meta = readOrCreateMeta(manager, partId, filePageStore);

                assertEquals(lastAppliedIndex, meta.lastAppliedIndex());
                assertEquals(lastAppliedTerm, meta.lastAppliedTerm());
                assertEquals(pageId, meta.lastReplicationProtocolGroupConfigFirstPageId());
                assertEquals(versionChainTreeRootPageId, meta.versionChainTreeRootPageId());
                assertEquals(freeListRootPageId, meta.freeListRootPageId());
                assertEquals(indexTreeMetaPageId, meta.indexTreeMetaPageId());
                assertEquals(gcQueueMetaPageId, meta.gcQueueMetaPageId());
                assertEquals(2, meta.pageCount());
                assertEquals(leaseStartTime, meta.leaseStartTime());
                assertEquals(1, meta.estimatedSize());
                assertEquals(wiHeadLink, meta.wiHeadLink());
            }

            // Check with delta file.
            try (FilePageStore filePageStore = createFilePageStore(testFilePath)) {
                int deltaPageCount = Integer.MAX_VALUE - 100;
                long deltaWiHeadLink = Long.MAX_VALUE - 10;
                long deltaLastAppliedIndex = Long.MAX_VALUE - 11;
                long deltaLastAppliedTerm = Long.MAX_VALUE - 12;
                long deltaPageId = Long.MAX_VALUE - 13;
                long deltaVersionChainTreeRootPageId = Long.MAX_VALUE - 14;
                long deltaFreeListRootPageId = Long.MAX_VALUE - 15;
                long deltaIndexTreeMetaPageId = Long.MAX_VALUE - 16;
                long deltaGcQueueMetaPageId = Long.MAX_VALUE - 17;
                long deltaLeaseStartTime = Long.MAX_VALUE - 18;
                long deltaEstimatedSize = Long.MAX_VALUE - 19;

                manager.writeMetaToBuffer(
                        partId,
                        new StoragePartitionMeta(deltaPageCount, 1, deltaLastAppliedIndex, deltaLastAppliedTerm, deltaPageId,
                                deltaLeaseStartTime, new UUID(1, 1), deltaPageId, deltaFreeListRootPageId,
                                deltaVersionChainTreeRootPageId, deltaIndexTreeMetaPageId, deltaGcQueueMetaPageId,
                                deltaEstimatedSize, deltaWiHeadLink)
                                .init(null)
                                .metaSnapshot(null),
                        buffer.rewind()
                );

                DeltaFilePageStoreIo deltaFilePageStoreIo = filePageStore.getOrCreateNewDeltaFile(
                        index -> workDir.resolve("testDelta" + index),
                        () -> new int[]{0}
                ).get(1, SECONDS);

                deltaFilePageStoreIo.write(partitionMetaPageId(partId.getPartitionId()), buffer.rewind());

                deltaFilePageStoreIo.sync();

                StoragePartitionMeta meta = readOrCreateMeta(manager, partId, filePageStore);

                assertEquals(deltaLastAppliedIndex, meta.lastAppliedIndex());
                assertEquals(deltaLastAppliedTerm, meta.lastAppliedTerm());
                assertEquals(deltaPageId, meta.lastReplicationProtocolGroupConfigFirstPageId());
                assertEquals(deltaFreeListRootPageId, meta.freeListRootPageId());
                assertEquals(deltaVersionChainTreeRootPageId, meta.versionChainTreeRootPageId());
                assertEquals(deltaIndexTreeMetaPageId, meta.indexTreeMetaPageId());
                assertEquals(deltaGcQueueMetaPageId, meta.gcQueueMetaPageId());
                assertEquals(deltaPageCount, meta.pageCount());
                assertEquals(deltaLeaseStartTime, meta.leaseStartTime());
                assertEquals(deltaEstimatedSize, meta.estimatedSize());
                assertEquals(deltaWiHeadLink, meta.wiHeadLink());
            }

            // Let's check the broken CRC.
            try (
                    FileIo fileIo = new RandomAccessFileIoFactory().create(testFilePath);
                    FilePageStore filePageStore = createFilePageStore(testFilePath)
            ) {
                zeroMemory(bufferAddress(buffer), PAGE_SIZE);

                fileIo.writeFully(buffer.rewind(), filePageStore.headerSize());

                StoragePartitionMeta meta = readOrCreateMeta(manager, partId, filePageStore);

                assertEquals(0, meta.lastAppliedIndex());
                assertEquals(0, meta.lastAppliedTerm());
                assertEquals(0, meta.lastReplicationProtocolGroupConfigFirstPageId());
                assertEquals(0, meta.versionChainTreeRootPageId());
                assertEquals(0, meta.freeListRootPageId());
                assertEquals(1, meta.pageCount());
                assertEquals(0, meta.estimatedSize());
                assertEquals(0, meta.wiHeadLink());
            }
        } finally {
            freeBuffer(buffer);
        }
    }

    private static FilePageStore createFilePageStore(Path filePath) throws Exception {
        FilePageStore filePageStore = new FilePageStore(new FilePageStoreIo(
                new RandomAccessFileIoFactory(),
                filePath,
                new FilePageStoreHeader(VERSION_1, PAGE_SIZE)
        ));

        filePageStore.ensure();

        return filePageStore;
    }

    private static StoragePartitionMeta readOrCreateMeta(
            PartitionMetaManager manager,
            GroupPartitionId partId,
            FilePageStore filePageStore
    ) throws Exception {
        ByteBuffer buffer = allocateBuffer(PAGE_SIZE);

        try {
            return (StoragePartitionMeta) manager.readOrCreateMeta(null, partId, filePageStore, buffer, 1);
        } finally {
            freeBuffer(buffer);
        }
    }
}
