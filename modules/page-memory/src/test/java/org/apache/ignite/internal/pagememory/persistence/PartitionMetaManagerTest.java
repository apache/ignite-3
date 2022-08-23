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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.pagememory.persistence.PartitionMeta.partitionMetaPageId;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStore.VERSION_1;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.zeroMemory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIo;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreHeader;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreIo;
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
public class PartitionMetaManagerTest {
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
    void testAddGetMeta() {
        PartitionMetaManager manager = new PartitionMetaManager(ioRegistry, PAGE_SIZE);

        GroupPartitionId id = new GroupPartitionId(0, 0);

        assertNull(manager.getMeta(id));

        PartitionMeta meta = mock(PartitionMeta.class);

        manager.addMeta(id, meta);

        assertSame(meta, manager.getMeta(id));

        assertNull(manager.getMeta(new GroupPartitionId(0, 1)));
    }

    @Test
    void testReadWritePartitionMeta(@WorkDirectory Path workDir) throws Exception {
        Path testFilePath = workDir.resolve("test");

        PartitionMetaManager manager = new PartitionMetaManager(ioRegistry, PAGE_SIZE);

        GroupPartitionId partId = new GroupPartitionId(0, 0);

        ByteBuffer buffer = allocateBuffer(PAGE_SIZE);

        try {
            // Check for an empty file.
            try (FilePageStore filePageStore = createFilePageStore(testFilePath)) {
                PartitionMeta meta = manager.readOrCreateMeta(null, partId, filePageStore);

                assertEquals(0, meta.lastAppliedIndex());
                assertEquals(0, meta.versionChainTreeRootPageId());
                assertEquals(0, meta.rowVersionFreeListRootPageId());
                assertEquals(1, meta.pageCount());

                // Change the meta and write it to the file.
                meta.lastAppliedIndex(null, 50);
                meta.versionChainTreeRootPageId(null, 300);
                meta.rowVersionFreeListRootPageId(null, 900);
                meta.incrementPageCount(null);

                manager.writeMetaToBuffer(partId, meta.metaSnapshot(UUID.randomUUID()), buffer);

                filePageStore.allocatePage();

                filePageStore.write(partitionMetaPageId(partId.getPartitionId()), buffer.rewind(), true);

                filePageStore.sync();
            }

            // Check not empty file.
            try (FilePageStore filePageStore = createFilePageStore(testFilePath)) {
                PartitionMeta meta = manager.readOrCreateMeta(null, partId, filePageStore);

                assertEquals(50, meta.lastAppliedIndex());
                assertEquals(300, meta.versionChainTreeRootPageId());
                assertEquals(900, meta.rowVersionFreeListRootPageId());
                assertEquals(2, meta.pageCount());
            }

            // Check with delta file.
            try (FilePageStore filePageStore = createFilePageStore(testFilePath)) {
                manager.writeMetaToBuffer(
                        partId,
                        new PartitionMeta(UUID.randomUUID(), 100, 300, 900, 4).metaSnapshot(null),
                        buffer.rewind()
                );

                DeltaFilePageStoreIo deltaFilePageStoreIo = filePageStore.getOrCreateNewDeltaFile(
                        index -> workDir.resolve("testDelta" + index),
                        () -> new int[]{0}
                ).get(1, SECONDS);

                deltaFilePageStoreIo.write(partitionMetaPageId(partId.getPartitionId()), buffer.rewind(), true);

                deltaFilePageStoreIo.sync();

                PartitionMeta meta = manager.readOrCreateMeta(null, partId, filePageStore);

                assertEquals(100, meta.lastAppliedIndex());
                assertEquals(300, meta.versionChainTreeRootPageId());
                assertEquals(900, meta.rowVersionFreeListRootPageId());
                assertEquals(4, meta.pageCount());
            }

            // Let's check the broken CRC.
            try (
                    FileIo fileIo = new RandomAccessFileIoFactory().create(testFilePath);
                    FilePageStore filePageStore = createFilePageStore(testFilePath);
            ) {
                zeroMemory(bufferAddress(buffer), PAGE_SIZE);

                fileIo.writeFully(buffer.rewind(), filePageStore.headerSize());

                PartitionMeta meta = manager.readOrCreateMeta(null, partId, filePageStore);

                assertEquals(0, meta.lastAppliedIndex());
                assertEquals(0, meta.versionChainTreeRootPageId());
                assertEquals(0, meta.rowVersionFreeListRootPageId());
                assertEquals(1, meta.pageCount());
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
}
