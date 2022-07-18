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

import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStore.VERSION_1;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.invocation.InvocationOnMock;

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
        Path filePath = workDir.resolve("test");

        PartitionMetaManager manager = new PartitionMetaManager(ioRegistry, PAGE_SIZE);

        PersistentPageMemory pageMemory = createPageMemory();

        GroupPartitionId id = new GroupPartitionId(0, 0);

        try (FilePageStore filePageStore = createFilePageStore(filePath)) {
            // Check for an empty file.
            PartitionMeta meta = manager.readOrCreateMeta(null, id, pageMemory, filePageStore);

            assertEquals(0, meta.treeRootPageId());
            assertEquals(0, meta.reuseListRootPageId());
            assertEquals(1, meta.pageCount());

            // Change the meta and write it to the file.
            meta.treeRootPageId(null, 100);
            meta.reuseListRootPageId(null, 500);
            meta.incrementPageCount(null);

            ByteBuffer buffer = allocateBuffer(PAGE_SIZE);

            try {
                manager.writeMetaToBuffer(id, pageMemory, meta.metaSnapshot(UUID.randomUUID()), buffer);

                filePageStore.write(pageMemory.partitionMetaPageId(id.getGroupId(), id.getPartitionId()), buffer.rewind(), true);

                filePageStore.sync();
            } finally {
                freeBuffer(buffer);
            }
        }

        try (FilePageStore filePageStore = createFilePageStore(filePath)) {
            // Check not empty file.
            PartitionMeta meta = manager.readOrCreateMeta(null, id, pageMemory, filePageStore);

            assertEquals(100, meta.treeRootPageId());
            assertEquals(500, meta.reuseListRootPageId());
            assertEquals(2, meta.pageCount());
        }
    }

    private static FilePageStore createFilePageStore(Path filePath) throws Exception {
        FilePageStore filePageStore = new FilePageStore(VERSION_1, PAGE_SIZE, PAGE_SIZE, filePath, new RandomAccessFileIoFactory());

        filePageStore.ensure();

        return filePageStore;
    }

    private static PersistentPageMemory createPageMemory() {
        PersistentPageMemory pageMemory = mock(PersistentPageMemory.class);

        when(pageMemory.partitionMetaPageId(anyInt(), anyInt())).then(InvocationOnMock::callRealMethod);

        return pageMemory;
    }
}
