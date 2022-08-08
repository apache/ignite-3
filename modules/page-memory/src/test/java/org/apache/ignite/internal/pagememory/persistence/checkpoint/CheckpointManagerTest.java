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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager.pageIndexesForDeltaFilePageStore;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager.safeToUpdateAllPageMemories;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryCheckpointConfiguration;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIo;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link CheckpointManager} testing.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
public class CheckpointManagerTest {
    @InjectConfiguration
    private PageMemoryCheckpointConfiguration checkpointConfig;

    @WorkDirectory
    private Path workDir;

    @Test
    void testSimple() throws Exception {
        PersistentPageMemory pageMemory = mock(PersistentPageMemory.class);

        DataRegion<PersistentPageMemory> dataRegion = () -> pageMemory;

        CheckpointManager checkpointManager = new CheckpointManager(
                "test",
                null,
                null,
                checkpointConfig,
                mock(FilePageStoreManager.class),
                mock(PartitionMetaManager.class),
                List.of(dataRegion),
                workDir,
                mock(PageIoRegistry.class),
                1024
        );

        assertDoesNotThrow(checkpointManager::start);

        assertNotNull(checkpointManager.checkpointTimeoutLock());

        CheckpointListener checkpointListener = new CheckpointListener() {
        };

        assertDoesNotThrow(() -> checkpointManager.addCheckpointListener(checkpointListener, dataRegion));
        assertDoesNotThrow(() -> checkpointManager.removeCheckpointListener(checkpointListener));

        assertNotNull(checkpointManager.forceCheckpoint("test"));
        assertNotNull(checkpointManager.forceCheckpoint("test"));

        assertDoesNotThrow(checkpointManager::stop);
    }

    @Test
    void testSafeToUpdateAllPageMemories() {
        assertTrue(safeToUpdateAllPageMemories(List.of()));

        AtomicBoolean safeToUpdate0 = new AtomicBoolean();
        AtomicBoolean safeToUpdate1 = new AtomicBoolean();

        PersistentPageMemory pageMemory0 = mock(PersistentPageMemory.class);
        PersistentPageMemory pageMemory1 = mock(PersistentPageMemory.class);

        when(pageMemory0.safeToUpdate()).then(answer -> safeToUpdate0.get());
        when(pageMemory1.safeToUpdate()).then(answer -> safeToUpdate1.get());

        DataRegion<PersistentPageMemory> dataRegion0 = () -> pageMemory0;
        DataRegion<PersistentPageMemory> dataRegion1 = () -> pageMemory1;

        assertFalse(safeToUpdateAllPageMemories(List.of(dataRegion0)));
        assertFalse(safeToUpdateAllPageMemories(List.of(dataRegion1)));
        assertFalse(safeToUpdateAllPageMemories(List.of(dataRegion0, dataRegion1)));

        safeToUpdate0.set(true);

        assertTrue(safeToUpdateAllPageMemories(List.of(dataRegion0)));
        assertFalse(safeToUpdateAllPageMemories(List.of(dataRegion1)));
        assertFalse(safeToUpdateAllPageMemories(List.of(dataRegion0, dataRegion1)));

        safeToUpdate1.set(true);

        assertTrue(safeToUpdateAllPageMemories(List.of(dataRegion0)));
        assertTrue(safeToUpdateAllPageMemories(List.of(dataRegion1)));
        assertTrue(safeToUpdateAllPageMemories(List.of(dataRegion0, dataRegion1)));
    }

    @Test
    void testPageIndexesForDeltaFilePageStore() {
        PersistentPageMemory pageMemory0 = mock(PersistentPageMemory.class);
        PersistentPageMemory pageMemory1 = mock(PersistentPageMemory.class);

        CheckpointDirtyPages dirtyPages = new CheckpointDirtyPages(List.of(
                new DataRegionDirtyPages<>(pageMemory0, dirtyPageArray(0, 0, 1)),
                new DataRegionDirtyPages<>(pageMemory1, dirtyPageArray(0, 1, 2, 3, 4))
        ));

        assertArrayEquals(new int[]{0, 1}, pageIndexesForDeltaFilePageStore(dirtyPages.getPartitionView(pageMemory0, 0, 0)));
        assertArrayEquals(new int[]{0, 2, 3, 4}, pageIndexesForDeltaFilePageStore(dirtyPages.getPartitionView(pageMemory1, 0, 1)));
    }

    @Test
    void testWritePageToDeltaFilePageStore() throws Exception {
        FilePageStoreManager filePageStoreManager = mock(FilePageStoreManager.class);

        DeltaFilePageStoreIo deltaFilePageStoreIo = mock(DeltaFilePageStoreIo.class);

        FilePageStore filePageStore = mock(FilePageStore.class);

        when(filePageStore.getOrCreateNewDeltaFile(any(IntFunction.class), any(Supplier.class)))
                .thenReturn(completedFuture(deltaFilePageStoreIo));

        FullPageId dirtyPageId = new FullPageId(pageId(0, (byte) 0, 1), 0);

        when(filePageStoreManager.getStore(eq(dirtyPageId.groupId()), eq(dirtyPageId.partitionId()))).thenReturn(filePageStore);

        CheckpointManager checkpointManager = spy(new CheckpointManager(
                "test",
                null,
                null,
                checkpointConfig,
                filePageStoreManager,
                mock(PartitionMetaManager.class),
                List.of(),
                workDir,
                mock(PageIoRegistry.class),
                1024
        ));

        PersistentPageMemory pageMemory = mock(PersistentPageMemory.class);

        CheckpointProgress checkpointProgress = mock(CheckpointProgress.class);

        CheckpointDirtyPages dirtyPages = new CheckpointDirtyPages(List.of(
                new DataRegionDirtyPages<>(pageMemory, new FullPageId[]{dirtyPageId})
        ));

        when(checkpointProgress.inProgress()).thenReturn(true);

        when(checkpointProgress.pagesToWrite()).thenReturn(dirtyPages);

        when(checkpointManager.lastCheckpointProgress()).thenReturn(checkpointProgress);

        ByteBuffer pageBuf = mock(ByteBuffer.class);

        checkpointManager.writePageToDeltaFilePageStore(pageMemory, dirtyPageId, pageBuf, true);

        verify(deltaFilePageStoreIo, times(1)).write(eq(dirtyPageId.pageId()), eq(pageBuf), eq(true));
    }

    private static FullPageId[] dirtyPageArray(int grpId, int partId, int... pageIndex) {
        Arrays.sort(pageIndex);

        return IntStream.of(pageIndex)
                .mapToObj(pageIdx -> new FullPageId(pageId(partId, (byte) 0, pageIdx), grpId))
                .toArray(FullPageId[]::new);
    }
}
