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

import static org.apache.ignite.internal.pagememory.PageMemoryTestUtils.newDataRegion;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager.safeToUpdateAllPageMemories;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.pagememory.PageMemoryDataRegion;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryCheckpointConfiguration;
import org.apache.ignite.internal.pagememory.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl;
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
        PageMemoryDataRegion dataRegion = newDataRegion(true, mock(PageMemoryImpl.class));

        CheckpointManager checkpointManager = new CheckpointManager(
                "test",
                null,
                null,
                checkpointConfig,
                mock(FilePageStoreManager.class),
                List.of(dataRegion),
                workDir,
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
        PageMemoryDataRegion dataRegion0 = newDataRegion(false, mock(PageMemoryNoStoreImpl.class));

        assertTrue(safeToUpdateAllPageMemories(List.of(dataRegion0)));

        AtomicBoolean safeToUpdate = new AtomicBoolean();

        PageMemoryImpl pageMemoryImpl = mock(PageMemoryImpl.class);

        when(pageMemoryImpl.safeToUpdate()).then(answer -> safeToUpdate.get());

        PageMemoryDataRegion dataRegion1 = newDataRegion(true, pageMemoryImpl);

        assertFalse(safeToUpdateAllPageMemories(List.of(dataRegion1)));
        assertFalse(safeToUpdateAllPageMemories(List.of(dataRegion0, dataRegion1)));

        safeToUpdate.set(true);

        assertTrue(safeToUpdateAllPageMemories(List.of(dataRegion1)));
        assertTrue(safeToUpdateAllPageMemories(List.of(dataRegion0, dataRegion1)));
    }
}
