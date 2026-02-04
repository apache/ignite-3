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

package org.apache.ignite.internal.pagememory.benchmark;

import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.TestDataRegion;
import org.apache.ignite.internal.pagememory.TestPageIoRegistry;
import org.apache.ignite.internal.pagememory.configuration.CheckpointConfiguration;
import org.apache.ignite.internal.pagememory.configuration.PersistentDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.configuration.ReplacementMode;
import org.apache.ignite.internal.pagememory.persistence.FakePartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemoryMetricSource;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointMetricSource;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.util.Constants;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.OffheapReadWriteLock;

/**
 * Base class for PersistentPageMemory benchmarks. Sets up checkpoint infrastructure,
 * file stores, and partition metadata.
 */
public class PersistentPageMemoryBenchmarkBase {
    protected static final long REGION_SIZE = Constants.GiB;

    protected static final int PAGE_SIZE = 4 * Constants.KiB;

    protected static final int GROUP_ID = 1;

    protected static final int PARTITION_ID = 0;

    protected long regionSizeOverride = -1;

    private static final String NODE_NAME = "benchmark-node";

    private static final int CHECKPOINT_BUFFER_SIZE = 10 * Constants.MiB;

    protected PersistentPageMemory persistentPageMemory;

    protected FilePageStoreManager filePageStoreManager;

    protected CheckpointManager checkpointManager;

    protected PartitionMetaManager partitionMetaManager;

    protected Path workDir;

    protected ExecutorService executorService;

    protected ReplacementMode replacementMode = ReplacementMode.CLOCK;

    /** Starts page memory infrastructure including file stores, checkpoint manager, and pre-allocates a free list. */
    public void setup() throws Exception {
        workDir = Path.of(System.getProperty("java.io.tmpdir"), "ignite-benchmark-" + System.nanoTime());
        if (!workDir.toFile().mkdirs()) {
            throw new IllegalStateException("Failed to create work directory: " + workDir);
        }

        executorService = Executors.newCachedThreadPool();

        FailureManager failureManager = mock(FailureManager.class);

        var ioRegistry = new TestPageIoRegistry();
        ioRegistry.loadFromServiceLoader();

        filePageStoreManager = new FilePageStoreManager(
                NODE_NAME,
                workDir,
                new RandomAccessFileIoFactory(),
                PAGE_SIZE,
                failureManager
        );

        partitionMetaManager = new PartitionMetaManager(ioRegistry, PAGE_SIZE, FakePartitionMeta.FACTORY);

        var dataRegionList = new ArrayList<DataRegion<PersistentPageMemory>>();

        checkpointManager = new CheckpointManager(
                NODE_NAME,
                null,
                failureManager,
                CheckpointConfiguration.builder().checkpointThreads(1).build(),
                filePageStoreManager,
                partitionMetaManager,
                dataRegionList,
                ioRegistry,
                mock(LogSyncer.class),
                executorService,
                new CheckpointMetricSource("benchmark"),
                PAGE_SIZE
        );

        long actualRegionSize = regionSizeOverride > 0 ? regionSizeOverride : REGION_SIZE;

        persistentPageMemory = new PersistentPageMemory(
                PersistentDataRegionConfiguration.builder()
                        .pageSize(PAGE_SIZE)
                        .size(actualRegionSize)
                        .replacementMode(replacementMode)
                        .build(),
                new PersistentPageMemoryMetricSource("benchmark"),
                ioRegistry,
                new long[]{actualRegionSize},
                CHECKPOINT_BUFFER_SIZE,
                filePageStoreManager,
                checkpointManager::writePageToFilePageStore,
                checkpointManager.checkpointTimeoutLock(),
                new OffheapReadWriteLock(OffheapReadWriteLock.DEFAULT_CONCURRENCY_LEVEL),
                checkpointManager.partitionDestructionLockManager()
        );

        dataRegionList.add(new TestDataRegion<>(persistentPageMemory));

        filePageStoreManager.start();
        checkpointManager.start();
        persistentPageMemory.start();

        createPartitionFilePageStores();
    }

    /** Stops page memory infrastructure and cleans up temporary files. */
    public void tearDown() throws Exception {
        IgniteUtils.closeAll(
                checkpointManager == null ? null : checkpointManager::stop,
                persistentPageMemory == null ? null : () -> persistentPageMemory.stop(true),
                filePageStoreManager == null ? null : filePageStoreManager::stop
        );

        if (executorService != null) {
            executorService.shutdown();
        }

        if (workDir != null) {
            IgniteUtils.deleteIfExists(workDir);
        }
    }

    private void createPartitionFilePageStores() throws Exception {
        createPartitionFilePageStore(PARTITION_ID);
    }

    /** Create partition file page store. */
    protected void createPartitionFilePageStore(int partitionId) throws Exception {
        ByteBuffer buffer = allocateBuffer(PAGE_SIZE);

        try {
            var groupPartitionId = new GroupPartitionId(GROUP_ID, partitionId);

            FilePageStore filePageStore = filePageStoreManager.readOrCreateStore(groupPartitionId, buffer.rewind());

            filePageStore.ensure();

            PartitionMeta partitionMeta = partitionMetaManager.readOrCreateMeta(
                    null,
                    groupPartitionId,
                    filePageStore,
                    buffer.rewind(),
                    persistentPageMemory.partGeneration(groupPartitionId.getGroupId(), groupPartitionId.getPartitionId())
            );

            filePageStore.pages(partitionMeta.pageCount());

            filePageStore.setPageAllocationListener(pageIdx -> {
                assert checkpointManager.checkpointTimeoutLock().checkpointLockIsHeldByThread();

                var checkpointProgress = checkpointManager.lastCheckpointProgress();

                partitionMeta.incrementPageCount(checkpointProgress == null ? null : checkpointProgress.id());
            });

            filePageStoreManager.addStore(groupPartitionId, filePageStore);
            partitionMetaManager.addMeta(groupPartitionId, partitionMeta);
        } finally {
            freeBuffer(buffer);
        }
    }
}
