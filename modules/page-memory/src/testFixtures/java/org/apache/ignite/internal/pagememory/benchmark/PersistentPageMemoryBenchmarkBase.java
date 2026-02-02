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

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Random;
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
import org.apache.ignite.internal.pagememory.freelist.FreeList;
import org.apache.ignite.internal.pagememory.freelist.FreeListImpl;
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
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.util.Constants;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.OffheapReadWriteLock;

/**
 * Base class for various JMH benchmarks that test PersistentPageMemory. This class handles the complex setup and teardown of all required
 * components including checkpoint infrastructure, file page stores, and partition metadata.
 *
 * <p>This base class provides a more realistic testing environment compared to {@link VolatilePageMemoryBenchmarkBase}, as it includes disk
 * I/O, checkpoint operations, and page replacement policies. It can be used to benchmark page cache metrics, page replacements, and overall
 * persistent storage performance.
 */
public class PersistentPageMemoryBenchmarkBase {
    /** Size of a data region. Should be large enough to fit all the data. */
    protected static final long REGION_SIZE = 1L * Constants.GiB;

    /** Page size. We use a more realistic 4KB for benchmarks. */
    protected static final int PAGE_SIZE = 4 * Constants.KiB;

    /** Group ID constant for the benchmark. Could be anything. */
    protected static final int GROUP_ID = 1;

    /** Partition ID constant for the benchmark. Could be anything. */
    protected static final int PARTITION_ID = 0;

    /** An instance of {@link Random}. */
    protected static final Random RANDOM = new Random(System.currentTimeMillis());

    /** Optional region size override for subclasses. If set, this overrides REGION_SIZE. */
    protected long regionSizeOverride = -1;

    /** Node name for testing. */
    private static final String NODE_NAME = "benchmark-node";

    /** Checkpoint buffer size (10 MB). */
    private static final int CHECKPOINT_BUFFER_SIZE = 10 * Constants.MiB;

    /** A {@link PersistentPageMemory} instance. */
    protected PersistentPageMemory persistentPageMemory;

    /** A {@link FreeListImpl} instance to be used as both {@link FreeList} and {@link ReuseList}. */
    protected FreeListImpl freeList;

    /** File page store manager for managing partition files. */
    protected FilePageStoreManager filePageStoreManager;

    /** Checkpoint manager for coordinating checkpoints. */
    protected CheckpointManager checkpointManager;

    /** Partition metadata manager. */
    protected PartitionMetaManager partitionMetaManager;

    /** Working directory for file page stores. */
    protected Path workDir;

    /** Executor service for async operations. */
    protected ExecutorService executorService;

    /** Page replacement mode (can be overridden by subclasses). */
    protected ReplacementMode replacementMode = ReplacementMode.CLOCK;

    /**
     * Starts persistent page memory infrastructure including file stores, checkpoint manager, and pre-allocates a free list.
     */
    public void setup() throws Exception {
        // Create temporary work directory
        workDir = Path.of(System.getProperty("java.io.tmpdir"), "ignite-benchmark-" + System.nanoTime());
        workDir.toFile().mkdirs();

        // Initialize executor service for async operations
        executorService = Executors.newCachedThreadPool();

        // Mock failure manager (not critical for benchmarks)
        FailureManager failureManager = mock(FailureManager.class);

        // Load page I/O registry
        var ioRegistry = new TestPageIoRegistry();
        ioRegistry.loadFromServiceLoader();

        // Initialize file page store manager
        filePageStoreManager = new FilePageStoreManager(
                NODE_NAME,
                workDir,
                new RandomAccessFileIoFactory(),
                PAGE_SIZE,
                failureManager
        );

        // Initialize partition metadata manager
        partitionMetaManager = new PartitionMetaManager(ioRegistry, PAGE_SIZE, FakePartitionMeta.FACTORY);

        // Create data region list for checkpoint manager
        var dataRegionList = new ArrayList<DataRegion<PersistentPageMemory>>();

        // Initialize checkpoint manager
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

        // Determine actual region size (use override if set)
        long actualRegionSize = regionSizeOverride > 0 ? regionSizeOverride : REGION_SIZE;

        // Create persistent page memory instance
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

        // Register data region with checkpoint manager
        dataRegionList.add(new TestDataRegion<>(persistentPageMemory));

        // Start all components in correct order
        filePageStoreManager.start();
        checkpointManager.start();
        persistentPageMemory.start();

        // Create partition file page stores
        createPartitionFilePageStores();

        // Pre-allocate free list
        freeList = new FreeListImpl(
                "freeList",
                GROUP_ID,
                PARTITION_ID,
                persistentPageMemory,
                persistentPageMemory.allocatePageNoReuse(GROUP_ID, PARTITION_ID, FLAG_AUX),
                true,
                null
        );
    }

    /**
     * Stops persistent page memory infrastructure and cleans up temporary files.
     */
    public void tearDown() throws Exception {
        IgniteUtils.closeAll(
                checkpointManager == null ? null : checkpointManager::stop,
                persistentPageMemory == null ? null : () -> persistentPageMemory.stop(true),
                filePageStoreManager == null ? null : filePageStoreManager::stop
        );

        // Shutdown executor service
        if (executorService != null) {
            executorService.shutdown();
        }

        // Clean up work directory
        if (workDir != null) {
            IgniteUtils.deleteIfExists(workDir);
        }
    }

    /**
     * Creates partition file page stores if they don't exist.
     */
    private void createPartitionFilePageStores() throws Exception {
        createPartitionFilePageStore(PARTITION_ID);
    }

    /**
     * Creates a file page store for the specified partition.
     *
     * @param partitionId Partition ID.
     */
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
