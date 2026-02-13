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

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.components.NoOpLogSyncer;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.NoOpFailureManager;
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
import org.apache.ignite.internal.pagememory.metrics.CollectionMetricSource;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
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
    protected static final int GROUP_ID = 1;

    private static final String NODE_NAME = "benchmark-node";

    private PersistentPageMemory persistentPageMemory;
    private CheckpointManager checkpointManager;
    private FilePageStoreManager filePageStoreManager;
    private PartitionMetaManager partitionMetaManager;

    private Path workDir;
    private ExecutorService executorService;

    protected PersistentPageMemory persistentPageMemory() {
        return persistentPageMemory;
    }

    protected CheckpointManager checkpointManager() {
        return checkpointManager;
    }

    /** Starts page memory infrastructure including file stores, checkpoint manager, and pre-allocates a free list. */
    public void setup(Config config) throws Exception {
        String tempDirectoryName = getClass().getSimpleName();
        workDir = Files.createTempDirectory(tempDirectoryName + "-" + System.nanoTime());
        executorService = Executors.newCachedThreadPool();

        FailureManager failureManager = new NoOpFailureManager();

        var ioRegistry = new TestPageIoRegistry();
        ioRegistry.loadFromServiceLoader();

        filePageStoreManager = new FilePageStoreManager(
                NODE_NAME,
                workDir,
                new RandomAccessFileIoFactory(),
                config.pageSize(),
                failureManager
        );

        partitionMetaManager = new PartitionMetaManager(ioRegistry, config.pageSize(), FakePartitionMeta.FACTORY);

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
                new NoOpLogSyncer(),
                executorService,
                new CollectionMetricSource("benchmark", "storage", null),
                config.pageSize()
        );

        filePageStoreManager.start();
        checkpointManager.start();

        persistentPageMemory = new PersistentPageMemory(
                PersistentDataRegionConfiguration.builder()
                        .pageSize(config.pageSize())
                        .size(config.regionSize())
                        .replacementMode(config.replacementMode())
                        .build(),
                new PersistentPageMemoryMetricSource("benchmark"),
                ioRegistry,
                new long[]{config.regionSize()},
                config.checkpointBufferSize(),
                filePageStoreManager,
                checkpointManager::writePageToFilePageStore,
                checkpointManager.checkpointTimeoutLock(),
                new OffheapReadWriteLock(OffheapReadWriteLock.DEFAULT_CONCURRENCY_LEVEL),
                checkpointManager.partitionDestructionLockManager()
        );

        dataRegionList.add(new TestDataRegion<>(persistentPageMemory));

        int partitionsCount = config.partitionsCount();
        for (int i = 0; i < partitionsCount; i++) {
            createPartitionFilePageStore(i, config.pageSize());
        }
    }

    /** Stops page memory infrastructure and cleans up temporary files. */
    public void tearDown() throws Exception {
        IgniteUtils.closeAll(
                checkpointManager::stop,
                () -> persistentPageMemory.stop(true),
                filePageStoreManager::stop,
                executorService::shutdown,
                () -> {
                    IgniteUtils.deleteIfExists(workDir);
                }
        );
    }

    /** Create partition file page store. */
    private void createPartitionFilePageStore(int partitionId, int pageSize) throws Exception {
        ByteBuffer buffer = allocateBuffer(pageSize);

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

    /**
     * Configuration of the benchmark infrastructure.
     */
    public static class Config {
        public static final long DEFAULT_REGION_SIZE = Constants.GiB;
        public static final int DEFAULT_PAGE_SIZE = 4 * Constants.KiB;
        public static final int DEFAULT_CHECKPOINT_BUFFER_SIZE = 10 * Constants.MiB;

        private final long regionSize;
        private final int pageSize;
        private final ReplacementMode replacementMode;
        private final int partitionsCount;
        private final int checkpointBufferSize;

        private Config(long regionSize, int pageSize, ReplacementMode replacementMode, int partitionsCount, int checkpointBufferSize) {
            this.regionSize = regionSize;
            this.pageSize = pageSize;
            this.replacementMode = replacementMode;
            this.partitionsCount = partitionsCount;
            this.checkpointBufferSize = checkpointBufferSize;
        }

        public long regionSize() {
            return regionSize;
        }

        public int pageSize() {
            return pageSize;
        }

        public ReplacementMode replacementMode() {
            return replacementMode;
        }

        public int partitionsCount() {
            return partitionsCount;
        }

        public int checkpointBufferSize() {
            return checkpointBufferSize;
        }

        public static Builder builder() {
            return new Builder();
        }

        /**
         * {@code Config} builder static inner class.
         */
        public static final class Builder {
            private long regionSize = DEFAULT_REGION_SIZE;
            private int pageSize = DEFAULT_PAGE_SIZE;
            private ReplacementMode replacementMode = ReplacementMode.CLOCK;
            private int partitionsCount = 1;
            private int checkpointBufferSize = DEFAULT_CHECKPOINT_BUFFER_SIZE;

            /**
             * Sets the {@code regionSize} and returns a reference to this Builder enabling method chaining.
             *
             * @param regionSize the {@code regionSize} to set
             * @return a reference to this Builder
             */
            public Builder regionSize(long regionSize) {
                this.regionSize = regionSize;
                return this;
            }

            /**
             * Sets the {@code pageSize} and returns a reference to this Builder enabling method chaining.
             *
             * @param pageSize the {@code pageSize} to set
             * @return a reference to this Builder
             */
            public Builder pageSize(int pageSize) {
                this.pageSize = pageSize;
                return this;
            }

            /**
             * Sets the {@code replacementMode} and returns a reference to this Builder enabling method chaining.
             *
             * @param replacementMode the {@code replacementMode} to set
             * @return a reference to this Builder
             */
            public Builder replacementMode(ReplacementMode replacementMode) {
                this.replacementMode = replacementMode;
                return this;
            }

            /**
             * Sets the {@code partiitonsCount} and returns a reference to this Builder enabling method chaining.
             *
             * @param partiitonsCount the {@code partiitonsCount} to set
             * @return a reference to this Builder
             */
            public Builder partitionsCount(int partiitonsCount) {
                this.partitionsCount = partiitonsCount;
                return this;
            }

            /**
             * Sets the {@code checkpointBufferSize} and returns a reference to this Builder enabling method chaining.
             *
             * @param checkpointBufferSize the {@code checkpointBufferSize} to set
             * @return a reference to this Builder
             */
            public Builder checkpointBufferSize(int checkpointBufferSize) {
                this.checkpointBufferSize = checkpointBufferSize;
                return this;
            }

            /**
             * Returns a {@code Config} built from the parameters previously set.
             *
             * @return a {@code Config} built with parameters of this {@code Config.Builder}
             */
            public Config build() {
                return new Config(regionSize, pageSize, replacementMode, partitionsCount, checkpointBufferSize);
            }
        }
    }
}
