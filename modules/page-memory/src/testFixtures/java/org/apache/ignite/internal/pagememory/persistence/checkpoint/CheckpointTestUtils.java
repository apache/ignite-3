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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.pagememory.metrics.CollectionMetricSource;
import org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency;
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.CheckpointDirtyPagesView;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;

/**
 * Useful class for testing a checkpoint.
 */
public class CheckpointTestUtils {
    private static final CheckpointReadWriteLockMetrics metrics = new CheckpointReadWriteLockMetrics(
            new CollectionMetricSource("test", "storage", null)
    );

    /**
     * Returns new instance of {@link CheckpointReadWriteLock}.
     *
     * @param log Logger.
     * @param executorService Executor service.
     */
    static CheckpointReadWriteLock newReadWriteLock(IgniteLogger log, ExecutorService executorService) {
        return new CheckpointReadWriteLock(new ReentrantReadWriteLockWithTracking(log, 5_000), executorService, metrics);
    }

    /**
     * Returns mocked {@link CheckpointTimeoutLock}.
     *
     * @param checkpointHeldByCurrentThread Result of {@link CheckpointTimeoutLock#checkpointLockIsHeldByThread()}.
     */
    public static CheckpointTimeoutLock mockCheckpointTimeoutLock(boolean checkpointHeldByCurrentThread) {
        // Do not use "mock(CheckpointTimeoutLock.class)" because calling the CheckpointTimeoutLock.checkpointLockIsHeldByThread
        // greatly degrades in time, which is critical for ItBPlus*Test (it increases from 2 minutes to 5 minutes).
        return new CheckpointTimeoutLock(
                mock(CheckpointReadWriteLock.class),
                Long.MAX_VALUE,
                () -> CheckpointUrgency.NOT_REQUIRED,
                mock(Checkpointer.class),
                mock(FailureManager.class)
        ) {
            @Override
            public boolean checkpointLockIsHeldByThread() {
                return checkpointHeldByCurrentThread;
            }
        };
    }

    /**
     * Collects dirty pages into a list.
     *
     * @param dirtyPagesView Checkpoint dirty pages view.
     */
    public static List<DirtyFullPageId> toListDirtyPageIds(CheckpointDirtyPagesView dirtyPagesView) {
        return IntStream.range(0, dirtyPagesView.size()).mapToObj(dirtyPagesView::get).collect(Collectors.toList());
    }

    /**
     * Returns mocked {@link FilePageStoreManager}.
     *
     * @param stores Partition file page stores.
     * @throws Exception If failed.
     */
    public static FilePageStoreManager createFilePageStoreManager(
            Map<GroupPartitionId, FilePageStore> stores
    ) throws Exception {
        FilePageStoreManager manager = mock(FilePageStoreManager.class);

        when(manager.getStore(any(GroupPartitionId.class))).then(answer -> {
            FilePageStore pageStore = stores.get(new GroupPartitionId(answer.getArgument(0), answer.getArgument(1)));

            assertNotNull(pageStore);

            return pageStore;
        });

        return manager;
    }

    /**
     * Returns mocked {@link PartitionMetaManager}.
     *
     * @param metas Meta information of partitions.
     */
    public static PartitionMetaManager createPartitionMetaManager(Map<GroupPartitionId, PartitionMeta> metas) {
        PartitionMetaManager manager = mock(PartitionMetaManager.class);

        when(manager.getMeta(any(GroupPartitionId.class))).then(answer -> metas.get(answer.getArgument(0)));

        return manager;
    }
}
