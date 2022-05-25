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

import java.nio.file.Path;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.pagememory.PageMemoryDataRegion;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.worker.IgniteWorkerListener;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * Main class to abstract checkpoint-related processes and actions and hide them from higher-level components.
 *
 * <p>Implements default checkpointing algorithm which is sharp checkpoint but can be replaced by other implementations if needed.
 *
 * <p>Represents only an intermediate step in refactoring of checkpointing component and may change in the future.
 *
 * <p>This checkpoint ensures that all pages marked as dirty under {@link #checkpointTimeoutLock} will be consistently saved to disk.
 *
 * <p>Configuration of this checkpoint allows the following:
 * <ul>
 *     <li>Collecting all pages from configured dataRegions which was marked as dirty under {@link #checkpointTimeoutLock}.</li>
 *     <li>Marking the start of checkpoint on disk.</li>
 *     <li>Notifying the subscribers of different checkpoint states through {@link CheckpointListener}.</li>
 *     <li>Synchronizing collected pages with disk using {@link FilePageStoreManager}.</li>
 * </ul>
 */
public class CheckpointManager implements IgniteComponent {
    /** Checkpoint worker. */
    private final Checkpointer checkpointer;

    /** Main checkpoint steps. */
    private final CheckpointWorkflow checkpointWorkflow;

    /** Checkpoint markers storage which mark the start and end of each checkpoint. */
    private final CheckpointMarkersStorage checkpointMarkersStorage;

    /** Timeout checkpoint lock which should be used while write to memory happened. */
    private final CheckpointTimeoutLock checkpointTimeoutLock;

    /** Checkpoint page writer factory. */
    private final CheckpointPagesWriterFactory checkpointPagesWriterFactory;

    /**
     * Constructor.
     *
     * @param logger Logger producer.
     * @param igniteInstanceName Ignite instance name.
     * @param workerListener Listener for life-cycle checkpoint worker events.
     * @param longJvmPauseDetector Long JVM pause detector.
     * @param filePageStoreManager File page store manager.
     * @param dataRegions Data regions.
     * @param storagePath Storage path.
     * @param pageSize Page size in bytes.
     * @throws IgniteInternalCheckedException If failed.
     */
    public CheckpointManager(
            Function<Class<?>, IgniteLogger> logger,
            String igniteInstanceName,
            @Nullable IgniteWorkerListener workerListener,
            @Nullable LongJvmPauseDetector longJvmPauseDetector,
            FilePageStoreManager filePageStoreManager,
            Collection<PageMemoryDataRegion> dataRegions,
            Path storagePath,
            // TODO: IGNITE-17017 Move to common config
            int pageSize
    ) throws IgniteInternalCheckedException {
        // TODO: IGNITE-16984 get from config
        ReentrantReadWriteLockWithTracking reentrantReadWriteLockWithTracking = new ReentrantReadWriteLockWithTracking(
                logger.apply(CheckpointReadWriteLock.class),
                5_000
        );

        CheckpointReadWriteLock checkpointReadWriteLock = new CheckpointReadWriteLock(reentrantReadWriteLockWithTracking);

        checkpointMarkersStorage = new CheckpointMarkersStorage(storagePath);

        checkpointWorkflow = new CheckpointWorkflow(
                checkpointMarkersStorage,
                checkpointReadWriteLock,
                // TODO: IGNITE-16984 get from config
                CheckpointWriteOrder.RANDOM,
                dataRegions
        );

        checkpointPagesWriterFactory = new CheckpointPagesWriterFactory(
                logger.apply(CheckpointPagesWriterFactory.class),
                (fullPage, buf, tag) -> filePageStoreManager.write(fullPage.groupId(), fullPage.pageId(), buf, tag, true),
                pageSize
        );

        checkpointer = new Checkpointer(
                logger.apply(Checkpoint.class),
                igniteInstanceName,
                workerListener,
                longJvmPauseDetector,
                checkpointWorkflow,
                checkpointPagesWriterFactory,
                // TODO: IGNITE-16984 get from config
                1,
                // TODO: IGNITE-16984 get from config
                () -> 180_000,
                // TODO: IGNITE-16984 get from config
                () -> 40
        );

        checkpointTimeoutLock = new CheckpointTimeoutLock(
                logger.apply(CheckpointTimeoutLock.class),
                checkpointReadWriteLock,
                // TODO: IGNITE-16984 get from config
                10_000,
                () -> safeToUpdateAllPageMemoryImpl(dataRegions),
                checkpointer
        );
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        checkpointWorkflow.start();

        checkpointer.start();

        checkpointTimeoutLock.start();
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        IgniteUtils.closeAll(
                checkpointTimeoutLock::stop,
                checkpointer::stop,
                checkpointWorkflow::stop
        );
    }

    /**
     * Returns checkpoint timeout lock which can be used for protection of writing to memory.
     */
    public CheckpointTimeoutLock checkpointTimeoutLock() {
        return checkpointTimeoutLock;
    }

    /**
     * Adds a listener to be called for the corresponding persistent data region.
     *
     * @param listener Listener.
     * @param dataRegion Persistent data region for which listener is corresponded to, {@code null} for all regions.
     */
    public void addCheckpointListener(CheckpointListener listener, PageMemoryDataRegion dataRegion) {
        checkpointWorkflow.addCheckpointListener(listener, dataRegion);
    }

    /**
     * Removes the listener.
     *
     * @param listener Listener.
     */
    public void removeCheckpointListener(CheckpointListener listener) {
        checkpointWorkflow.removeCheckpointListener(listener);
    }

    /**
     * Start the new checkpoint immediately.
     *
     * @param reason Checkpoint reason.
     * @param finishListener Listener which will be called on finish checkpoint.
     * @return Triggered checkpoint progress.
     */
    public CheckpointProgress forceCheckpoint(
            String reason,
            @Nullable BiConsumer<Void, Throwable> finishListener
    ) {
        return checkpointer.scheduleCheckpoint(0, reason, finishListener);
    }

    /**
     * Returns {@link true} if it is safe for all persistent data regions to update their {@link PageMemoryImpl}.
     *
     * @param dataRegions Data regions.
     * @see PageMemoryImpl#safeToUpdate()
     */
    boolean safeToUpdateAllPageMemoryImpl(Collection<PageMemoryDataRegion> dataRegions) {
        for (PageMemoryDataRegion dataRegion : dataRegions) {
            if (dataRegion.persistent() && !((PageMemoryImpl) dataRegion.pageMemory()).safeToUpdate()) {
                return false;
            }
        }

        return true;
    }
}
