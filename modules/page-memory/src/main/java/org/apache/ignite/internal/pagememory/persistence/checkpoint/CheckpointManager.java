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
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.PageMemoryDataRegion;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryCheckpointConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryCheckpointView;
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
 * <p>Implements sharp checkpointing algorithm.
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
     * @param igniteInstanceName Ignite instance name.
     * @param checkpointConfig Checkpoint configuration.
     * @param workerListener Listener for life-cycle checkpoint worker events.
     * @param longJvmPauseDetector Long JVM pause detector.
     * @param filePageStoreManager File page store manager.
     * @param dataRegions Data regions.
     * @param storagePath Storage path.
     * @param pageSize Page size in bytes.
     * @throws IgniteInternalCheckedException If failed.
     */
    public CheckpointManager(
            String igniteInstanceName,
            @Nullable IgniteWorkerListener workerListener,
            @Nullable LongJvmPauseDetector longJvmPauseDetector,
            PageMemoryCheckpointConfiguration checkpointConfig,
            FilePageStoreManager filePageStoreManager,
            Collection<PageMemoryDataRegion> dataRegions,
            Path storagePath,
            // TODO: IGNITE-17017 Move to common config
            int pageSize
    ) throws IgniteInternalCheckedException {
        PageMemoryCheckpointView checkpointConfigView = checkpointConfig.value();

        long logReadLockThresholdTimeout = checkpointConfigView.logReadLockThresholdTimeout();

        ReentrantReadWriteLockWithTracking reentrantReadWriteLockWithTracking = logReadLockThresholdTimeout > 0
                ? new ReentrantReadWriteLockWithTracking(IgniteLogger.forClass(CheckpointReadWriteLock.class), logReadLockThresholdTimeout)
                : new ReentrantReadWriteLockWithTracking();

        CheckpointReadWriteLock checkpointReadWriteLock = new CheckpointReadWriteLock(reentrantReadWriteLockWithTracking);

        checkpointMarkersStorage = new CheckpointMarkersStorage(storagePath);

        checkpointWorkflow = new CheckpointWorkflow(
                checkpointConfig,
                checkpointMarkersStorage,
                checkpointReadWriteLock,
                dataRegions
        );

        checkpointPagesWriterFactory = new CheckpointPagesWriterFactory(
                IgniteLogger.forClass(CheckpointPagesWriterFactory.class),
                (fullPage, buf, tag) -> filePageStoreManager.write(fullPage.groupId(), fullPage.pageId(), buf, tag, true),
                pageSize
        );

        checkpointer = new Checkpointer(
                IgniteLogger.forClass(Checkpoint.class),
                igniteInstanceName,
                workerListener,
                longJvmPauseDetector,
                checkpointWorkflow,
                checkpointPagesWriterFactory,
                checkpointConfig
        );

        checkpointTimeoutLock = new CheckpointTimeoutLock(
                IgniteLogger.forClass(CheckpointTimeoutLock.class),
                checkpointReadWriteLock,
                checkpointConfigView.readLockTimeout(),
                () -> safeToUpdateAllPageMemories(dataRegions),
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
     * @return Triggered checkpoint progress.
     */
    public CheckpointProgress forceCheckpoint(String reason) {
        return checkpointer.scheduleCheckpoint(0, reason);
    }

    /**
     * Returns {@link true} if it is safe for all {@link PageMemoryDataRegion data regions} to update their {@link PageMemory}.
     *
     * @param dataRegions Data regions.
     * @see PageMemoryImpl#safeToUpdate()
     */
    static boolean safeToUpdateAllPageMemories(Collection<PageMemoryDataRegion> dataRegions) {
        for (PageMemoryDataRegion dataRegion : dataRegions) {
            if (dataRegion.persistent() && !((PageMemoryImpl) dataRegion.pageMemory()).safeToUpdate()) {
                return false;
            }
        }

        return true;
    }
}
