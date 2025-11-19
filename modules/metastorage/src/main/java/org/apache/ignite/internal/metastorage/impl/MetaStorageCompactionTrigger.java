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

package org.apache.ignite.internal.metastorage.impl;

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockSafe;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metastorage.command.CompactionCommand;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Metastorage compaction trigger.
 *
 * <p>Algorithm:</p>
 * <ol>
 *     <li>Metastorage leader waits locally for the start of scheduled compaction.</li>
 *     <li>Metastorage leader locally calculates revision for compaction: it takes the current safe time and subtracts the data
 *     availability time and uses that timestamp to get the revision.</li>
 *     <li>If the revision is less than or equal to the last compacted revision, then go to point 6.</li>
 *     <li>Metastorage leader creates and sends a {@link CompactionCommand} (see the command description what each node will do) with a new
 *     revision for compaction.</li>
 *     <li>Metastorage leader locally gets notification of the completion of the local compaction for the new revision.</li>
 *     <li>Metastorage leader locally schedules a new start of compaction.</li>
 * </ol>
 *
 * <p>About recovery:</p>
 * <ul>
 *     <li>At the start of the component, we will start a local compaction for the compaction revision from
 *     {@link MetaStorageManager#recoveryFinishedFuture}.</li>
 *     <li>{@link CompactionCommand}s that were received before the {@link MetaStorageManager#deployWatches} will start a local compaction
 *     in the {@link MetaStorageManager#deployWatches}.</li>
 * </ul>
 */
public class MetaStorageCompactionTrigger implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageCompactionTrigger.class);

    private final String localNodeName;

    private final KeyValueStorage storage;

    private final MetaStorageManagerImpl metaStorageManager;

    private final FailureProcessor failureProcessor;

    private final ReadOperationForCompactionTracker readOperationForCompactionTracker;

    private final MetaStorageCompactionTriggerConfiguration config;

    private final ScheduledExecutorService compactionExecutor;

    /** Guarded by {@link #lock}. */
    private @Nullable ScheduledFuture<?> lastScheduledFuture;

    /** Guarded by {@link #lock}. */
    private boolean isLocalNodeLeader;

    /**
     * Whether that the component has started. It is expected that the component will be started after the distributed configuration has
     * started, so that we can get the configuration value correctly and make a compaction with predictable behavior.
     *
     * <p>Guarded by {@link #lock}.</p>
     */
    private boolean started;

    private final Lock lock = new ReentrantLock();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param localNodeName Local node name.
     * @param storage Storage.
     * @param metaStorageManager Metastorage manager.
     * @param failureProcessor Failure processor.
     * @param readOperationForCompactionTracker Tracker of read operations, both local and from the leader.
     * @param systemDistributedConfig Distributed system configuration.
     */
    public MetaStorageCompactionTrigger(
            String localNodeName,
            KeyValueStorage storage,
            MetaStorageManagerImpl metaStorageManager,
            FailureProcessor failureProcessor,
            ReadOperationForCompactionTracker readOperationForCompactionTracker,
            SystemDistributedConfiguration systemDistributedConfig
    ) {
        this.localNodeName = localNodeName;
        this.storage = storage;
        this.metaStorageManager = metaStorageManager;
        this.failureProcessor = failureProcessor;
        this.readOperationForCompactionTracker = readOperationForCompactionTracker;

        config = new MetaStorageCompactionTriggerConfiguration(systemDistributedConfig);

        compactionExecutor = Executors.newSingleThreadScheduledExecutor(
                IgniteThreadFactory.create(localNodeName, "metastorage-compaction-executor", LOG)
        );

        storage.registerCompactionRevisionUpdateListener(this::onCompactionRevisionUpdate);

        metaStorageManager.addElectionListener(this::onLeaderElected);
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return inBusyLockAsync(busyLock, () -> {
            lock.lock();

            try {
                config.init();

                startCompactionOnRecoveryInBackground();

                started = true;

                scheduleNextCompactionBusy();

                return nullCompletedFuture();
            } finally {
                lock.unlock();
            }
        });
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        cancelLastScheduledFutureBusy();

        IgniteUtils.shutdownAndAwaitTermination(compactionExecutor, 10, TimeUnit.SECONDS);

        return nullCompletedFuture();
    }

    private void doCompactionBusy() {
        lock.lock();

        try {
            if (!isLocalNodeLeader) {
                return;
            }

            HybridTimestamp candidateCompactionRevisionTimestamp = createCandidateCompactionRevisionTimestampBusy();

            Long newCompactionRevision = calculateCandidateCompactionRevisionBusy(candidateCompactionRevisionTimestamp);

            if (newCompactionRevision == null) {
                scheduleNextCompactionBusy();
            } else {
                metaStorageManager.sendCompactionCommand(newCompactionRevision)
                        .whenComplete((unused, throwable) -> {
                            if (throwable != null) {
                                if (!hasCause(throwable, NodeStoppingException.class)) {
                                    String errorMessage = String.format(
                                            "Unknown error occurred while sending the metastorage compaction command: "
                                                    + "[newCompactionRevision=%s]",
                                            newCompactionRevision
                                    );
                                    failureProcessor.process(new FailureContext(throwable, errorMessage));

                                    inBusyLockSafe(busyLock, this::scheduleNextCompactionBusy);
                                }
                            }
                        });
            }
        } catch (Throwable t) {
            failureProcessor.process(new FailureContext(t, "Unknown error on new metastorage compaction revision scheduling"));

            inBusyLockSafe(busyLock, this::scheduleNextCompactionBusy);
        } finally {
            lock.unlock();
        }
    }

    private HybridTimestamp createCandidateCompactionRevisionTimestampBusy() {
        HybridTimestamp safeTime = metaStorageManager.clusterTime().currentSafeTime();

        long dataAvailabilityTime = config.dataAvailabilityTime();

        return safeTime.getPhysical() <= dataAvailabilityTime
                ? HybridTimestamp.MIN_VALUE
                : safeTime.subtractPhysicalTime(dataAvailabilityTime);
    }

    /** Returns {@code null} if there is no need to compact yet. */
    private @Nullable Long calculateCandidateCompactionRevisionBusy(HybridTimestamp candidateTimestamp) {
        try {
            long candidateCompactionRevision = storage.revisionByTimestamp(candidateTimestamp);
            long currentStorageRevision = storage.revision();

            if (candidateCompactionRevision >= currentStorageRevision) {
                candidateCompactionRevision = currentStorageRevision - 1;
            }

            return candidateCompactionRevision <= storage.getCompactionRevision() ? null : candidateCompactionRevision;
        } catch (CompactedException exception) {
            // Revision has already been compacted, we need to plan the next compaction.
            return null;
        }
    }

    private void scheduleNextCompactionBusy() {
        lock.lock();

        try {
            if (started && isLocalNodeLeader) {
                lastScheduledFuture = compactionExecutor.schedule(
                        () -> inBusyLock(busyLock, this::doCompactionBusy),
                        config.interval(),
                        MILLISECONDS
                );
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * It should be used precisely at the complete of the compaction, so as not to schedule a new update together with the event of
     * electing a local node as a new leader.
     */
    private void scheduleNextCompactionIfNotScheduleBusy() {
        lock.lock();

        try {
            if (started && isLocalNodeLeader) {
                ScheduledFuture<?> lastScheduledFuture = this.lastScheduledFuture;

                if (lastScheduledFuture == null || lastScheduledFuture.isDone()) {
                    this.lastScheduledFuture = compactionExecutor.schedule(
                            () -> inBusyLock(busyLock, this::doCompactionBusy),
                            config.interval(),
                            MILLISECONDS
                    );
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /** Invoked when the metastorage compaction revision is updated. */
    private void onCompactionRevisionUpdate(long compactionRevision) {
        inBusyLockSafe(busyLock, () -> onCompactionRevisionUpdateBusy(compactionRevision));
    }

    private void onCompactionRevisionUpdateBusy(long compactionRevision) {
        supplyAsync(() -> readOperationForCompactionTracker.collect(compactionRevision), compactionExecutor)
                .thenComposeAsync(Function.identity(), compactionExecutor)
                .thenRunAsync(() -> storage.compact(compactionRevision), compactionExecutor)
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        if (!hasCause(throwable, NodeStoppingException.class, RejectedExecutionException.class)) {
                            String errorMessage = String.format(
                                    "Unknown error on new metastorage compaction revision: %s",
                                    compactionRevision
                            );
                            failureProcessor.process(new FailureContext(throwable, errorMessage));
                        }
                    }

                    inBusyLockSafe(busyLock, this::scheduleNextCompactionIfNotScheduleBusy);
                });
    }

    /** Invoked when a new leader is elected. */
    private void onLeaderElected(InternalClusterNode newLeader) {
        inBusyLockSafe(busyLock, () -> onLeaderElectedBusy(newLeader));
    }

    private void onLeaderElectedBusy(InternalClusterNode newLeader) {
        lock.lock();

        try {
            if (localNodeName.equals(newLeader.name())) {
                isLocalNodeLeader = true;

                scheduleNextCompactionBusy();
            } else {
                isLocalNodeLeader = false;

                cancelLastScheduledFutureBusy();
            }
        } finally {
            lock.unlock();
        }
    }

    private void cancelLastScheduledFutureBusy() {
        lock.lock();

        try {
            ScheduledFuture<?> lastScheduledFuture = this.lastScheduledFuture;

            if (lastScheduledFuture != null) {
                lastScheduledFuture.cancel(true);
            }

            this.lastScheduledFuture = null;
        } finally {
            lock.unlock();
        }
    }

    private void startCompactionOnRecoveryInBackground() {
        CompletableFuture<Revisions> recoveryFuture = metaStorageManager.recoveryFinishedFuture();

        assert recoveryFuture.isDone();

        long recoveredCompactionRevision = recoveryFuture.join().compactionRevision();

        if (recoveredCompactionRevision != -1) {
            runAsync(() -> inBusyLockSafe(busyLock, () -> storage.compact(recoveredCompactionRevision)), compactionExecutor)
                    .whenComplete((unused, throwable) -> {
                        if (throwable != null) {
                            if (!hasCause(throwable, NodeStoppingException.class, RejectedExecutionException.class)) {
                                String errorMessage = String.format(
                                        "Unknown error during metastore compaction launched on node recovery: [compactionRevision=%s]",
                                        recoveredCompactionRevision
                                );
                                failureProcessor.process(new FailureContext(throwable, errorMessage));
                            }
                        }
                    });
        }
    }
}
