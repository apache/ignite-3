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

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockSafe;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.command.CompactionCommand;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.time.ClusterTime;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
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
 */
// TODO: IGNITE-23280 Turn on compaction
class MetaStorageCompactionTrigger implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageCompactionTrigger.class);

    /** System property that defines compaction start interval (in milliseconds). Default value is {@link Long#MAX_VALUE}. */
    public static final String COMPACTION_INTERVAL_PROPERTY = "IGNITE_COMPACTION_INTERVAL";

    /** System property that defines data availability time (in milliseconds). Default value is {@link Long#MAX_VALUE}. */
    public static final String COMPACTION_DATA_AVAILABILITY_TIME_PROPERTY = "IGNITE_COMPACTION_DATA_AVAILABILITY_TIME";

    private final String localNodeName;

    private final KeyValueStorage storage;

    private final CompletableFuture<MetaStorageServiceImpl> metastorageServiceFuture;

    private final ClusterTime clusterTime;

    private final ReadOperationForCompactionTracker readOperationForCompactionTracker;

    private final ScheduledExecutorService compactionExecutor;

    /** Guarded by {@link #lock}. */
    private @Nullable ScheduledFuture<?> lastScheduledFuture;

    /** Guarded by {@link #lock}. */
    private boolean isLocalNodeLeader;

    private final Lock lock = new ReentrantLock();

    /** Compaction start interval (in milliseconds). */
    // TODO: IGNITE-23279 Change configuration
    private final long startInterval = IgniteSystemProperties.getLong(COMPACTION_INTERVAL_PROPERTY, Long.MAX_VALUE);

    /** Data availability time (in milliseconds). */
    // TODO: IGNITE-23279 Change configuration
    private final long dataAvailabilityTime = IgniteSystemProperties.getLong(COMPACTION_DATA_AVAILABILITY_TIME_PROPERTY, Long.MAX_VALUE);

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param localNodeName Local node name.
     * @param storage Storage.
     * @param metastorageServiceFuture Metastorage service future.
     * @param clusterTime Cluster time.
     * @param readOperationForCompactionTracker Tracker of read operations, both local and from the leader.
     */
    MetaStorageCompactionTrigger(
            String localNodeName,
            KeyValueStorage storage,
            CompletableFuture<MetaStorageServiceImpl> metastorageServiceFuture,
            ClusterTime clusterTime,
            ReadOperationForCompactionTracker readOperationForCompactionTracker
    ) {
        this.localNodeName = localNodeName;
        this.storage = storage;
        this.metastorageServiceFuture = metastorageServiceFuture;
        this.clusterTime = clusterTime;
        this.readOperationForCompactionTracker = readOperationForCompactionTracker;

        compactionExecutor = Executors.newSingleThreadScheduledExecutor(
                NamedThreadFactory.create(localNodeName, "metastorage-compaction-executor", LOG)
        );
    }

    @Override
    public void close() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        cancelLastScheduledFutureBusy();

        IgniteUtils.shutdownAndAwaitTermination(compactionExecutor, 10, TimeUnit.SECONDS);
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
                metastorageServiceFuture.thenCompose(svc -> svc.sendCompactionCommand(newCompactionRevision))
                        .whenComplete((unused, throwable) -> {
                            if (throwable != null) {
                                Throwable cause = unwrapCause(throwable);

                                if (!(cause instanceof NodeStoppingException)) {
                                    LOG.error(
                                            "Unknown error occurred while sending the metastorage compaction command: "
                                                    + "[newCompactionRevision={}]",
                                            cause,
                                            newCompactionRevision
                                    );

                                    inBusyLockSafe(busyLock, this::scheduleNextCompactionBusy);
                                }
                            }
                        });
            }
        } catch (Throwable t) {
            LOG.error("Unknown error on new metastorage compaction revision scheduling", t);

            inBusyLockSafe(busyLock, this::scheduleNextCompactionBusy);
        } finally {
            lock.unlock();
        }
    }

    private HybridTimestamp createCandidateCompactionRevisionTimestampBusy() {
        HybridTimestamp safeTime = clusterTime.currentSafeTime();

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
            if (isLocalNodeLeader) {
                lastScheduledFuture = compactionExecutor.schedule(
                        () -> inBusyLock(busyLock, this::doCompactionBusy),
                        startInterval,
                        MILLISECONDS
                );
            }
        } finally {
            lock.unlock();
        }
    }

    /** Invokes when the metastorage compaction revision is updated. */
    void onCompactionRevisionUpdate(long compactionRevision) {
        inBusyLockSafe(busyLock, () -> onCompactionRevisionUpdateBusy(compactionRevision));
    }

    private void onCompactionRevisionUpdateBusy(long compactionRevision) {
        supplyAsync(() -> readOperationForCompactionTracker.collect(compactionRevision), compactionExecutor)
                .thenComposeAsync(Function.identity(), compactionExecutor)
                .thenRunAsync(() -> storage.compact(compactionRevision), compactionExecutor)
                .whenComplete((unused, throwable) -> {
                    if (throwable == null) {
                        LOG.info("Metastore compaction completed successfully: [compactionRevision={}]", compactionRevision);
                    } else {
                        Throwable cause = unwrapCause(throwable);

                        if (!(cause instanceof NodeStoppingException)) {
                            LOG.error(
                                    "Unknown error on new metastorage compaction revision: {}",
                                    cause,
                                    compactionRevision
                            );
                        }
                    }

                    inBusyLockSafe(busyLock, this::scheduleNextCompactionBusy);
                });
    }

    /** Invokes when a new leader is elected. */
    void onLeaderElected(ClusterNode newLeader) {
        inBusyLockSafe(busyLock, () -> onLeaderElectedBusy(newLeader));
    }

    private void onLeaderElectedBusy(ClusterNode newLeader) {
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
}
