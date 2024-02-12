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

package org.apache.ignite.internal.table.distributed;

import static org.apache.ignite.internal.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.configuration.LowWatermarkConfiguration;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.jetbrains.annotations.Nullable;

/**
 * Class to manage the low watermark.
 *
 * <p>Low watermark is the node's local time, which ensures that read-only transactions have completed by this time, and new read-only
 * transactions will only be created after this time, and we can safely delete obsolete/garbage data such as: obsolete versions of table
 * rows, remote indexes, remote tables, etc.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-91%3A+Transaction+protocol">IEP-91</a>
 */
public class LowWatermark implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(LowWatermark.class);

    static final ByteArray LOW_WATERMARK_VAULT_KEY = new ByteArray("low-watermark");

    private final LowWatermarkConfiguration lowWatermarkConfig;

    private final HybridClock clock;

    private final TxManager txManager;

    private final VaultManager vaultManager;

    private final MvGc mvGc;

    private final ScheduledExecutorService scheduledThreadPool;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    private volatile HybridTimestamp lowWatermark;

    private final AtomicReference<ScheduledFuture<?>> lastScheduledTaskFuture = new AtomicReference<>();

    private final FailureProcessor failureProcessor;

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param lowWatermarkConfig Low watermark configuration.
     * @param clock A hybrid logical clock.
     * @param txManager Transaction manager.
     * @param vaultManager Vault manager.
     * @param mvGc MVCC garbage collector.
     * @param failureProcessor Failure processor tha is used to handle critical errors.
     */
    public LowWatermark(
            String nodeName,
            LowWatermarkConfiguration lowWatermarkConfig,
            HybridClock clock,
            TxManager txManager,
            VaultManager vaultManager,
            MvGc mvGc,
            FailureProcessor failureProcessor
    ) {
        this.lowWatermarkConfig = lowWatermarkConfig;
        this.clock = clock;
        this.txManager = txManager;
        this.vaultManager = vaultManager;
        this.mvGc = mvGc;
        this.failureProcessor = failureProcessor;

        scheduledThreadPool = Executors.newSingleThreadScheduledExecutor(
                NamedThreadFactory.create(nodeName, "low-watermark-updater", LOG)
        );
    }

    /**
     * Starts the watermark manager.
     */
    public void start() {
        inBusyLock(busyLock, () -> {
            HybridTimestamp lowWatermark = readLowWatermarkFromVault();

            if (lowWatermark == null) {
                LOG.info("Previous value of the low watermark was not found, will schedule to update it");

                scheduleUpdateLowWatermarkBusy();

                return;
            }

            LOG.info(
                    "Low watermark has been successfully retrieved from the vault and is scheduled to be updated: {}",
                    lowWatermark
            );

            txManager.updateLowWatermark(lowWatermark)
                    .thenRun(() -> inBusyLock(busyLock, () -> {
                        this.lowWatermark = lowWatermark;

                        runGcAndScheduleUpdateLowWatermarkBusy(lowWatermark);
                    }))
                    .whenComplete((unused, throwable) -> {
                        if (throwable != null && !(throwable instanceof NodeStoppingException)) {
                            LOG.error("Error during the Watermark manager start", throwable);

                            failureProcessor.process(new FailureContext(CRITICAL_ERROR, throwable));

                            inBusyLock(busyLock, this::scheduleUpdateLowWatermarkBusy);
                        }
                    });
        });
    }

    private @Nullable HybridTimestamp readLowWatermarkFromVault() {
        VaultEntry vaultEntry = vaultManager.get(LOW_WATERMARK_VAULT_KEY);

        return vaultEntry == null ? null : ByteUtils.fromBytes(vaultEntry.value());
    }

    @Override
    public void close() {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        ScheduledFuture<?> lastScheduledTaskFuture = this.lastScheduledTaskFuture.get();

        if (lastScheduledTaskFuture != null) {
            lastScheduledTaskFuture.cancel(true);
        }

        IgniteUtils.shutdownAndAwaitTermination(scheduledThreadPool, 10, TimeUnit.SECONDS);
    }

    /**
     * Returns the current low watermark, {@code null} means no low watermark has been assigned yet.
     */
    public @Nullable HybridTimestamp getLowWatermark() {
        return lowWatermark;
    }

    void updateLowWatermark() {
        inBusyLock(busyLock, () -> {
            HybridTimestamp lowWatermarkCandidate = createNewLowWatermarkCandidate();

            // Wait until all the read-only transactions are finished before the new candidate, since no new RO transactions could be
            // created, then we can safely promote the candidate as a new low watermark, store it in vault, and we can safely start cleaning
            // up the stale/junk data in the tables.
            txManager.updateLowWatermark(lowWatermarkCandidate)
                    .thenRunAsync(() -> inBusyLock(busyLock, () -> {
                        vaultManager.put(LOW_WATERMARK_VAULT_KEY, ByteUtils.toBytes(lowWatermarkCandidate));

                        lowWatermark = lowWatermarkCandidate;

                        runGcAndScheduleUpdateLowWatermarkBusy(lowWatermarkCandidate);
                    }), scheduledThreadPool)
                    .whenComplete((unused, throwable) -> {
                        if (throwable != null) {
                            if (!(throwable instanceof NodeStoppingException)) {
                                LOG.error("Failed to update low watermark, will schedule again: {}", throwable, lowWatermarkCandidate);

                                inBusyLock(busyLock, this::scheduleUpdateLowWatermarkBusy);
                            }
                        } else {
                            LOG.info("Successful low watermark update: {}", lowWatermarkCandidate);
                        }
                    });
        });
    }

    private void runGcAndScheduleUpdateLowWatermarkBusy(HybridTimestamp lowWatermark) {
        mvGc.updateLowWatermark(lowWatermark);

        scheduleUpdateLowWatermarkBusy();
    }

    private void scheduleUpdateLowWatermarkBusy() {
        ScheduledFuture<?> previousScheduledFuture = this.lastScheduledTaskFuture.get();

        assert previousScheduledFuture == null || previousScheduledFuture.isDone() : "previous scheduled task has not finished";

        ScheduledFuture<?> newScheduledFuture = scheduledThreadPool.schedule(
                this::updateLowWatermark,
                lowWatermarkConfig.updateFrequency().value(),
                TimeUnit.MILLISECONDS
        );

        boolean casResult = lastScheduledTaskFuture.compareAndSet(previousScheduledFuture, newScheduledFuture);

        assert casResult : "only one scheduled task is expected";
    }

    HybridTimestamp createNewLowWatermarkCandidate() {
        HybridTimestamp now = clock.now();

        HybridTimestamp lowWatermarkCandidate = now.addPhysicalTime(
                -lowWatermarkConfig.dataAvailabilityTime().value() - getMaxClockSkew()
        );

        HybridTimestamp lowWatermark = this.lowWatermark;

        assert lowWatermark == null || lowWatermarkCandidate.compareTo(lowWatermark) > 0 :
                "lowWatermark=" + lowWatermark + ", lowWatermarkCandidate=" + lowWatermarkCandidate;

        return lowWatermarkCandidate;
    }

    private long getMaxClockSkew() {
        // TODO: IGNITE-19287 Add Implementation
        return HybridTimestamp.CLOCK_SKEW;
    }
}
