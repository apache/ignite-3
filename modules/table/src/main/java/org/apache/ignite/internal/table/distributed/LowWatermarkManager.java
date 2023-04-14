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

import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.configuration.LowWatermarkConfiguration;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;

/**
 * Class to manage the low watermark.
 */
public class LowWatermarkManager implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(LowWatermarkManager.class);

    static final ByteArray LOW_WATERMARK_VAULT_KEY = new ByteArray("low-watermark");

    private final LowWatermarkConfiguration lowWatermarkConfig;

    private final HybridClock clock;

    private final TxManager txManager;

    private final VaultManager vaultManager;

    private final MvGc mvGc;

    private final ScheduledExecutorService scheduledThreadPool;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    private final AtomicReference<HybridTimestamp> lowWatermark = new AtomicReference<>();

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param lowWatermarkConfig Low watermark configuration.
     * @param clock A hybrid logical clock.
     * @param txManager Transaction manager.
     * @param vaultManager Vault manager.
     */
    public LowWatermarkManager(
            String nodeName,
            LowWatermarkConfiguration lowWatermarkConfig,
            HybridClock clock,
            TxManager txManager,
            VaultManager vaultManager,
            MvGc mvGc
    ) {
        this.lowWatermarkConfig = lowWatermarkConfig;
        this.clock = clock;
        this.txManager = txManager;
        this.vaultManager = vaultManager;
        this.mvGc = mvGc;

        scheduledThreadPool = Executors.newSingleThreadScheduledExecutor(
                NamedThreadFactory.create(nodeName, "low-watermark-updater", LOG)
        );
    }

    /**
     * Starts the watermark manager.
     */
    public void start() {
        inBusyLock(busyLock, () -> {
            vaultManager.get(LOW_WATERMARK_VAULT_KEY)
                    .thenApply(vaultEntry -> inBusyLock(busyLock, () -> {
                        if (vaultEntry == null) {
                            scheduleUpdateLowWatermarkBusy();

                            return null;
                        }

                        HybridTimestamp lowWatermark = ByteUtils.fromBytes(vaultEntry.value());

                        this.lowWatermark.set(lowWatermark);

                        runGcAndScheduleUpdateLowWatermarkBusy(lowWatermark);

                        return lowWatermark;
                    }))
                    .whenComplete((lowWatermark, throwable) -> {
                        if (throwable != null) {
                            if (!(throwable instanceof NodeStoppingException)) {
                                LOG.error("Error getting low watermark", throwable);

                                // TODO: IGNITE-16899 Perhaps we need to fail the node by FailureHandler
                            }
                        } else {
                            LOG.info(
                                    "Low watermark has been successfully got from the vault and is scheduled to be updated: {}",
                                    lowWatermark
                            );
                        }
                    });
        });
    }

    @Override
    public void close() throws Exception {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        IgniteUtils.shutdownAndAwaitTermination(scheduledThreadPool, 10, TimeUnit.SECONDS);
    }

    /**
     * Returns the current low watermark, {@code null} means no low watermark has been assigned yet.
     */
    public @Nullable HybridTimestamp getLowWatermark() {
        return lowWatermark.get();
    }

    void updateLowWatermark() {
        inBusyLock(busyLock, () -> {
            HybridTimestamp lowWatermarkCandidate = createNewLowWatermarkCandidate();

            txManager.updateLowerBoundToStartNewReadOnlyTransaction(lowWatermarkCandidate);

            txManager.getFutureAllReadOnlyTransactions(lowWatermarkCandidate)
                    .thenCompose(unused -> inBusyLock(
                            busyLock,
                            () -> vaultManager.put(LOW_WATERMARK_VAULT_KEY, ByteUtils.toBytes(lowWatermarkCandidate)))
                    )
                    .thenRun(() -> inBusyLock(busyLock, () -> {
                        lowWatermark.set(lowWatermarkCandidate);

                        runGcAndScheduleUpdateLowWatermarkBusy(lowWatermarkCandidate);
                    }))
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
        scheduledThreadPool.schedule(this::updateLowWatermark, lowWatermarkConfig.updateFrequency().value(), TimeUnit.MILLISECONDS);
    }

    HybridTimestamp createNewLowWatermarkCandidate() {
        HybridTimestamp now = clock.now();

        long newPhysicalTime = now.getPhysical() - lowWatermarkConfig.dataAvailabilityTime().value() - getMaxClockSkew();

        HybridTimestamp lowWatermarkCandidate = new HybridTimestamp(newPhysicalTime, now.getLogical());

        HybridTimestamp lowWatermark = this.lowWatermark.get();

        assert lowWatermark == null || lowWatermarkCandidate.compareTo(lowWatermark) > 0 :
                "lowWatermark=" + lowWatermark + ", lowWatermarkCandidate=" + lowWatermarkCandidate;

        return lowWatermarkCandidate;
    }

    private long getMaxClockSkew() {
        // TODO: IGNITE-19287 Add Implementation
        return 0;
    }
}
