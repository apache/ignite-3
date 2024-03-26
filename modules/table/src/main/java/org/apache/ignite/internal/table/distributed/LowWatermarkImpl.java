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
import static org.apache.ignite.internal.hlc.HybridTimestamp.MIN_VALUE;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.schema.configuration.LowWatermarkConfiguration;
import org.apache.ignite.internal.schema.configuration.LowWatermarkConfigurationSchema;
import org.apache.ignite.internal.table.distributed.message.GetLowWatermarkRequest;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Class to manage the low watermark.
 *
 * <p>Low watermark updating occurs in the following cases (will only be updated if the new value is greater than the existing one):</p>
 * <ul>
 *     <li>By calling {@link #updateAndNotify(HybridTimestamp)}.</li>
 *     <li>In the background every {@link LowWatermarkConfigurationSchema#updateFrequency} milliseconds, a new value will be created in
 *     {@link #createNewLowWatermarkCandidate()}.</li>
 * </ul>
 *
 * <p>Algorithm for updating a new low watermark:</p>
 * <ul>
 *     <li>Wait for all RO transactions up to the new value to complete locally, while new RO transactions with read timestamp below
 *     the new low watermark will be rejected.</li>
 *     <li>Write the new value in vault by {@link #LOW_WATERMARK_VAULT_KEY}.</li>
 *     <li>Notify all {@link LowWatermarkChangedListener}s that the new watermark has changed and wait until they complete processing the
 *     event.</li>
 * </ul>
 */
public class LowWatermarkImpl implements LowWatermark, IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(LowWatermarkImpl.class);

    static final ByteArray LOW_WATERMARK_VAULT_KEY = new ByteArray("low-watermark");

    private static final TableMessagesFactory MESSAGES_FACTORY = new TableMessagesFactory();

    private final LowWatermarkConfiguration lowWatermarkConfig;

    private final HybridClock clock;

    private final TxManager txManager;

    private final VaultManager vaultManager;

    private final List<LowWatermarkChangedListener> updateListeners = new CopyOnWriteArrayList<>();

    // TODO: IGNITE-21772 Consider using a shared pool to update a low watermark
    private final ScheduledExecutorService scheduledThreadPool;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    private volatile @Nullable HybridTimestamp lowWatermark;

    private final AtomicReference<ScheduledFuture<?>> lastScheduledTaskFuture = new AtomicReference<>();

    private final FailureProcessor failureProcessor;

    private final ReadWriteLock updateLowWatermarkLock = new ReentrantReadWriteLock();

    private final MessagingService messagingService;

    private final AtomicReference<LowWatermarkCandidate> lowWatermarkCandidate = new AtomicReference<>(
            new LowWatermarkCandidate(MIN_VALUE, nullCompletedFuture())
    );

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param lowWatermarkConfig Low watermark configuration.
     * @param clock A hybrid logical clock.
     * @param txManager Transaction manager.
     * @param vaultManager Vault manager.
     * @param failureProcessor Failure processor tha is used to handle critical errors.
     */
    public LowWatermarkImpl(
            String nodeName,
            LowWatermarkConfiguration lowWatermarkConfig,
            HybridClock clock,
            TxManager txManager,
            VaultManager vaultManager,
            FailureProcessor failureProcessor,
            MessagingService messagingService
    ) {
        this.lowWatermarkConfig = lowWatermarkConfig;
        this.clock = clock;
        this.txManager = txManager;
        this.vaultManager = vaultManager;
        this.failureProcessor = failureProcessor;
        this.messagingService = messagingService;

        scheduledThreadPool = Executors.newSingleThreadScheduledExecutor(
                NamedThreadFactory.create(nodeName, "low-watermark-updater", LOG)
        );
    }

    @Override
    public CompletableFuture<Void> start() {
        return inBusyLockAsync(busyLock, () -> {
            setLowWatermarkOnRecovery(readLowWatermarkFromVault());

            messagingService.addMessageHandler(TableMessageGroup.class, this::onReceiveNetworkMessage);

            return nullCompletedFuture();
        });
    }

    /**
     * Schedule low watermark updates.
     */
    public void scheduleUpdates() {
        inBusyLock(busyLock, () -> {
            HybridTimestamp lowWatermarkCandidate = lowWatermark;

            if (lowWatermarkCandidate == null) {
                LOG.info("Previous value of the low watermark was not found, will schedule to update it");

                scheduleUpdateLowWatermarkBusy();

                return;
            }

            LOG.info("Low watermark has been scheduled to be updated: {}", lowWatermarkCandidate);

            // TODO IGNTIE-21751: txManager should read lwm on recocvery instead.
            //  Other components uses the lwm recovered from the vault, so notification shouldn't have any effect.
            // TODO: IGNITE-21773 No need to notify listeners at node start
            txManager.updateLowWatermark(lowWatermarkCandidate)
                    .thenComposeAsync(unused -> inBusyLock(busyLock, () -> notifyListeners(lowWatermarkCandidate)), scheduledThreadPool)
                    .whenComplete((unused, throwable) -> {
                        if (throwable == null) {
                            inBusyLock(busyLock, this::scheduleUpdateLowWatermarkBusy);
                        } else if (!(throwable instanceof NodeStoppingException)) {
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
    public void stop() {
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

    @Override
    public @Nullable HybridTimestamp getLowWatermark() {
        return inBusyLock(busyLock, () -> lowWatermark);
    }

    @Override
    public void addUpdateListener(LowWatermarkChangedListener listener) {
        updateListeners.add(listener);
    }

    @Override
    public void removeUpdateListener(LowWatermarkChangedListener listener) {
        updateListeners.remove(listener);
    }

    private CompletableFuture<Void> notifyListeners(HybridTimestamp lowWatermark) {
        if (updateListeners.isEmpty()) {
            return nullCompletedFuture();
        }

        var res = new ArrayList<CompletableFuture<?>>();
        for (LowWatermarkChangedListener updateListener : updateListeners) {
            res.add(updateListener.onLwmChanged(lowWatermark));
        }

        return CompletableFuture.allOf(res.toArray(CompletableFuture[]::new));
    }

    @Override
    public void getLowWatermarkSafe(Consumer<@Nullable HybridTimestamp> consumer) {
        inBusyLock(busyLock, () -> {
            updateLowWatermarkLock.readLock().lock();

            try {
                consumer.accept(lowWatermark);
            } finally {
                updateLowWatermarkLock.readLock().unlock();
            }
        });
    }

    private void scheduleUpdateLowWatermarkBusy() {
        ScheduledFuture<?> previousScheduledFuture = this.lastScheduledTaskFuture.get();

        assert previousScheduledFuture == null || previousScheduledFuture.isDone() : "previous scheduled task has not finished";

        ScheduledFuture<?> newScheduledFuture = scheduledThreadPool.schedule(
                () -> updateLowWatermark(createNewLowWatermarkCandidate()),
                lowWatermarkConfig.updateFrequency().value(),
                TimeUnit.MILLISECONDS
        );

        boolean casResult = lastScheduledTaskFuture.compareAndSet(previousScheduledFuture, newScheduledFuture);

        assert casResult : "only one scheduled task is expected";
    }

    HybridTimestamp createNewLowWatermarkCandidate() {
        HybridTimestamp now = clock.now();

        return now.addPhysicalTime(-lowWatermarkConfig.dataAvailabilityTime().value() - getMaxClockSkew());
    }

    private long getMaxClockSkew() {
        // TODO: IGNITE-19287 Add Implementation
        return HybridTimestamp.CLOCK_SKEW;
    }

    private void setLowWatermark(HybridTimestamp newLowWatermark) {
        updateLowWatermarkLock.writeLock().lock();

        try {
            assert lowWatermark == null || newLowWatermark.compareTo(lowWatermark) > 0 :
                    "Low watermark should only grow: [cur=" + lowWatermark + ", new=" + newLowWatermark + "]";

            lowWatermark = newLowWatermark;
        } finally {
            updateLowWatermarkLock.writeLock().unlock();
        }
    }

    private void setLowWatermarkOnRecovery(@Nullable HybridTimestamp newLowWatermark) {
        updateLowWatermarkLock.writeLock().lock();

        try {
            lowWatermark = newLowWatermark;
        } finally {
            updateLowWatermarkLock.writeLock().unlock();
        }
    }

    void onReceiveNetworkMessage(NetworkMessage message, ClusterNode sender, @Nullable Long correlationId) {
        inBusyLock(busyLock, () -> {
            if (!(message instanceof GetLowWatermarkRequest)) {
                return;
            }

            assert correlationId != null : sender;

            messagingService.respond(
                    sender,
                    MESSAGES_FACTORY.getLowWatermarkResponse().lowWatermark(hybridTimestampToLong(lowWatermark)).build(),
                    correlationId
            );
        });
    }

    @Override
    public void updateLowWatermark(HybridTimestamp newLowWatermark) {
        inBusyLock(busyLock, () -> {
            LowWatermarkCandidate newLowWatermarkCandidate = new LowWatermarkCandidate(newLowWatermark, new CompletableFuture<>());
            LowWatermarkCandidate oldLowWatermarkCandidate;

            do {
                oldLowWatermarkCandidate = lowWatermarkCandidate.get();

                // If another candidate contains a higher low watermark, then there is no need to update.
                if (oldLowWatermarkCandidate.lowWatermark().compareTo(newLowWatermark) >= 0) {
                    return;
                }
            } while (!lowWatermarkCandidate.compareAndSet(oldLowWatermarkCandidate, newLowWatermarkCandidate));

            // We will start the update as soon as the previous one finishes.
            oldLowWatermarkCandidate.updateFuture()
                    .thenComposeAsync(unused -> updateAndNotify(newLowWatermark), scheduledThreadPool)
                    .whenComplete((unused, throwable) -> {
                        if (throwable != null) {
                            newLowWatermarkCandidate.updateFuture().completeExceptionally(throwable);
                        } else {
                            newLowWatermarkCandidate.updateFuture().complete(null);
                        }
                    });
        });
    }

    CompletableFuture<Void> updateAndNotify(HybridTimestamp newLowWatermark) {
        return inBusyLockAsync(busyLock, () -> {
            // Wait until all the read-only transactions are finished before the new candidate, since no new RO transactions could be
            // created, then we can safely promote the candidate as a new low watermark, store it in vault, and we can safely start cleaning
            // up the stale/junk data in the tables.
            return txManager.updateLowWatermark(newLowWatermark)
                    .thenComposeAsync(unused -> inBusyLock(busyLock, () -> {
                        vaultManager.put(LOW_WATERMARK_VAULT_KEY, ByteUtils.toBytes(newLowWatermark));

                        setLowWatermark(newLowWatermark);

                        return notifyListeners(newLowWatermark);
                    }), scheduledThreadPool)
                    .whenComplete((unused, throwable) -> {
                        if (throwable != null) {
                            if (!(throwable instanceof NodeStoppingException)) {
                                LOG.error("Failed to update low watermark, will schedule again: {}", throwable, newLowWatermark);

                                failureProcessor.process(new FailureContext(CRITICAL_ERROR, throwable));

                                inBusyLock(busyLock, this::scheduleUpdateLowWatermarkBusy);
                            }
                        } else {
                            LOG.info("Successful low watermark update: {}", newLowWatermark);

                            inBusyLock(busyLock, this::scheduleUpdateLowWatermarkBusy);
                        }
                    });
        });
    }
}
