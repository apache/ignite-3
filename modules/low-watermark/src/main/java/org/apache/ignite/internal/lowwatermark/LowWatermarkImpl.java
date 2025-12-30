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

package org.apache.ignite.internal.lowwatermark;

import static org.apache.ignite.configuration.notifications.ConfigurationListener.fromConsumer;
import static org.apache.ignite.internal.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.internal.hlc.HybridTimestamp.MIN_VALUE;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent.LOW_WATERMARK_CHANGED;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.ScheduledUpdateLowWatermarkTask.State;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent;
import org.apache.ignite.internal.lowwatermark.event.LowWatermarkEventParameters;
import org.apache.ignite.internal.lowwatermark.message.GetLowWatermarkRequest;
import org.apache.ignite.internal.lowwatermark.message.LowWatermarkMessageGroup;
import org.apache.ignite.internal.lowwatermark.message.LowWatermarkMessagesFactory;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.schema.configuration.LowWatermarkConfiguration;
import org.apache.ignite.internal.schema.configuration.LowWatermarkConfigurationSchema;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.jetbrains.annotations.Nullable;

/**
 * Class to manage the low watermark.
 *
 * <p>Low watermark updating occurs in the following cases (will only be updated if the new value is greater than the existing one):</p>
 * <ul>
 *     <li>By calling {@link #updateAndNotify(HybridTimestamp)}.</li>
 *     <li>In the background every {@link LowWatermarkConfigurationSchema#updateIntervalMillis} milliseconds, a new value will be created in
 *     {@link #createNewLowWatermarkCandidate()}.</li>
 * </ul>
 *
 * <p>Algorithm for updating a new low watermark:</p>
 * <ul>
 *     <li>Write the new value in vault by {@link #LOW_WATERMARK_VAULT_KEY}.</li>
 *     <li>Notify all {@link LowWatermarkEvent} listeners that the new watermark has changed and wait until they complete processing the
 *     event.</li>
 * </ul>
 *
 * @see LowWatermarkEvent
 */
public class LowWatermarkImpl extends AbstractEventProducer<LowWatermarkEvent, LowWatermarkEventParameters> implements LowWatermark,
        IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(LowWatermarkImpl.class);

    public static final ByteArray LOW_WATERMARK_VAULT_KEY = new ByteArray("low-watermark");

    private static final LowWatermarkMessagesFactory MESSAGES_FACTORY = new LowWatermarkMessagesFactory();

    private final LowWatermarkConfiguration lowWatermarkConfig;

    private final ClockService clockService;

    private final VaultManager vaultManager;

    // TODO: IGNITE-21772 Consider using a shared pool to update a low watermark
    private final ScheduledExecutorService scheduledThreadPool;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    private volatile @Nullable HybridTimestamp lowWatermark;

    private final AtomicReference<ScheduledFuture<?>> lastScheduledTaskFuture = new AtomicReference<>();

    private final FailureManager failureManager;

    private final ReadWriteLock updateLowWatermarkLock = new ReentrantReadWriteLock();

    private final MessagingService messagingService;

    private final AtomicReference<LowWatermarkCandidate> lowWatermarkCandidate = new AtomicReference<>(
            new LowWatermarkCandidate(MIN_VALUE, nullCompletedFuture())
    );

    private final Map<UUID, LowWatermarkLock> locks = new ConcurrentHashMap<>();

    /** Guarded by {@link #scheduleUpdateLowWatermarkTaskLock}. */
    private @Nullable ScheduledUpdateLowWatermarkTask lastScheduledUpdateLowWatermarkTask;

    private final Lock scheduleUpdateLowWatermarkTaskLock = new ReentrantLock();

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param lowWatermarkConfig Low watermark configuration.
     * @param clockService A hybrid logical clock.
     * @param vaultManager Vault manager.
     * @param failureManager Failure processor that is used to handle critical errors.
     */
    public LowWatermarkImpl(
            String nodeName,
            LowWatermarkConfiguration lowWatermarkConfig,
            ClockService clockService,
            VaultManager vaultManager,
            FailureManager failureManager,
            MessagingService messagingService
    ) {
        this.lowWatermarkConfig = lowWatermarkConfig;
        this.clockService = clockService;
        this.vaultManager = vaultManager;
        this.failureManager = failureManager;
        this.messagingService = messagingService;

        scheduledThreadPool = Executors.newSingleThreadScheduledExecutor(
                IgniteThreadFactory.create(nodeName, "low-watermark-updater", LOG)
        );
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return inBusyLockAsync(busyLock, () -> {
            HybridTimestamp lwmFromVault = readLowWatermarkFromVault();
            if (lwmFromVault != null) {
                setLowWatermarkOnRecovery(lwmFromVault);
            }

            messagingService.addMessageHandler(LowWatermarkMessageGroup.class, this::onReceiveNetworkMessage);

            lowWatermarkConfig.updateIntervalMillis().listen(fromConsumer(ctx -> scheduleUpdates()));

            return nullCompletedFuture();
        });
    }

    /** Schedule low watermark updates. */
    public void scheduleUpdates() {
        inBusyLock(busyLock, this::scheduleUpdateLowWatermarkBusy);
    }

    private @Nullable HybridTimestamp readLowWatermarkFromVault() {
        VaultEntry vaultEntry = vaultManager.get(LOW_WATERMARK_VAULT_KEY);

        return vaultEntry == null ? null : HybridTimestamp.fromBytes(vaultEntry.value());
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!closeGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        ScheduledFuture<?> lastScheduledTaskFuture = this.lastScheduledTaskFuture.get();

        if (lastScheduledTaskFuture != null) {
            lastScheduledTaskFuture.cancel(true);
        }

        IgniteUtils.shutdownAndAwaitTermination(scheduledThreadPool, 10, TimeUnit.SECONDS);

        return nullCompletedFuture();
    }

    @Override
    public @Nullable HybridTimestamp getLowWatermark() {
        return inBusyLock(busyLock, () -> lowWatermark);
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
        scheduleUpdateLowWatermarkTaskLock.lock();

        try {
            ScheduledUpdateLowWatermarkTask lastTask = lastScheduledUpdateLowWatermarkTask;
            ScheduledUpdateLowWatermarkTask newTask = new ScheduledUpdateLowWatermarkTask(this, State.NEW);

            State lastTaskState = lastTask == null ? State.COMPLETED : lastTask.state();

            switch (lastTaskState) {
                case NEW:
                    if (lastTask.tryCancel()) {
                        lastScheduledUpdateLowWatermarkTask = newTask;

                        scheduleUpdateLowWatermarkTaskBusy(newTask);
                    }

                    break;
                case IN_PROGRESS:
                    // In this case we don't need to schedule a new task because the current task that is in progress will schedule a new
                    // task when it finishes.
                    break;
                case COMPLETED:
                    lastScheduledUpdateLowWatermarkTask = newTask;

                    scheduleUpdateLowWatermarkTaskBusy(newTask);

                    break;
                default:
                    throw new AssertionError("Unknown state: " + lastTaskState);
            }
        } finally {
            scheduleUpdateLowWatermarkTaskLock.unlock();
        }
    }

    private void scheduleUpdateLowWatermarkTaskBusy(ScheduledUpdateLowWatermarkTask task) {
        ScheduledFuture<?> previousScheduledFuture = this.lastScheduledTaskFuture.get();

        if (previousScheduledFuture != null && !previousScheduledFuture.isDone()) {
            previousScheduledFuture.cancel(true);
        }

        ScheduledFuture<?> newScheduledFuture = scheduledThreadPool.schedule(
                task,
                lowWatermarkConfig.updateIntervalMillis().value(),
                TimeUnit.MILLISECONDS
        );

        boolean casResult = lastScheduledTaskFuture.compareAndSet(previousScheduledFuture, newScheduledFuture);

        assert casResult : "only one scheduled task is expected";
    }

    HybridTimestamp createNewLowWatermarkCandidate() {
        HybridTimestamp now = clockService.now();

        return now.subtractPhysicalTime(lowWatermarkConfig.dataAvailabilityTimeMillis().value() + clockService.maxClockSkewMillis());
    }

    private void setLowWatermark(HybridTimestamp newLowWatermark) {
        HybridTimestamp lwm = lowWatermark;

        assert lwm == null || newLowWatermark.compareTo(lwm) > 0 :
                "Low watermark should only grow: [cur=" + lwm + ", new=" + newLowWatermark + "]";

        lowWatermark = newLowWatermark;
    }

    @Override
    public void setLowWatermarkOnRecovery(HybridTimestamp newLowWatermark) {
        updateLowWatermarkLock.writeLock().lock();

        try {
            lowWatermark = newLowWatermark;

            persistWatermark(newLowWatermark);
        } finally {
            updateLowWatermarkLock.writeLock().unlock();
        }
    }

    void onReceiveNetworkMessage(NetworkMessage message, InternalClusterNode sender, @Nullable Long correlationId) {
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
        updateLowWatermarkAsync(newLowWatermark);
    }

    CompletableFuture<Void> updateLowWatermarkAsync(HybridTimestamp newLowWatermark) {
        return inBusyLockAsync(busyLock, () -> {
            LowWatermarkCandidate newLowWatermarkCandidate = new LowWatermarkCandidate(newLowWatermark, new CompletableFuture<>());
            LowWatermarkCandidate oldLowWatermarkCandidate;

            do {
                oldLowWatermarkCandidate = lowWatermarkCandidate.get();

                // If another candidate contains a higher low watermark, then there is no need to update.
                if (oldLowWatermarkCandidate.lowWatermark().compareTo(newLowWatermark) >= 0) {
                    return nullCompletedFuture();
                }
            } while (!lowWatermarkCandidate.compareAndSet(oldLowWatermarkCandidate, newLowWatermarkCandidate));

            // We will start the update as soon as the previous one finishes.
            return oldLowWatermarkCandidate.updateFuture()
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

    @Override
    public boolean tryLock(UUID lockId, HybridTimestamp ts) {
        return inBusyLock(busyLock, () -> {
            updateLowWatermarkLock.readLock().lock();

            try {
                HybridTimestamp lwm = lowWatermark;
                if (lwm != null && ts.compareTo(lwm) < 0) {
                    return false;
                }

                locks.put(lockId, new LowWatermarkLock(ts));

                return true;
            } finally {
                updateLowWatermarkLock.readLock().unlock();
            }
        });
    }

    @Override
    public void unlock(UUID lockId) {
        LowWatermarkLock lock = locks.remove(lockId);

        if (lock == null) {
            // Already released.
            return;
        }

        lock.future().complete(null);
    }

    CompletableFuture<Void> updateAndNotify(HybridTimestamp newLowWatermark) {
        return inBusyLockAsync(busyLock, () -> {
            persistWatermark(newLowWatermark);

            return waitForLocksAndSetLowWatermark(newLowWatermark)
                            .thenComposeAsync(unused2 -> fireEvent(
                                    LOW_WATERMARK_CHANGED,
                                    new ChangeLowWatermarkEventParameters(newLowWatermark)), scheduledThreadPool)
                            .whenCompleteAsync((unused, throwable) -> {
                                if (throwable != null) {
                                    if (!(hasCause(throwable, NodeStoppingException.class))) {
                                        LOG.error("Failed to update low watermark: {}", throwable, newLowWatermark);

                                        failureManager.process(new FailureContext(CRITICAL_ERROR, throwable));
                                    }
                                } else {
                                    LOG.info("Successful low watermark update: {}", newLowWatermark);
                                }
                            }, scheduledThreadPool);
                }
        );
    }

    private void persistWatermark(HybridTimestamp newLowWatermark) {
        vaultManager.put(LOW_WATERMARK_VAULT_KEY, newLowWatermark.toBytes());
    }

    private CompletableFuture<Void> waitForLocksAndSetLowWatermark(HybridTimestamp newLowWatermark) {
        return inBusyLockAsync(busyLock, () -> {
            // Write lock so no new LWM locks can be added.
            updateLowWatermarkLock.writeLock().lock();

            try {
                for (LowWatermarkLock lock : locks.values()) {
                    if (lock.timestamp().compareTo(newLowWatermark) < 0) {
                        return lock.future().thenCompose(unused -> waitForLocksAndSetLowWatermark(newLowWatermark));
                    }
                }

                setLowWatermark(newLowWatermark);

                return nullCompletedFuture();
            } finally {
                updateLowWatermarkLock.writeLock().unlock();
            }
        });
    }
}
