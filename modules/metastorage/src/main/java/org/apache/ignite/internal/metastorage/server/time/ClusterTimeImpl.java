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

package org.apache.ignite.internal.metastorage.server.time;

import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.metrics.MetaStorageMetrics;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.jetbrains.annotations.Nullable;

/**
 * Cluster time implementation with additional methods to adjust time and update safe time.
 */
public class ClusterTimeImpl implements ClusterTime, MetaStorageMetrics, ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterTimeImpl.class);

    private final String nodeName;

    private final IgniteSpinBusyLock busyLock;

    private final HybridClock clock;

    private final PendingComparableValuesTracker<HybridTimestamp, Void> safeTime =
            new PendingComparableValuesTracker<>(HybridTimestamp.MIN_VALUE);

    /**
     * Scheduler for sending safe time periodically when Meta Storage is idle.
     *
     * <p>Scheduler is only created if this node has been elected as the Meta Storage leader.
     *
     * <p>Concurrent access is guarded by {@code this}.
     */
    private @Nullable SafeTimeScheduler safeTimeScheduler;

    @Override
    public long safeTimeLag() {
        return clock.now().getPhysical() - safeTime.current().getPhysical();
    }

    /** Action that issues a time sync command. */
    @FunctionalInterface
    public interface SyncTimeAction {
        CompletableFuture<Void> syncTime(HybridTimestamp time);
    }

    /**
     * Constructor.
     *
     * @param busyLock Busy lock.
     * @param clock Node's hybrid clock.
     */
    public ClusterTimeImpl(String nodeName, IgniteSpinBusyLock busyLock, HybridClock clock) {
        this.nodeName = nodeName;
        this.busyLock = busyLock;
        this.clock = clock;
    }

    /**
     * Starts sync time scheduler.
     *
     * @param syncTimeAction Action that performs the time sync operation.
     */
    public void startSafeTimeScheduler(SyncTimeAction syncTimeAction, MetaStorageConfiguration configuration) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            synchronized (this) {
                assert safeTimeScheduler == null;

                safeTimeScheduler = new SafeTimeScheduler(syncTimeAction, configuration);

                safeTimeScheduler.start();
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Stops sync time scheduler if it exists.
     */
    public synchronized void stopSafeTimeScheduler() {
        if (safeTimeScheduler != null) {
            safeTimeScheduler.stop();

            safeTimeScheduler = null;
        }
    }

    @Override
    public void close() throws Exception {
        stopSafeTimeScheduler();

        safeTime.close();
    }

    @Override
    public HybridTimestamp now() {
        return clock.now();
    }

    @Override
    public long nowLong() {
        return clock.nowLong();
    }

    @Override
    public HybridTimestamp currentSafeTime() {
        return this.safeTime.current();
    }

    @Override
    public CompletableFuture<Void> waitFor(HybridTimestamp time) {
        return safeTime.waitFor(time);
    }

    /**
     * Updates the internal safe time.
     *
     * @param newValue New safe time value.
     */
    public void updateSafeTime(HybridTimestamp newValue) {
        this.safeTime.update(newValue, null);
    }

    /**
     * Updates hybrid logical clock using {@code ts}. Selects the maximum between current system time, hybrid clock's latest time and
     * {@code ts} adding 1 logical tick to the result.
     *
     * @param ts Timestamp.
     */
    public synchronized void adjust(HybridTimestamp ts) {
        this.clock.update(ts);

        // Since this method is called when a write command is being processed and safe time is also updated by write commands,
        // we need to re-schedule the idle time scheduler.
        if (safeTimeScheduler != null) {
            safeTimeScheduler.schedule();
        }
    }

    private class SafeTimeScheduler {
        private final SyncTimeAction syncTimeAction;

        private final MetaStorageConfiguration configuration;

        private final ScheduledExecutorService executorService =
                Executors.newSingleThreadScheduledExecutor(NamedThreadFactory.create(nodeName, "meta-storage-safe-time", LOG));

        /**
         * Current scheduled task.
         *
         * <p>Concurrent access is guarded by {@code this}.
         */
        @Nullable
        private ScheduledFuture<?> currentTask;

        SafeTimeScheduler(SyncTimeAction syncTimeAction, MetaStorageConfiguration configuration) {
            this.syncTimeAction = syncTimeAction;
            this.configuration = configuration;
        }

        void start() {
            schedule();
        }

        synchronized void schedule() {
            // Cancel the previous task if we were re-scheduled because Meta Storage was not actually idle.
            if (currentTask != null) {
                currentTask.cancel(false);
            }

            currentTask = executorService.schedule(() -> {
                if (!busyLock.enterBusy()) {
                    return;
                }

                try {
                    syncTimeAction.syncTime(clock.now())
                            .whenComplete((v, e) -> {
                                if (e != null) {
                                    Throwable cause = unwrapCause(e);

                                    if (!(cause instanceof CancellationException) && !(cause instanceof NodeStoppingException)) {
                                        LOG.error("Unable to perform idle time sync", e);
                                    }
                                }
                            });

                    // Re-schedule the task again.
                    schedule();
                } finally {
                    busyLock.leaveBusy();
                }
            }, configuration.idleSyncTimeInterval().value(), TimeUnit.MILLISECONDS);
        }

        void stop() {
            synchronized (this) {
                if (currentTask != null) {
                    currentTask.cancel(false);
                }
            }

            IgniteUtils.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);
        }

    }
}
