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

import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.FailureType;
import org.apache.ignite.internal.failure.handlers.NoOpFailureHandler;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.IgniteThrottledLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.metrics.MetaStorageMetrics;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Cluster time implementation with additional methods to adjust time and update safe time.
 */
public class ClusterTimeImpl implements ClusterTime, MetaStorageMetrics, ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterTimeImpl.class);

    private final String nodeName;

    private final IgniteSpinBusyLock busyLock;

    private final HybridClock clock;

    private final FailureProcessor failureProcessor;

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

    /**
     * Last term when the scheduler was stopped.
     *
     * <p>Scheduler is only created if passed term is greater than the last stopped term.
     *
     * <p>Concurrent access is guarded by {@code this}.
     */
    private long lastSchedulerStoppedTerm = -1;

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
     * @param nodeName Node name.
     * @param busyLock Busy lock.
     * @param clock Node's hybrid clock.
     */
    @TestOnly
    public ClusterTimeImpl(String nodeName, IgniteSpinBusyLock busyLock, HybridClock clock) {
        this(nodeName, busyLock, clock, new FailureManager(new NoOpFailureHandler()));
    }

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param busyLock Busy lock.
     * @param clock Node's hybrid clock.
     * @param failureProcessor Failure processor to use when reporting failures.
     */
    public ClusterTimeImpl(String nodeName, IgniteSpinBusyLock busyLock, HybridClock clock, FailureProcessor failureProcessor) {
        this.nodeName = nodeName;
        this.busyLock = busyLock;
        this.clock = clock;
        this.failureProcessor = failureProcessor;
    }

    /**
     * Starts sync time scheduler.
     *
     * @param syncTimeAction Action that performs the time sync operation.
     */
    public void startSafeTimeScheduler(SyncTimeAction syncTimeAction, SystemDistributedConfiguration configuration, long term) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            synchronized (this) {
                if (lastSchedulerStoppedTerm > term) {
                    return;
                }

                assert safeTimeScheduler == null;

                safeTimeScheduler = new SafeTimeScheduler(syncTimeAction, configuration);

                safeTimeScheduler.start();
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Stops sync time scheduler if it exists and forbids to create schedulers for terms not exceeding {@param term}.
     */
    public synchronized void stopSafeTimeScheduler(long term) {
        lastSchedulerStoppedTerm = term;

        if (safeTimeScheduler != null) {
            safeTimeScheduler.stop();

            safeTimeScheduler = null;
        }
    }

    @Override
    public void close() throws Exception {
        stopSafeTimeScheduler(Long.MAX_VALUE);

        safeTime.close(new NodeStoppingException());
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
    public synchronized void adjustClock(HybridTimestamp ts) {
        this.clock.update(ts);

        // Since this method is called when a write command is being processed and safe time is also updated by write commands,
        // we need to re-schedule the idle time scheduler.
        if (safeTimeScheduler != null) {
            safeTimeScheduler.schedule();
        }
    }

    private class SafeTimeScheduler {
        private final SyncTimeAction syncTimeAction;

        private final SystemDistributedConfiguration configuration;

        private final ScheduledExecutorService executorService =
                Executors.newSingleThreadScheduledExecutor(IgniteThreadFactory.create(nodeName, "meta-storage-safe-time", LOG));

        private final IgniteThrottledLogger throttledLogger = Loggers.toThrottledLogger(LOG, executorService);

        /**
         * Current scheduled task.
         *
         * <p>Concurrent access is guarded by {@code this}.
         */
        @Nullable
        private ScheduledFuture<?> currentTask;

        SafeTimeScheduler(SyncTimeAction syncTimeAction, SystemDistributedConfiguration configuration) {
            this.syncTimeAction = syncTimeAction;
            this.configuration = configuration;
        }

        void start() {
            schedule();

            LOG.info("Started safe time scheduler");
        }

        synchronized void schedule() {
            // Cancel the previous task if we were re-scheduled because Meta Storage was not actually idle.
            if (currentTask != null) {
                currentTask.cancel(false);
            }

            currentTask = executorService.schedule(() -> {
                try {
                    tryToSyncTimeAndReschedule();
                } catch (RejectedExecutionException ignored) {
                    // Ignore, the node is stopping.
                } catch (Throwable t) {
                    failureProcessor.process(new FailureContext(FailureType.CRITICAL_ERROR, t));

                    if (t instanceof Error) {
                        throw t;
                    }
                }
            }, configuration.idleSafeTimeSyncIntervalMillis().value(), TimeUnit.MILLISECONDS);
        }

        private void tryToSyncTimeAndReschedule() {
            if (!busyLock.enterBusy()) {
                return;
            }

            try {
                syncTimeAction.syncTime(clock.now())
                        .whenComplete((v, e) -> {
                            if (e != null) {
                                // We don't invoke FH on timeout, because it can happen when meta storage loses majority.
                                if (hasCause(e, TimeoutException.class)) {
                                    throttledLogger.warn("Unable to perform idle time sync because of timeout on meta storage service.");
                                } else if (!hasCause(e, NodeStoppingException.class)) {
                                    failureProcessor.process(new FailureContext(e, "Unable to perform idle time sync"));
                                }
                            }
                        });

                // Re-schedule the task again.
                schedule();
            } finally {
                busyLock.leaveBusy();
            }
        }

        void stop() {
            LOG.info("Stopping safe time scheduler");

            synchronized (this) {
                if (currentTask != null) {
                    currentTask.cancel(false);
                }
            }

            IgniteUtils.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);
        }

    }
}
