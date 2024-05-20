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

package org.apache.ignite.internal.worker;

import static org.apache.ignite.internal.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.internal.failure.FailureType.SYSTEM_WORKER_BLOCKED;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.CriticalWorkers.SYSTEM_WORKER_BLOCKED_ERR;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.worker.configuration.CriticalWorkersConfiguration;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * A watchdog that monitors liveness of the registered workers and, if a worker is suspected to be blocked, logs the corresponding
 * information (including the stack trace corresponding to the worker's thread).
 *
 * <p>Each worker is expected to maintain its {@link CriticalWorker#heartbeatNanos()} growing while the worker executes some computations.
 * If the worker does not do any computations (it is blocked on an I/O operation, waits for a lock, or has no work to do),
 * it must set its {@link CriticalWorker#heartbeatNanos()} to {@link CriticalWorker#NOT_MONITORED}.
 *
 * <p>The watchdog periodically performs a check; if it finds a worker that lags more than allowed and it is not in the
 * NOT_MONITORED state, then a logging is triggered.
 */
public class CriticalWorkerWatchdog implements CriticalWorkerRegistry, IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(CriticalWorkerWatchdog.class);

    private final CriticalWorkersConfiguration configuration;

    private final ScheduledExecutorService scheduler;

    private final Set<CriticalWorker> registeredWorkers = ConcurrentHashMap.newKeySet();

    @Nullable
    private volatile ScheduledFuture<?> livenessProbeTaskFuture;

    private final ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();

    private final FailureProcessor failureProcessor;

    /**
     * Creates a new instance of the watchdog.
     *
     * @param configuration Configuration.
     * @param scheduler Scheduler.
     * @param failureProcessor Failure processor.
     */
    public CriticalWorkerWatchdog(
            CriticalWorkersConfiguration configuration,
            ScheduledExecutorService scheduler,
            FailureProcessor failureProcessor
    ) {
        this.configuration = configuration;
        this.scheduler = scheduler;
        this.failureProcessor = failureProcessor;
    }

    @Override
    public void register(CriticalWorker worker) {
        registeredWorkers.add(worker);
    }

    @Override
    public void unregister(CriticalWorker worker) {
        registeredWorkers.remove(worker);
    }

    @Override
    public CompletableFuture<Void> startAsync(ExecutorService startupExecutor) {
        long livenessCheckIntervalMs = configuration.livenessCheckInterval().value();

        livenessProbeTaskFuture = scheduler.scheduleAtFixedRate(
                this::probeLiveness,
                livenessCheckIntervalMs,
                livenessCheckIntervalMs,
                TimeUnit.MILLISECONDS
        );

        return nullCompletedFuture();
    }

    private void probeLiveness() {
        try {
            doProbeLiveness();
        } catch (Exception | AssertionError e) {
            LOG.debug("Error while probing liveness", e);
        } catch (Error e) {
            failureProcessor.process(new FailureContext(CRITICAL_ERROR, e));
        }
    }

    private void doProbeLiveness() {
        long maxAllowedLag = configuration.maxAllowedLag().value();

        Long2LongMap delayedThreadIdsToDelays = getDelayedThreadIdsAndDelays(maxAllowedLag);

        if (delayedThreadIdsToDelays == null) {
            return;
        }

        ThreadInfo[] threadInfos = threadMxBean.getThreadInfo(
                delayedThreadIdsToDelays.keySet().toLongArray(),
                threadMxBean.isObjectMonitorUsageSupported(),
                threadMxBean.isSynchronizerUsageSupported());

        for (ThreadInfo threadInfo : threadInfos) {
            if (threadInfo != null) {
                String message = String.format(
                        "A critical thread is blocked for %d ms that is more than the allowed %d ms, it is %s",
                        delayedThreadIdsToDelays.get(threadInfo.getThreadId()),
                        maxAllowedLag,
                        toString(threadInfo));

                LOG.error(message);

                failureProcessor.process(
                        new FailureContext(
                                SYSTEM_WORKER_BLOCKED,
                                new IgniteException(SYSTEM_WORKER_BLOCKED_ERR, message)));
            }
        }
    }

    @Nullable
    private Long2LongMap getDelayedThreadIdsAndDelays(long maxAllowedLag) {
        long nowNanos = System.nanoTime();

        Long2LongMap delayedThreadIdsToDelays = null;

        for (CriticalWorker worker : registeredWorkers) {
            long heartbeatNanos = worker.heartbeatNanos();

            if (heartbeatNanos == CriticalWorker.NOT_MONITORED) {
                continue;
            }

            long delayMillis = TimeUnit.NANOSECONDS.toMillis(nowNanos - heartbeatNanos);
            if (delayMillis > maxAllowedLag) {
                if (delayedThreadIdsToDelays == null) {
                    delayedThreadIdsToDelays = new Long2LongOpenHashMap();
                }

                delayedThreadIdsToDelays.put(worker.threadId(), delayMillis);
            }
        }

        return delayedThreadIdsToDelays;
    }

    private static String toString(ThreadInfo threadInfo) {
        // This method is based on code taken from ThreadInfo#toString(). The original method limits the depth of the
        // stacktrace it includes in the string representation to just 8 frames, which is too few. Here, we
        // removed this limitation and include the stack trace in its entirety.

        StringBuilder sb = new StringBuilder()
                .append('\"').append(threadInfo.getThreadName()).append('\"')
                .append(threadInfo.isDaemon() ? " daemon" : "")
                .append(" prio=").append(threadInfo.getPriority())
                .append(" Id=").append(threadInfo.getThreadId()).append(' ')
                .append(threadInfo.getThreadState());

        if (threadInfo.getLockName() != null) {
            sb.append(" on ").append(threadInfo.getLockName());
        }
        if (threadInfo.getLockOwnerName() != null) {
            sb.append(" owned by \"").append(threadInfo.getLockOwnerName())
                    .append("\" Id=").append(threadInfo.getLockOwnerId());
        }
        if (threadInfo.isSuspended()) {
            sb.append(" (suspended)");
        }
        if (threadInfo.isInNative()) {
            sb.append(" (in native)");
        }
        sb.append('\n');
        int i = 0;
        for (; i < threadInfo.getStackTrace().length; i++) {
            StackTraceElement ste = threadInfo.getStackTrace()[i];
            sb.append("\tat ").append(ste.toString());
            sb.append('\n');
            if (i == 0 && threadInfo.getLockInfo() != null) {
                Thread.State ts = threadInfo.getThreadState();
                switch (ts) {
                    case BLOCKED:
                        sb.append("\t-  blocked on ").append(threadInfo.getLockInfo());
                        sb.append('\n');
                        break;
                    case WAITING:
                        sb.append("\t-  waiting on ").append(threadInfo.getLockInfo());
                        sb.append('\n');
                        break;
                    case TIMED_WAITING:
                        sb.append("\t-  waiting on ").append(threadInfo.getLockInfo());
                        sb.append('\n');
                        break;
                    default:
                }
            }

            for (MonitorInfo mi : threadInfo.getLockedMonitors()) {
                if (mi.getLockedStackDepth() == i) {
                    sb.append("\t-  locked ").append(mi);
                    sb.append('\n');
                }
            }
        }

        LockInfo[] locks = threadInfo.getLockedSynchronizers();
        if (locks.length > 0) {
            sb.append("\n\tNumber of locked synchronizers = ").append(locks.length);
            sb.append('\n');
            for (LockInfo li : locks) {
                sb.append("\t- ").append(li);
                sb.append('\n');
            }
        }
        sb.append('\n');
        return sb.toString();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ExecutorService stopExecutor) {
        ScheduledFuture<?> taskFuture = livenessProbeTaskFuture;
        if (taskFuture != null) {
            taskFuture.cancel(false);
        }
        return nullCompletedFuture();
    }
}
