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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.worker.configuration.CriticalWorkersConfiguration;
import org.jetbrains.annotations.Nullable;

// TODO: IGNITE-16899 - update the javadoc to mention that the failure handler is invoked.
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
    private final IgniteLogger log = Loggers.forClass(CriticalWorkerWatchdog.class);

    private final CriticalWorkersConfiguration configuration;

    private final ScheduledExecutorService scheduler;

    private final Set<CriticalWorker> registeredWorkers = ConcurrentHashMap.newKeySet();

    @Nullable
    private volatile ScheduledFuture<?> livenessProbeTaskFuture;

    private final ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();

    public CriticalWorkerWatchdog(CriticalWorkersConfiguration configuration, ScheduledExecutorService scheduler) {
        this.configuration = configuration;
        this.scheduler = scheduler;
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
    public CompletableFuture<Void> start() {
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
        long maxAllowedLag = configuration.maxAllowedLag().value();

        Long2LongMap delayedThreadIdsToDelays = getDelayedThreadIdsAndDelays(maxAllowedLag);

        if (delayedThreadIdsToDelays == null) {
            return;
        }

        ThreadInfo[] threadInfos = threadMxBean.getThreadInfo(delayedThreadIdsToDelays.keySet().toLongArray(), true, true);
        for (ThreadInfo threadInfo : threadInfos) {
            if (threadInfo != null) {
                log.error("A critical thread is blocked for {} ms that is more than the allowed {} ms, it is {}",
                        delayedThreadIdsToDelays.get(threadInfo.getThreadId()), maxAllowedLag, threadInfo);

                // TODO: IGNITE-16899 - invoke failure handler.
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

    @Override
    public void stop() throws Exception {
        ScheduledFuture<?> taskFuture = livenessProbeTaskFuture;
        if (taskFuture != null) {
            taskFuture.cancel(false);
        }
    }
}
