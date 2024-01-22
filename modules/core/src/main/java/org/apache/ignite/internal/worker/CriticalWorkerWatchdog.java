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
import org.jetbrains.annotations.Nullable;

/**
 * A watchdog that monitors liveness of the registered workers and, if a worker is suspected to be blocked, logs the corresponding
 * information (including the stack trace corresponding to the worker's thread) and then calls the failure handler
 * to fail the node.
 *
 * <p>Each worker is expected to maintain its {@link CriticalWorker#heartbeatNanos()} growing while the worker executes some computations.
 * If the worker does not do any computations (it is blocked on an I/O operation, waits for a lock, or has no work to do),
 * it must set its {@link CriticalWorker#heartbeatNanos()} to {@link CriticalWorker#NOT_MONITORED}.
 *
 * <p>The watchdog periodically does its check; if it finds a worker that lags more than allowed and it is not in
 * NOT_MONITORED state, then logging and failure handling notification is triggered.
 */
public class CriticalWorkerWatchdog implements CriticalWorkerRegistry, IgniteComponent {
    private final IgniteLogger log = Loggers.forClass(CriticalWorkerWatchdog.class);

    // TODO: IGNITE-21227 - make this configurable.
    private static final long LIVENESS_CHECK_INTERVAL_MS = 200;
    private static final long MAX_ALLOWED_LAG_MS = 500;

    private final ScheduledExecutorService scheduler;

    private final Set<CriticalWorker> registeredWorkers = ConcurrentHashMap.newKeySet();

    @Nullable
    private volatile ScheduledFuture<?> livenessProbeTaskFuture;

    public CriticalWorkerWatchdog(ScheduledExecutorService scheduler) {
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
        livenessProbeTaskFuture = scheduler.scheduleAtFixedRate(
                this::probeLiveness,
                LIVENESS_CHECK_INTERVAL_MS,
                LIVENESS_CHECK_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );

        return nullCompletedFuture();
    }

    private void probeLiveness() {
        long nowNanos = System.nanoTime();

        for (CriticalWorker worker : registeredWorkers) {
            long heartbeatNanos = worker.heartbeatNanos();

            if (heartbeatNanos == CriticalWorker.NOT_MONITORED) {
                continue;
            }

            long delayMillis = TimeUnit.NANOSECONDS.toMillis(nowNanos - heartbeatNanos);
            if (delayMillis > MAX_ALLOWED_LAG_MS) {
                ThreadMXBean bean = ManagementFactory.getThreadMXBean();

                ThreadInfo[] threadInfos = bean.getThreadInfo(new long[]{worker.threadId()}, true, true, Integer.MAX_VALUE);
                ThreadInfo threadInfo = threadInfos[0];
                if (threadInfo != null) {
                    log.error("A critical thread is blocked for {} ms that is more than the allowed {} ms, it is {}",
                            delayMillis, MAX_ALLOWED_LAG_MS, threadInfo);

                    // TODO: IGNITE-16899 - invoke failure handler.
                }
            }
        }
    }

    @Override
    public void stop() throws Exception {
        ScheduledFuture<?> taskFuture = livenessProbeTaskFuture;
        if (taskFuture != null) {
            taskFuture.cancel(false);
        }
    }
}
