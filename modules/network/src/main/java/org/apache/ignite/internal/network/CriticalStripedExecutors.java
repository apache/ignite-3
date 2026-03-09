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

package org.apache.ignite.internal.network;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.thread.StripedExecutor;
import org.apache.ignite.internal.worker.CriticalWorker;
import org.apache.ignite.internal.worker.CriticalWorkerRegistry;
import org.jetbrains.annotations.Nullable;

/**
 * Collection of {@link StripedExecutor executors} for the network based on {@link ChannelType#id()}.
 *
 * <p>Executors are created once in the constructor, so it is important that all {@link ChannelType}s are registered at the time the
 * constructor is called. This was done intentionally to optimize and get rid of contention.</p>
 */
class CriticalStripedExecutors implements ManuallyCloseable {
    private final CriticalWorkerRegistry workerRegistry;

    private final StripedExecutorByChannelTypeId executorByChannelTypeId;

    private final List<CriticalWorker> registeredWorkers = new CopyOnWriteArrayList<>();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    CriticalStripedExecutors(
            String nodeName,
            String poolNamePrefix,
            CriticalWorkerRegistry workerRegistry,
            ChannelTypeRegistry channelTypeRegistry,
            IgniteLogger log,
            @Nullable MetricManager metricManager,
            @Nullable String metricNamePrefix,
            @Nullable String metricDescription
    ) {
        this.workerRegistry = workerRegistry;

        var factory = new CriticalStripedThreadPoolExecutorFactory(nodeName, poolNamePrefix, log, workerRegistry, registeredWorkers,
                metricManager, metricNamePrefix, metricDescription);

        executorByChannelTypeId = StripedExecutorByChannelTypeId.of(channelTypeRegistry, factory);
    }

    @Override
    public void close() throws Exception {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        registeredWorkers.forEach(workerRegistry::unregister);

        closeAll(
                executorByChannelTypeId.stream()
                        .parallel()
                        .map(executor -> () -> shutdownAndAwaitTermination(executor, 10, SECONDS))
        );
    }

    /**
     * Returns executor to execute a command on a stripe with the given index.
     *
     * @param channelTypeId {@link ChannelType#id() Channel type ID}.
     * @param stripeIndex Index of the stripe.
     */
    Executor executorFor(short channelTypeId, int stripeIndex) {
        return executorByChannelTypeId.get(channelTypeId).stripeExecutor(stripeIndex);
    }
}
