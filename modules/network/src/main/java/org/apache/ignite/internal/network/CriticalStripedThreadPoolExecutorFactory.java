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

import static org.apache.ignite.internal.thread.ThreadOperation.NOTHING_ALLOWED;

import java.util.List;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.sources.StripedThreadPoolMetricSource;
import org.apache.ignite.internal.worker.CriticalStripedThreadPoolExecutor;
import org.apache.ignite.internal.worker.CriticalWorker;
import org.apache.ignite.internal.worker.CriticalWorkerRegistry;
import org.jetbrains.annotations.Nullable;

/** Factory for creating {@link CriticalStripedThreadPoolExecutor}. */
class CriticalStripedThreadPoolExecutorFactory {
    /**
     * Maximum number of stripes in the thread pool in which incoming network messages for the {@link ChannelType#DEFAULT} channel are
     * handled.
     */
    private static final int DEFAULT_CHANNEL_INBOUND_WORKERS = 4;

    private final String nodeName;

    private final String poolNamePrefix;

    private final IgniteLogger log;

    private final CriticalWorkerRegistry workerRegistry;

    private final List<CriticalWorker> registeredWorkers;

    @Nullable
    private final MetricManager metricManager;

    @Nullable
    private final String metricNamePrefix;

    @Nullable
    private final String metricDescription;

    CriticalStripedThreadPoolExecutorFactory(
            String nodeName,
            String poolNamePrefix,
            IgniteLogger log,
            CriticalWorkerRegistry workerRegistry,
            List<CriticalWorker> registeredWorkers,
            @Nullable MetricManager metricManager,
            @Nullable String metricNamePrefix,
            @Nullable String metricDescription
    ) {
        this.nodeName = nodeName;
        this.poolNamePrefix = poolNamePrefix;
        this.log = log;
        this.workerRegistry = workerRegistry;
        this.registeredWorkers = registeredWorkers;

        this.metricManager = metricManager;
        this.metricNamePrefix = metricNamePrefix;
        this.metricDescription = metricDescription;
    }

    CriticalStripedThreadPoolExecutor create(ChannelType channelType) {
        short channelTypeId = channelType.id();
        String poolName = poolNamePrefix + "-" + channelType.name() + "-" + channelTypeId;

        var threadFactory = IgniteMessageServiceThreadFactory.create(nodeName, poolName, log, NOTHING_ALLOWED);
        var executor = new CriticalStripedThreadPoolExecutor(stripeCountForIndex(channelTypeId), threadFactory, false, 0);

        if (metricManager != null && metricNamePrefix != null) {
            String metricName = String.format("%s.%s", metricNamePrefix, channelType.name());
            var metricSource = new StripedThreadPoolMetricSource<>(metricName, metricDescription, null,
                    executor);

            metricManager.registerSource(metricSource);
            metricManager.enable(metricSource);
        }

        for (CriticalWorker worker : executor.workers()) {
            workerRegistry.register(worker);
            registeredWorkers.add(worker);
        }

        return executor;
    }

    private static int stripeCountForIndex(short channelTypeId) {
        return channelTypeId == ChannelType.DEFAULT.id() ? DEFAULT_CHANNEL_INBOUND_WORKERS : 1;
    }
}
