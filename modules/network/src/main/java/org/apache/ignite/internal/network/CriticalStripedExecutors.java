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

import static java.util.Comparator.comparingInt;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.thread.ThreadOperation.NOTHING_ALLOWED;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.thread.StripedExecutor;
import org.apache.ignite.internal.worker.CriticalStripedThreadPoolExecutor;
import org.apache.ignite.internal.worker.CriticalWorker;
import org.apache.ignite.internal.worker.CriticalWorkerRegistry;

/**
 * Collection of {@link StripedExecutor executors} for the network based on {@link ChannelType#register registered channels} and optimized
 * for quick get of an executor.
 *
 * <p>It is important that all {@link ChannelType} are {@link ChannelType#register} registered} before creating this object.</p>
 */
class CriticalStripedExecutors implements ManuallyCloseable {
    /**
     * Maximum number of stripes in the thread pool in which incoming network messages for the {@link ChannelType#DEFAULT} channel
     * are handled.
     */
    private static final int DEFAULT_CHANNEL_INBOUND_WORKERS = 4;

    private final CriticalWorkerRegistry workerRegistry;

    private final StripedExecutor[] executorByChannelTypeId;

    private final List<CriticalWorker> registeredWorkers = new CopyOnWriteArrayList<>();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /** Constructor. */
    CriticalStripedExecutors(
            String nodeName,
            String poolNamePrefix,
            CriticalWorkerRegistry workerRegistry,
            IgniteLogger log
    ) {
        this.workerRegistry = workerRegistry;

        executorByChannelTypeId = createStripedExecutorsForRegisteredChannelTypes(nodeName, poolNamePrefix, log);
    }

    @Override
    public void close() throws Exception {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        registeredWorkers.forEach(workerRegistry::unregister);

        closeAll(
                Arrays.stream(executorByChannelTypeId).parallel()
                        .map(executor -> () -> shutdownAndAwaitTermination(executor, 10, SECONDS))
        );
    }

    /**
     * Returns executor to executes a command on a stripe with the given index.Ø
     *
     * @param channelTypeId {@link ChannelType#id() Channel type ID}.
     * @param stripeIndex Index of the stripe.
     */
    Executor executorFor(short channelTypeId, int stripeIndex) {
        assert channelTypeId >= 0 && channelTypeId < executorByChannelTypeId.length : "Channel type is not registered: " + channelTypeId;

        return executorByChannelTypeId[channelTypeId].stripeExecutor(stripeIndex);
    }

    private StripedExecutor[] createStripedExecutorsForRegisteredChannelTypes(String nodeName, String poolNamePrefix, IgniteLogger log) {
        ChannelType[] channelTypes = ChannelType.getRegisteredChannelTypes().stream()
                .sorted(comparingInt(ChannelType::id))
                .toArray(ChannelType[]::new);

        assert channelTypes.length > 0;
        assert channelTypes[0].id() == 0 : channelTypes[0].id();

        StripedExecutor[] executors = new StripedExecutor[channelTypes.length];
        int previousChannelTypeId = -1;

        for (int i = 0; i < channelTypes.length; i++) {
            ChannelType channelType = channelTypes[i];

            assert channelType.id() == previousChannelTypeId + 1 : String.format(
                    "There should be no gaps between channel IDs: [current=%s, previous=%s]", channelType.id(), previousChannelTypeId);

            executors[i] = newStripedExecutor(nodeName, poolNamePrefix, channelType, log);

            previousChannelTypeId = channelType.id();
        }

        return executors;
    }

    private StripedExecutor newStripedExecutor(String nodeName, String poolNamePrefix, ChannelType channelType, IgniteLogger log) {
        short channelTypeId = channelType.id();
        String poolName = poolNamePrefix + "-" + channelType.name() + "-" + channelTypeId;

        var threadFactory = IgniteThreadFactory.create(nodeName, poolName, log, NOTHING_ALLOWED);
        var executor = new CriticalStripedThreadPoolExecutor(stripeCountForIndex(channelTypeId), threadFactory, false, 0);

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
