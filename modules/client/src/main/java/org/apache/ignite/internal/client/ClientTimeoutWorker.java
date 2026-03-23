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

package org.apache.ignite.internal.client;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.internal.future.timeout.TimeoutWorker;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.jetbrains.annotations.Nullable;

final class ClientTimeoutWorker {
    static final ClientTimeoutWorker INSTANCE = new ClientTimeoutWorker();

    private static final int emptyCountThreshold = 10;

    private @Nullable ScheduledExecutorService executor = null;

    private final Set<TcpClientChannel> channels = ConcurrentHashMap.newKeySet();

    private int emptyCount;

    private ClientTimeoutWorker() {
        // No-op.
    }

    synchronized void registerClientChannel(TcpClientChannel ch, IgniteClientConfiguration clientCfg) {
        channels.add(ch);
        emptyCount = 0;

        if (executor == null) {
            executor = createExecutor(clientCfg);
            emptyCount = 0;

            long sleepInterval = TimeoutWorker.getSleepInterval();
            executor.scheduleAtFixedRate(this::checkTimeouts, sleepInterval, sleepInterval, MILLISECONDS);
        }
    }

    private synchronized void shutdownIfEmpty() {
        if (executor != null && channels.isEmpty()) {
            emptyCount++;

            if (emptyCount >= emptyCountThreshold) {
                executor.shutdown();
                executor = null;
            }
        }
    }

    private static ScheduledExecutorService createExecutor(IgniteClientConfiguration clientCfg) {
        return Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory(
                        "TcpClientChannel-timeout-worker",
                        ClientUtils.logger(clientCfg, ClientTimeoutWorker.class)));
    }

    private void checkTimeouts() {
        long now = coarseCurrentTimeMillis();

        for (TcpClientChannel ch : channels) {
            if (ch.closed()) {
                channels.remove(ch);
            }

            ch.checkTimeouts(now);
        }

        shutdownIfEmpty();
    }
}
