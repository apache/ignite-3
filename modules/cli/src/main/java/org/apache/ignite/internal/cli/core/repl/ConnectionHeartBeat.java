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

package org.apache.ignite.internal.cli.core.repl;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.event.ConnectionEventListener;
import org.apache.ignite.internal.cli.event.EventPublisher;
import org.apache.ignite.internal.cli.event.Events;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.rest.client.api.NodeManagementApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.jetbrains.annotations.Nullable;

/**
 * Connection to node heart beat.
 */
@Singleton
public class ConnectionHeartBeat implements ConnectionEventListener {

    private static final IgniteLogger LOG = CliLoggers.forClass(ConnectionHeartBeat.class);

    /** CLI check connection period period. */
    private final long cliCheckConnectionPeriodSecond;

    /** Scheduled executor for connection heartbeat. */
    @Nullable
    private ScheduledExecutorService scheduledConnectionHeartbeatExecutor;

    private final ApiClientFactory clientFactory;

    private final EventPublisher eventPublisher;

    private final AtomicBoolean connected = new AtomicBoolean(false);

    private final AtomicReference<String> lastKnownUrl = new AtomicReference<>(null);

    private final Lock lock = new ReentrantLock();

    /**
     * Creates the instance of connection heartbeat.
     *
     * @param clientFactory api client factory.
     * @param eventPublisher event publisher.
     */
    public ConnectionHeartBeat(@Value("${cli.check.connection.period.second:5}") long cliCheckConnectionPeriodSecond,
            ApiClientFactory clientFactory,
            EventPublisher eventPublisher) {
        this.clientFactory = clientFactory;
        this.eventPublisher = eventPublisher;
        this.cliCheckConnectionPeriodSecond = cliCheckConnectionPeriodSecond;
    }

    /**
     * Starts connection heartbeat. By default connection will be checked every 5 sec.
     *
     * @param sessionInfo session info with node url
     */
    @Override
    public void onConnect(SessionInfo sessionInfo) {
        if (connected.compareAndSet(false, true)) {
            eventPublisher.publish(Events.connectionRestored());
        }

        lastKnownUrl.set(sessionInfo.nodeUrl());

        lock.lock();
        try {
            if (scheduledConnectionHeartbeatExecutor == null) {
                scheduledConnectionHeartbeatExecutor =
                        Executors.newScheduledThreadPool(1, new NamedThreadFactory("cli-check-connection-thread", LOG));

                // Start connection heart beat
                scheduledConnectionHeartbeatExecutor.scheduleAtFixedRate(
                        this::pingConnection,
                        0,
                        cliCheckConnectionPeriodSecond,
                        TimeUnit.SECONDS
                );
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Stops connection heartbeat.
     */
    @Override
    public void onDisconnect() {
        lock.lock();
        try {
            if (scheduledConnectionHeartbeatExecutor != null) {
                scheduledConnectionHeartbeatExecutor.shutdownNow();
                scheduledConnectionHeartbeatExecutor = null;
            }
        } finally {
            lock.unlock();
        }
    }

    public void pingConnection() {
        try {
            new NodeManagementApi(clientFactory.getClient(lastKnownUrl.get())).nodeState();
            if (connected.compareAndSet(false, true)) {
                eventPublisher.publish(Events.connectionRestored());
            }
        } catch (ApiException exception) {
            if (connected.compareAndSet(true, false)) {
                eventPublisher.publish(Events.connectionLost());
            }
        }
    }
}
