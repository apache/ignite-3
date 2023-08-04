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

import jakarta.inject.Singleton;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.event.Event;
import org.apache.ignite.internal.cli.event.EventFactory;
import org.apache.ignite.internal.cli.event.EventListener;
import org.apache.ignite.internal.cli.event.EventType;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.rest.client.api.NodeManagementApi;
import org.apache.ignite.rest.client.invoker.ApiException;

/**
 * Connection to node heart beat.
 */
@Singleton
public class ConnectionHeartBeat implements EventListener {

    private static final IgniteLogger log = CliLoggers.forClass(ConnectionHeartBeat.class);

    /** CLI check connection period period. */
    private final long cliCheckConnectionPeriodSecond;

    /** Scheduled executor for connection heartbeat. */
    @Nullable
    private ScheduledExecutorService scheduledConnectionHeartbeatExecutor;

    private final ApiClientFactory clientFactory;

    private final EventFactory eventFactory;

    private final AtomicBoolean connected = new AtomicBoolean(false);

    /**
     * Created instance of connection heartbeat.
     *
     * @param clientFactory api client factory.
     * @param eventFactory event factory.
     */
    public ConnectionHeartBeat(ApiClientFactory clientFactory, EventFactory eventFactory) {
        this.clientFactory = clientFactory;
        this.eventFactory = eventFactory;
        this.cliCheckConnectionPeriodSecond = 5; //ToDo: use micronaut config params
    }

    /**
     * Starts connection heartbeat. By default connection will be checked every 5 sec.
     *
     * @param sessionInfo session info with node url
     */
    private void onConnect(SessionInfo sessionInfo) {
        //eventFactory.fireEvent(EventType.CONNECTION_RESTORED, new ConnectionStatusEvent());

        if (scheduledConnectionHeartbeatExecutor == null) {
            scheduledConnectionHeartbeatExecutor =
                    Executors.newScheduledThreadPool(1, new NamedThreadFactory("cli-check-connection-thread", log));

            //Start connection heart beat
            scheduledConnectionHeartbeatExecutor.scheduleAtFixedRate(
                    () -> pingConnection(sessionInfo.nodeUrl()),
                    0,
                    cliCheckConnectionPeriodSecond,
                    TimeUnit.SECONDS
            );
        }
    }

    /**
     * Stops connection heartbeat.
     */
    private void onDisconnect() {
        if (scheduledConnectionHeartbeatExecutor != null) {
            scheduledConnectionHeartbeatExecutor.shutdownNow();
            scheduledConnectionHeartbeatExecutor = null;
        }
    }

    private void pingConnection(String nodeUrl) {
        try {
            new NodeManagementApi(clientFactory.getClient(nodeUrl)).nodeState();
            if (!connected.get()) {
                connected.compareAndSet(false, true);
                eventFactory.fireEvent(EventType.CONNECTION_RESTORED, new ConnectionStatusEvent());
            }
        } catch (ApiException exception) {
            if (connected.get()) {
                connected.compareAndSet(true, false);
                eventFactory.fireEvent(EventType.CONNECTION_LOST, new ConnectionStatusEvent());
            }
        }
    }

    @Override
    public void onEvent(EventType eventType, Event event) {
        if (EventType.SESSION_ON_CONNECT == eventType) {
            SessionConnectEvent sessionConnectEvent = (SessionConnectEvent) event;
            onConnect(sessionConnectEvent.getSessionInfo());
        } else if (EventType.SESSION_ON_DISCONNECT == eventType) {
            onDisconnect();
        }
    }
}
