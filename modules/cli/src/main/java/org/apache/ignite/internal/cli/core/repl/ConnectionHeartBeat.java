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
import org.apache.ignite.internal.cli.core.repl.SessionInfo.ConnectionStatus;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.rest.client.api.NodeManagementApi;
import org.apache.ignite.rest.client.invoker.ApiException;

/**
 * Connection to node heart beat.
 */
@Singleton
public class ConnectionHeartBeat {

    private static final IgniteLogger log = CliLoggers.forClass(Session.class);

    /** CLI check connection period period. */
    private static final int CLI_CHECK_CONNECTION_PERIOD_SECONDS = 10;

    /** Scheduled executor for idle safe time sync. */
    private final ScheduledExecutorService scheduledIdleSafeTimeSyncExecutor =
            Executors.newScheduledThreadPool(1, new NamedThreadFactory("cli-check-connection-thread", log));

    /**
     * Start connection heartbeat.
     *
     * @param session session
     * @param clientFactory api client factory to ping connection.
     */
    public void start(Session session, ApiClientFactory clientFactory) {
        scheduledIdleSafeTimeSyncExecutor.scheduleAtFixedRate(
                () -> pingConnection(session, clientFactory),
                0,
                CLI_CHECK_CONNECTION_PERIOD_SECONDS,
                TimeUnit.SECONDS
        );
    }

    public void stop() {
        scheduledIdleSafeTimeSyncExecutor.shutdownNow();
    }

    private static void pingConnection(Session session, ApiClientFactory clientFactory) {
        try {
            new NodeManagementApi(clientFactory.getClient(session.info().nodeUrl())).nodeState();
            SessionInfo currentSessionInfo = session.info();
            session.updateSessionInfo(new SessionInfo(
                    currentSessionInfo.nodeUrl(),
                    currentSessionInfo.nodeName(),
                    currentSessionInfo.jdbcUrl(),
                    currentSessionInfo.username(),
                    ConnectionStatus.OPEN));
        } catch (ApiException exception) {
            SessionInfo currentSessionInfo = session.info();
            session.updateSessionInfo(new SessionInfo(
                    currentSessionInfo.nodeUrl(),
                    currentSessionInfo.nodeName(),
                    currentSessionInfo.jdbcUrl(),
                    currentSessionInfo.username(),
                    ConnectionStatus.BROkEN));
        }
    }
}
