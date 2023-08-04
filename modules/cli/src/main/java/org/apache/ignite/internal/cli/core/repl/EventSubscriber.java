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
import org.apache.ignite.internal.cli.core.repl.prompt.ReplPromptProvider;
import org.apache.ignite.internal.cli.core.repl.registry.impl.ClusterConfigRegistryImpl;
import org.apache.ignite.internal.cli.core.repl.registry.impl.MetricRegistryImpl;
import org.apache.ignite.internal.cli.core.repl.registry.impl.NodeConfigRegistryImpl;
import org.apache.ignite.internal.cli.core.repl.registry.impl.UnitsRegistryImpl;
import org.apache.ignite.internal.cli.event.EventFactory;
import org.apache.ignite.internal.cli.event.EventType;

/**
 * Subscribes beans on appropriate events.
 */
@Singleton
public class EventSubscriber {

    /**
     * Create event subscriber and subscribes event listeners.
     *
     * @param eventFactory event factory
     * @param periodicSessionTaskExecutor periodic task executor
     * @param clusterConfigRegistry cluster configuration registry
     * @param metricRegistry metric registry
     * @param nodeConfigRegistry node configuration registry
     * @param unitsRegistry units registry
     * @param connectionHeartBeat connection heartbeat service
     * @param session user session
     * @param promptProvider prompt provider
     */
    public EventSubscriber(EventFactory eventFactory, PeriodicSessionTaskExecutor periodicSessionTaskExecutor,
            ClusterConfigRegistryImpl clusterConfigRegistry, MetricRegistryImpl metricRegistry, NodeConfigRegistryImpl nodeConfigRegistry,
            UnitsRegistryImpl unitsRegistry, ConnectionHeartBeat connectionHeartBeat, Session session, ReplPromptProvider promptProvider) {
        eventFactory.listen(EventType.SESSION_ON_CONNECT, periodicSessionTaskExecutor);
        eventFactory.listen(EventType.SESSION_ON_DISCONNECT, periodicSessionTaskExecutor);

        eventFactory.listen(EventType.SESSION_ON_CONNECT, clusterConfigRegistry);
        eventFactory.listen(EventType.SESSION_ON_DISCONNECT, clusterConfigRegistry);

        eventFactory.listen(EventType.SESSION_ON_CONNECT, metricRegistry);
        eventFactory.listen(EventType.SESSION_ON_DISCONNECT, metricRegistry);

        eventFactory.listen(EventType.SESSION_ON_CONNECT, nodeConfigRegistry);
        eventFactory.listen(EventType.SESSION_ON_DISCONNECT, nodeConfigRegistry);

        eventFactory.listen(EventType.SESSION_ON_CONNECT, unitsRegistry);
        eventFactory.listen(EventType.SESSION_ON_DISCONNECT, unitsRegistry);

        eventFactory.listen(EventType.SESSION_ON_CONNECT, connectionHeartBeat);
        eventFactory.listen(EventType.SESSION_ON_DISCONNECT, connectionHeartBeat);

        eventFactory.listen(EventType.SESSION_ON_CONNECT, session);
        eventFactory.listen(EventType.SESSION_ON_DISCONNECT, session);

        eventFactory.listen(EventType.CONNECTION_LOST, promptProvider);
        eventFactory.listen(EventType.CONNECTION_RESTORED, promptProvider);
    }
}
