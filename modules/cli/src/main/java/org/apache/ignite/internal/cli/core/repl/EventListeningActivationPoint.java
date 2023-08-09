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

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.core.repl.prompt.ReplPromptProvider;
import org.apache.ignite.internal.cli.core.repl.registry.impl.ClusterConfigRegistryImpl;
import org.apache.ignite.internal.cli.core.repl.registry.impl.MetricRegistryImpl;
import org.apache.ignite.internal.cli.core.repl.registry.impl.NodeConfigRegistryImpl;
import org.apache.ignite.internal.cli.core.repl.registry.impl.UnitsRegistryImpl;
import org.apache.ignite.internal.cli.event.EventSubscriptionManager;
import org.apache.ignite.internal.cli.event.EventType;

/**
 * Subscribes beans on appropriate events.
 */
@Singleton
public class EventListeningActivationPoint {

    @Inject
    private EventSubscriptionManager eventSubscriptionManager;

    @Inject
    private PeriodicSessionTaskExecutor periodicSessionTaskExecutor;

    @Inject
    private ClusterConfigRegistryImpl clusterConfigRegistry;

    @Inject
    private MetricRegistryImpl metricRegistry;

    @Inject
    private NodeConfigRegistryImpl nodeConfigRegistry;

    @Inject
    private UnitsRegistryImpl unitsRegistry;

    @Inject
    private ConnectionHeartBeat connectionHeartBeat;

    @Inject
    private Session session;

    @Inject
    private ReplPromptProvider promptProvider;

    /**
     * Subscribes event listeners.
     */
    public void subscribe() {
        eventSubscriptionManager.subscribe(EventType.CONNECT, periodicSessionTaskExecutor);
        eventSubscriptionManager.subscribe(EventType.DISCONNECT, periodicSessionTaskExecutor);

        eventSubscriptionManager.subscribe(EventType.CONNECT, clusterConfigRegistry);
        eventSubscriptionManager.subscribe(EventType.DISCONNECT, clusterConfigRegistry);

        eventSubscriptionManager.subscribe(EventType.CONNECT, metricRegistry);
        eventSubscriptionManager.subscribe(EventType.DISCONNECT, metricRegistry);

        eventSubscriptionManager.subscribe(EventType.CONNECT, nodeConfigRegistry);
        eventSubscriptionManager.subscribe(EventType.DISCONNECT, nodeConfigRegistry);

        eventSubscriptionManager.subscribe(EventType.CONNECT, unitsRegistry);
        eventSubscriptionManager.subscribe(EventType.DISCONNECT, unitsRegistry);

        eventSubscriptionManager.subscribe(EventType.CONNECT, connectionHeartBeat);
        eventSubscriptionManager.subscribe(EventType.DISCONNECT, connectionHeartBeat);

        eventSubscriptionManager.subscribe(EventType.CONNECT, session);
        eventSubscriptionManager.subscribe(EventType.DISCONNECT, session);

        eventSubscriptionManager.subscribe(EventType.CONNECTION_LOST, promptProvider);
        eventSubscriptionManager.subscribe(EventType.CONNECTION_RESTORED, promptProvider);
    }
}
