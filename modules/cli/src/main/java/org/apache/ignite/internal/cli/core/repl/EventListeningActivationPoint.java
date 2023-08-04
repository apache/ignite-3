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
import org.apache.ignite.internal.cli.event.EventSubscriber;
import org.apache.ignite.internal.cli.event.EventType;

/**
 * Subscribes beans on appropriate events.
 */
@Singleton
public class EventListeningActivationPoint {

    @Inject
    private EventSubscriber eventSubscriber;

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
        eventSubscriber.listen(EventType.CONNECT, periodicSessionTaskExecutor);
        eventSubscriber.listen(EventType.DISCONNECT, periodicSessionTaskExecutor);

        eventSubscriber.listen(EventType.CONNECT, clusterConfigRegistry);
        eventSubscriber.listen(EventType.DISCONNECT, clusterConfigRegistry);

        eventSubscriber.listen(EventType.CONNECT, metricRegistry);
        eventSubscriber.listen(EventType.DISCONNECT, metricRegistry);

        eventSubscriber.listen(EventType.CONNECT, nodeConfigRegistry);
        eventSubscriber.listen(EventType.DISCONNECT, nodeConfigRegistry);

        eventSubscriber.listen(EventType.CONNECT, unitsRegistry);
        eventSubscriber.listen(EventType.DISCONNECT, unitsRegistry);

        eventSubscriber.listen(EventType.CONNECT, connectionHeartBeat);
        eventSubscriber.listen(EventType.DISCONNECT, connectionHeartBeat);

        eventSubscriber.listen(EventType.CONNECT, session);
        eventSubscriber.listen(EventType.DISCONNECT, session);

        eventSubscriber.listen(EventType.CONNECTION_LOST, promptProvider);
        eventSubscriber.listen(EventType.CONNECTION_RESTORED, promptProvider);
    }
}
