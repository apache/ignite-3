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

package org.apache.ignite.internal.cli.commands;

import static org.apache.ignite.internal.cli.event.EventType.CONNECTION_LOST;
import static org.apache.ignite.internal.cli.event.EventType.CONNECTION_RESTORED;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

import io.micronaut.context.annotation.Property;
import jakarta.inject.Inject;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.apache.ignite.internal.cli.event.EventSubscriptionManager;
import org.apache.ignite.internal.cli.event.EventType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Property(name = "cli.check.connection.period.second", value = "1")
class ItConnectionHeartbeatTest extends CliIntegrationTest {
    @Inject
    private EventSubscriptionManager eventSubscriptionManager;

    private final AtomicReference<EventType> lastEvent = new AtomicReference<>();

    @BeforeEach
    void resetConnectionCounts() {
        lastEvent.set(null);

        // Register listeners
        eventSubscriptionManager.subscribe(CONNECTION_LOST, event -> lastEvent.set(event.eventType()));
        eventSubscriptionManager.subscribe(CONNECTION_RESTORED, event -> lastEvent.set(event.eventType()));
    }

    @Test
    void checkEvents() {
        // When CLI is connected
        connect(NODE_URL);

        // Then listener was invoked with connection restored event
        await().until(lastEvent::get, is(CONNECTION_RESTORED));

        // When stop node
        CLUSTER.stopNode(0);

        // Then connection lost event obtained
        await().until(lastEvent::get, is(CONNECTION_LOST));

        // When start node again
        CLUSTER.startNode(0);

        // Then one more connection restore event obtained
        await().until(lastEvent::get, is(CONNECTION_RESTORED));
    }
}
