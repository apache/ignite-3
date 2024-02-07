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
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.apache.ignite.internal.cli.core.repl.Session;
import org.apache.ignite.internal.cli.event.EventSubscriptionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@Property(name = "cli.check.connection.period.second", value = "1")
class ItConnectionHeartbeatTest extends CliIntegrationTest {

    @Inject
    Session session;

    @Inject
    EventSubscriptionManager eventSubscriptionManager;

    @Value("${cli.check.connection.period.second}")
    private long cliCheckConnectionPeriodSecond;

    private final AtomicInteger connectionLostCount = new AtomicInteger(0);
    private final AtomicInteger connectionRestoredCount = new AtomicInteger(0);

    @BeforeEach
    void resetConnectionCounts() {
        connectionLostCount.set(0);
        connectionRestoredCount.set(0);

        // Register listeners
        eventSubscriptionManager.subscribe(CONNECTION_LOST, event -> connectionLostCount.incrementAndGet());
        eventSubscriptionManager.subscribe(CONNECTION_RESTORED, event -> connectionRestoredCount.incrementAndGet());
    }

    @Override
    protected Class<?> getCommandClass() {
        return TopLevelCliReplCommand.class;
    }

    @Test
    @DisplayName("Should send event CONNECTION_RESTORED on connection start")
    void connectionEstablished() {
        // Given null session info before connect
        assertNull(session.info());

        // When connect without parameters
        execute("connect");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300")
        );

        // Listener was invoked
        await().timeout(cliCheckConnectionPeriodSecond * 2, TimeUnit.SECONDS).until(() -> connectionRestoredCount.get() == 1);
        assertEquals(0, connectionLostCount.get());
    }

    @Test
    @DisplayName("Should send event CONNECTION_LOST on cluster stop")
    void onConnectionLost() {
        // Given connected cli
        execute("connect");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300")
        );

        // When stop node
        int nodeIndex = CLUSTER.nodeIndex(session.info().nodeName());
        CLUSTER.stopNode(nodeIndex);

        // Listener was invoked
        await().timeout(cliCheckConnectionPeriodSecond * 2, TimeUnit.SECONDS).until(
                () -> connectionRestoredCount.get() == 1 && connectionLostCount.get() == 1
        );

        // Tear down. Restore initial state of node to exclude any impact on next test.
        CLUSTER.startNode(nodeIndex);
    }

    @Test
    @DisplayName("Should send event CONNECTION_LOST on cluster stop")
    void restoreConnectionAfterConnectionLost() {
        // Given connected cli
        execute("connect");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300")
        );

        // When stop node
        int nodeIndex = CLUSTER.nodeIndex(session.info().nodeName());
        CLUSTER.stopNode(nodeIndex);

        // Then connection lost event obtained
        await().timeout(cliCheckConnectionPeriodSecond * 2, TimeUnit.SECONDS).until(
                () -> connectionRestoredCount.get() == 1 && connectionLostCount.get() == 1
        );

        // When
        CLUSTER.startNode(nodeIndex);

        // Then one more connection restore event obtained
        await().timeout(cliCheckConnectionPeriodSecond * 2, TimeUnit.SECONDS).until(
                () -> connectionRestoredCount.get() == 2 && connectionLostCount.get() == 1
        );
    }
}
