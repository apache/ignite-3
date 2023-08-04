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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.cli.core.repl.Session;
import org.apache.ignite.internal.cli.event.EventFactory;
import org.apache.ignite.internal.cli.event.EventListener;
import org.apache.ignite.internal.cli.event.EventType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@Property(name = "cli.check.connection.period.second", value = "1")
class ItConnectionHeartbeatTest extends CliCommandTestInitializedIntegrationBase {

    @Inject
    Session session;

    @Inject
    EventFactory eventFactory;

    @Value("${cli.check.connection.period.second}")
    private long cliCheckConnectionPeriodSecond;

    private final AtomicInteger connectionLost = new AtomicInteger(0);
    private final AtomicInteger connectionRestored = new AtomicInteger(0);

    @BeforeEach
    void setUp() {
        //ToDo: Set connection check timeout to 1 sec to make test fast
        connectionLost.set(0);
        connectionRestored.set(0);
        EventListener eventListener = (eventType, event) -> {
            if (EventType.CONNECTION_LOST == eventType) {
                connectionLost.incrementAndGet();
            } else if (EventType.CONNECTION_RESTORED == eventType) {
                connectionRestored.incrementAndGet();
            }
        };

        //Register listeners
        eventFactory.listen(EventType.CONNECTION_LOST, eventListener);
        eventFactory.listen(EventType.CONNECTION_RESTORED, eventListener);
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

        //Listener was invoked
        await().timeout(cliCheckConnectionPeriodSecond * 2, TimeUnit.SECONDS).until(() -> connectionRestored.get() == 1);
        assertEquals(0, connectionLost.get());
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
        String nodeName = session.info().nodeName();
        this.stopNode(nodeName);

        //Listener was invoked
        await().timeout(cliCheckConnectionPeriodSecond * 2, TimeUnit.SECONDS).until(() -> connectionRestored.get() == 1);
        await().timeout(cliCheckConnectionPeriodSecond * 2, TimeUnit.SECONDS).until(() -> connectionLost.get() == 1);

        //Tear down
        this.startNode(nodeName);
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
        String nodeName = session.info().nodeName();
        this.stopNode(nodeName);

        //Then
        await().timeout(cliCheckConnectionPeriodSecond * 2, TimeUnit.SECONDS).until(() -> connectionRestored.get() == 1);
        await().timeout(cliCheckConnectionPeriodSecond * 2, TimeUnit.SECONDS).until(() -> connectionLost.get() == 1);

        // When
        this.startNode(nodeName);

        //Then
        await().timeout(cliCheckConnectionPeriodSecond * 2, TimeUnit.SECONDS).until(() -> connectionRestored.get() == 2);
        await().timeout(cliCheckConnectionPeriodSecond * 2, TimeUnit.SECONDS).until(() -> connectionLost.get() == 1);
    }
}
