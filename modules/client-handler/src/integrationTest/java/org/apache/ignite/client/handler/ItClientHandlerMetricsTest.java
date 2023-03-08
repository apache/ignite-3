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

package org.apache.ignite.client.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.SocketException;
import java.nio.file.Path;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Client handler metrics tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItClientHandlerMetricsTest {
    private TestServer testServer;

    @WorkDirectory
    private Path workDir;

    @AfterEach
    void tearDown() throws Exception {
        if (testServer != null) {
            testServer.tearDown();
        }
    }

    @Test
    void sessionsRejectedTls(TestInfo testInfo) throws Exception {
        testServer = new TestServer(
                TestSslConfig.builder()
                        .keyStorePath(ItClientHandlerTestUtils.generateKeystore(workDir))
                        .keyStorePassword("changeit")
                        .build()
        );

        var serverModule = testServer.start(testInfo);

        assertThrows(SocketException.class, () -> ItClientHandlerTestUtils.connectAndHandshake(serverModule));

        assertEquals(1, testServer.metrics().sessionsRejectedTls().value());
        assertEquals(0, testServer.metrics().sessionsRejected().value());
        assertEquals(0, testServer.metrics().sessionsAccepted().value());
    }

    @Test
    void sessionsRejected(TestInfo testInfo) throws Exception {
        testServer = new TestServer(null);
        var serverModule = testServer.start(testInfo);

        // Bad MAGIC.
        assertThrows(SocketException.class, () -> ItClientHandlerTestUtils.connectAndHandshake(serverModule, true, false));
        assertEquals(1, testServer.metrics().sessionsRejected().value());

        // Bad version.
        ItClientHandlerTestUtils.connectAndHandshake(serverModule, false, true);
        assertEquals(2, testServer.metrics().sessionsRejected().value());

        assertEquals(0, testServer.metrics().sessionsRejectedTls().value());
        assertEquals(0, testServer.metrics().sessionsAccepted().value());
        assertEquals(0, testServer.metrics().sessionsActive().value());
        assertEquals(2, testServer.metrics().connectionsInitiated().value());
    }

    @Test
    void sessionsRejectedTimeout(TestInfo testInfo) throws Exception {
        testServer = new TestServer(null);
        testServer.idleTimeout(300);
        var serverModule = testServer.start(testInfo);

        try (var ignored = ItClientHandlerTestUtils.connectAndHandshakeAndGetSocket(serverModule)) {
            assertTrue(IgniteTestUtils.waitForCondition(() ->
                    testServer.metrics().sessionsRejectedTimeout().value() == 1, 5_000));
        }

        assertEquals(1, testServer.metrics().sessionsAccepted().value());
        assertEquals(0, testServer.metrics().sessionsActive().value());
    }

    @Test
    void sessionsAccepted(TestInfo testInfo) throws Exception {
        testServer = new TestServer(null);
        var serverModule = testServer.start(testInfo);

        ItClientHandlerTestUtils.connectAndHandshake(serverModule);
        assertTrue(IgniteTestUtils.waitForCondition(() -> testServer.metrics().sessionsAccepted().value() == 1, 1000));
    }

    @Test
    void sessionsActive(TestInfo testInfo) throws Exception {
        testServer = new TestServer(null);
        var serverModule = testServer.start(testInfo);

        try (var ignored = ItClientHandlerTestUtils.connectAndHandshakeAndGetSocket(serverModule)) {
            assertTrue(IgniteTestUtils.waitForCondition(() -> testServer.metrics().sessionsActive().value() == 1, 1000));
        }

        assertTrue(IgniteTestUtils.waitForCondition(() -> testServer.metrics().sessionsActive().value() == 0, 5_000));
    }

    @Test
    void bytesSentReceived(TestInfo testInfo) throws Exception {
        testServer = new TestServer(null);
        var serverModule = testServer.start(testInfo);

        assertEquals(0, testServer.metrics().bytesSent().value());
        assertEquals(0, testServer.metrics().bytesReceived().value());

        ItClientHandlerTestUtils.connectAndHandshake(serverModule);

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> testServer.metrics().bytesSent().value() == 54, 1000),
                () -> "bytesSent: " + testServer.metrics().bytesSent().value());

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> testServer.metrics().bytesReceived().value() == 15, 1000),
                () -> "bytesReceived: " + testServer.metrics().bytesReceived().value());

        ItClientHandlerTestUtils.connectAndHandshake(serverModule, false, true);

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> testServer.metrics().bytesSent().value() == 216, 1000),
                () -> "bytesSent: " + testServer.metrics().bytesSent().value());

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> testServer.metrics().bytesReceived().value() == 30, 1000),
                () -> "bytesReceived: " + testServer.metrics().bytesReceived().value());
    }

    @Test
    public void todo() {
        // TODO:
        // requests: active, processed, failed
        // transactions.active
        // cursors.active
    }
}
