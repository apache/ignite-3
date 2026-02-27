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
import java.util.Properties;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.properties.IgniteProperties;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Client handler metrics tests. See also {@code org.apache.ignite.client.MetricsTest}.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ItClientHandlerMetricsTest extends BaseIgniteAbstractTest {
    private static String igniteVersion;

    private static Properties props;

    @InjectConfiguration
    private NetworkConfiguration networkConfiguration;

    @InjectConfiguration
    private ClientConnectorConfiguration clientConnectorConfiguration;

    private TestServer testServer;

    @WorkDirectory
    private Path workDir;

    @AfterEach
    void tearDown() {
        if (testServer != null) {
            testServer.tearDown();
        }
    }

    @BeforeAll
    static void beforeAll() {
        props = IgniteTestUtils.getFieldValue(null, IgniteProperties.class, "PROPS");
        igniteVersion = props.getProperty(IgniteProperties.VERSION);
        props.setProperty(IgniteProperties.VERSION, "3.0.0-SNAPSHOT");
    }

    @AfterAll
    static void afterAll() {
        props.setProperty(IgniteProperties.VERSION, igniteVersion);
    }

    @Test
    void testSessionsRejectedTls(TestInfo testInfo) throws Exception {
        testServer = new TestServer(
                TestSslConfig.builder()
                        .keyStorePath(ItClientHandlerTestUtils.generateKeystore(workDir))
                        .keyStorePassword("changeit")
                        .build(),
                null,
                clientConnectorConfiguration,
                networkConfiguration
        );

        var serverModule = testServer.start(testInfo);

        assertThrows(SocketException.class, () -> ItClientHandlerTestUtils.connectAndHandshake(serverModule));

        assertEquals(1, testServer.metrics().sessionsRejectedTls());
        assertEquals(0, testServer.metrics().sessionsRejected());
        assertEquals(0, testServer.metrics().sessionsAccepted());
    }

    @Test
    void testSessionsRejected(TestInfo testInfo) throws Exception {
        testServer = new TestServer(null, null, clientConnectorConfiguration, networkConfiguration);
        var serverModule = testServer.start(testInfo);

        // Bad MAGIC.
        assertThrows(SocketException.class, () -> ItClientHandlerTestUtils.connectAndHandshake(serverModule, true, false));
        assertEquals(1, testServer.metrics().sessionsRejected());

        // Bad version.
        ItClientHandlerTestUtils.connectAndHandshake(serverModule, false, true);
        assertTrue(
                IgniteTestUtils.waitForCondition(() -> testServer.metrics().sessionsRejected() == 2, 1000),
                () -> "sessionsRejected: " + testServer.metrics().sessionsRejected());

        assertEquals(0, testServer.metrics().sessionsRejectedTls());
        assertEquals(0, testServer.metrics().sessionsAccepted());
        assertEquals(0, testServer.metrics().sessionsActive());
        assertEquals(2, testServer.metrics().connectionsInitiated());
    }

    @Test
    void testSessionsRejectedTimeout(TestInfo testInfo) throws Exception {
        testServer = new TestServer(null, null, clientConnectorConfiguration, networkConfiguration);
        testServer.idleTimeout(300);
        var serverModule = testServer.start(testInfo);

        try (var ignored = ItClientHandlerTestUtils.connectAndHandshakeAndGetSocket(serverModule)) {
            assertTrue(IgniteTestUtils.waitForCondition(() ->
                    testServer.metrics().sessionsRejectedTimeout() == 1, 5_000));
        }

        assertEquals(1, testServer.metrics().sessionsAccepted());

        assertTrue(IgniteTestUtils.waitForCondition(() ->
                testServer.metrics().sessionsActive() == 0, 5_000));
    }

    @Test
    void testSessionsAccepted(TestInfo testInfo) throws Exception {
        testServer = new TestServer(null, null, clientConnectorConfiguration, networkConfiguration);
        var serverModule = testServer.start(testInfo);

        ItClientHandlerTestUtils.connectAndHandshake(serverModule);
        assertTrue(IgniteTestUtils.waitForCondition(() -> testServer.metrics().sessionsAccepted() == 1, 1000));
    }

    @Test
    void testSessionsActive(TestInfo testInfo) throws Exception {
        testServer = new TestServer(null, null, clientConnectorConfiguration, networkConfiguration);
        var serverModule = testServer.start(testInfo);

        try (var ignored = ItClientHandlerTestUtils.connectAndHandshakeAndGetSocket(serverModule)) {
            assertTrue(IgniteTestUtils.waitForCondition(() -> testServer.metrics().sessionsActive() == 1, 1000));
        }

        assertTrue(IgniteTestUtils.waitForCondition(() -> testServer.metrics().sessionsActive() == 0, 5_000));
    }

    @Test
    void testBytesSentReceived(TestInfo testInfo) throws Exception {
        testServer = new TestServer(null, null, clientConnectorConfiguration, networkConfiguration);
        var serverModule = testServer.start(testInfo);

        assertEquals(0, testServer.metrics().bytesSent());
        assertEquals(0, testServer.metrics().bytesReceived());

        ItClientHandlerTestUtils.connectAndHandshake(serverModule);

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> testServer.metrics().bytesSent() == 108, 1000),
                () -> "bytesSent: " + testServer.metrics().bytesSent());

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> testServer.metrics().bytesReceived() == 15, 1000),
                () -> "bytesReceived: " + testServer.metrics().bytesReceived());

        ItClientHandlerTestUtils.connectAndHandshake(serverModule, false, true);

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> testServer.metrics().bytesSent() == 320, 1000),
                () -> "bytesSent: " + testServer.metrics().bytesSent());

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> testServer.metrics().bytesReceived() == 30, 1000),
                () -> "bytesReceived: " + testServer.metrics().bytesReceived());
    }
}
