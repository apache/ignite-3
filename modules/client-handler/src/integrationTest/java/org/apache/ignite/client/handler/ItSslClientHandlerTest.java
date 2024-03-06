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

import static org.apache.ignite.client.handler.ItClientHandlerTestUtils.generateKeystore;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.SocketException;
import java.nio.file.Path;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/** SSL client integration test. */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ItSslClientHandlerTest extends BaseIgniteAbstractTest {
    private ClientHandlerModule serverModule;

    private TestServer testServer;

    private String keyStorePkcs12Path;

    @InjectConfiguration
    private NetworkConfiguration networkConfiguration;

    @InjectConfiguration
    private ClientConnectorConfiguration clientConnectorConfiguration;

    @WorkDirectory
    private Path workDir;

    @BeforeEach
    void setUp() throws Exception {
        keyStorePkcs12Path = generateKeystore(workDir);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (testServer != null) {
            testServer.tearDown();
        }
    }

    @Test
    @DisplayName("When SSL not configured (by default) the client can connect")
    void sslNotConfigured(TestInfo testInfo) throws IOException {
        // Given server started
        testServer = new TestServer(clientConnectorConfiguration, networkConfiguration);
        serverModule = testServer.start(testInfo);

        // Then
        performAndCheckMagic();
    }

    @Test
    @DisplayName("When SSL configured on the server but not configured on the client then exception is thrown")
    void sslConfiguredOnlyForServer(TestInfo testInfo) {
        // Given server started
        testServer = new TestServer(
                TestSslConfig.builder()
                        .keyStorePath(keyStorePkcs12Path)
                        .keyStorePassword("changeit")
                        .build(),
                null,
                clientConnectorConfiguration,
                networkConfiguration
        );
        serverModule = testServer.start(testInfo);

        // Then
        assertThrows(SocketException.class, this::performAndCheckMagic);
    }

    @Test
    @DisplayName("When SSL is configured incorrectly then exception is thrown on start")
    @SuppressWarnings("ThrowableNotThrown")
    void sslMisconfigured(TestInfo testInfo) {
        testServer = new TestServer(
                TestSslConfig.builder()
                        .keyStorePath(keyStorePkcs12Path)
                        .keyStorePassword("wrong-password")
                        .build(),
                null,
                clientConnectorConfiguration,
                networkConfiguration
        );

        assertThrowsWithCause(() -> testServer.start(testInfo), IgniteException.class, "keystore password was incorrect");
    }

    private void performAndCheckMagic() throws IOException {
        ItClientHandlerTestUtils.connectAndHandshake(serverModule);
    }
}
