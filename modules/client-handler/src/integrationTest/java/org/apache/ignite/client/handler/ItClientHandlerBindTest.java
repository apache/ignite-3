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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ConnectException;
import java.net.Socket;
import java.util.concurrent.CompletionException;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Client connector address configuration tests.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItClientHandlerBindTest extends BaseIgniteAbstractTest {
    @InjectConfiguration
    private NetworkConfiguration networkConfiguration;

    private TestServer server;

    private ClientHandlerModule serverModule;

    @AfterEach
    final void tearDown() throws Exception {
        closeAll(
                server == null ? null : () -> server.tearDown(),
                serverModule == null ? null : () -> assertThat(serverModule.stopAsync(), willCompleteSuccessfully())
        );
    }

    @Test
    void listenOnlySpecificAddress(
            TestInfo testInfo,
            @InjectConfiguration("mock.listenAddress=127.0.0.7") ClientConnectorConfiguration clientConnectorConfiguration
    ) throws Exception {
        server = new TestServer(null, null, clientConnectorConfiguration, networkConfiguration);

        serverModule = assertDoesNotThrow(() -> server.start(testInfo));

        int port = serverModule.localAddress().getPort();

        assertDoesNotThrow(
                () -> {
                    Socket socket = new Socket("127.0.0.7", port);
                    socket.close();
                });

        assertThrows(
                ConnectException.class,
                () -> {
                    Socket socket = new Socket("127.0.0.1", port);
                    socket.close();
                });
    }

    @Test
    void listenUnknownAddress(
            TestInfo testInfo,
            @InjectConfiguration("mock.listenAddress=unknown-address") ClientConnectorConfiguration clientConnectorConfiguration
    ) {
        server = new TestServer(null, null, clientConnectorConfiguration, networkConfiguration);

        CompletionException e = assertThrows(CompletionException.class, () -> server.start(testInfo));
        assertTrue(e.getMessage().contains("Failed to start thin connector endpoint, unresolved socket address \"unknown-address\""));
    }
}
