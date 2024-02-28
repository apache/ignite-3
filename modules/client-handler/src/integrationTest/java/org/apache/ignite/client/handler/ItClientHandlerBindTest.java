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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletionException;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
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

    @Test
    void connectSpecificAddress(
            TestInfo testInfo,
            @InjectConfiguration("mock.listenAddress=localhost") ClientConnectorConfiguration clientConnectorConfiguration) {
        TestServer server = new TestServer(null, null, clientConnectorConfiguration, networkConfiguration);

        server.start(testInfo);
    }

    @Test
    void connectUnknownAddress(
            TestInfo testInfo,
            @InjectConfiguration("mock.listenAddress=unknown-address") ClientConnectorConfiguration clientConnectorConfiguration) {

        TestServer server = new TestServer(null, null, clientConnectorConfiguration, networkConfiguration);

        CompletionException e = assertThrows(CompletionException.class, () -> server.start(testInfo));
        assertTrue(e.getMessage().contains("Failed to start thin connector endpoint, address \"unknown-address\" is not found"));
    }
}
