/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.clientconnector;

import io.netty.channel.ChannelFuture;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.junit.jupiter.api.Test;
import org.slf4j.helpers.NOPLogger;

import java.io.OutputStream;
import java.net.Socket;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Client connector integration tests with real sockets.
 */
public class ClientConnectorIntegrationTest {
    @Test
    void testHandshakeInvalidMagicHeaderDropsConnection() throws Exception {
        ChannelFuture channelFuture = startServer();

        try {
            var sock = new Socket("127.0.0.1", 10800);
            OutputStream out = sock.getOutputStream();
            out.write(new byte[]{63, 64, 65, 66, 67});
            out.flush();

            // TODO: Connection should be dropped by server at this point.
            var res = sock.getInputStream().readAllBytes();
            assertEquals(0, res.length);

            out.write(new byte[]{63, 64, 65, 66, 67});
            out.flush();

            res = sock.getInputStream().readAllBytes();
            assertEquals(0, res.length);

            assertFalse(sock.isConnected());
        } finally {
            channelFuture.cancel(true);
            channelFuture.await();
        }
    }

    private ChannelFuture startServer() throws InterruptedException {
        var registry = new ConfigurationRegistry(
                Collections.singletonList(ClientConnectorConfiguration.KEY),
                Collections.emptyMap(),
                Collections.singletonList(new TestConfigurationStorage(ConfigurationType.LOCAL))
        );

        var module = new ClientConnectorModule(NOPLogger.NOP_LOGGER);

        module.prepareStart(registry);

        return module.start();
    }
}
