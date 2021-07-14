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

package org.apache.ignite.client;

import io.netty.channel.ChannelFuture;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.helpers.NOPLogger;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class ClientConnectionTest {
    private ChannelFuture serverFuture;

    @BeforeEach
    public void setUp() throws Exception {
        serverFuture = startServer();
    }

    @AfterEach
    public void tearDown() throws Exception {
        serverFuture.cancel(true);
        serverFuture.await();
    }

    @Test
    public void TestConnection() throws Exception {
        var builder = IgniteClient.builder().addresses("127.0.0.1");

        try (var client = builder.build()) {
            var tables = client.tables().tables();

            assertEquals(0, tables.size());
        }
    }

    private ChannelFuture startServer() throws InterruptedException {
        var registry = new ConfigurationRegistry(
                Collections.singletonList(ClientConnectorConfiguration.KEY),
                Collections.emptyMap(),
                Collections.singletonList(new TestConfigurationStorage(ConfigurationType.LOCAL))
        );

        var module = new ClientHandlerModule(mock(Ignite.class), NOPLogger.NOP_LOGGER);

        module.prepareStart(registry);

        return module.start();
    }
}
