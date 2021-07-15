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
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.helpers.NOPLogger;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class ClientTablesTest {
    private static final String DEFAULT_TABLE = "default_test_table";

    private static ChannelFuture serverFuture;

    private static Ignite server;

    private static Ignite client;

    @BeforeAll
    public static void setUp() throws Exception {
        serverFuture = startServer();
        client = startClient();
    }

    @AfterAll
    public static void tearDown() throws Exception {
        client.close();
        serverFuture.cancel(true);
        serverFuture.await();
    }

    @Test
    public void testTablesReturnsDefaultTable() {
        var tables = client.tables().tables();

        assertEquals(1, tables.size());
        assertEquals(DEFAULT_TABLE, tables.get(0).tableName());
    }

    private static Ignite startClient() {
        var builder = IgniteClient.builder().addresses("127.0.0.2:10800");

        return builder.build();
    }

    private static ChannelFuture startServer() throws InterruptedException {
        var registry = new ConfigurationRegistry(
                Collections.singletonList(ClientConnectorConfiguration.KEY),
                Collections.emptyMap(),
                Collections.singletonList(new TestConfigurationStorage(ConfigurationType.LOCAL))
        );

        server = new FakeIgnite();
        server.tables().createTable(DEFAULT_TABLE, null);

        var module = new ClientHandlerModule(server, NOPLogger.NOP_LOGGER);

        module.prepareStart(registry);

        return module.start();
    }
}
