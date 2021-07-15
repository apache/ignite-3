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
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.helpers.NOPLogger;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class ClientTablesTest {
    private static final String DEFAULT_TABLE = "default_test_table";

    private static ChannelFuture serverFuture;

    private static Ignite server;

    private static Ignite client;

    @BeforeAll
    public static void beforeAll() throws Exception {
        serverFuture = startServer();
        client = startClient();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        client.close();
        serverFuture.cancel(true);
        serverFuture.await();
    }

    @BeforeEach
    public void beforeEach() {
        for (var t : server.tables().tables())
            server.tables().dropTable(t.tableName());
    }

    @Test
    public void testTablesWhenTablesExist() {
        server.tables().createTable(DEFAULT_TABLE, null);
        server.tables().createTable("t", null);

        var tables = client.tables().tables();
        assertEquals(2, tables.size());

        tables.sort(Comparator.comparing(Table::tableName));
        assertEquals(DEFAULT_TABLE, tables.get(0).tableName());
        assertEquals("t", tables.get(1).tableName());
    }

    @Test
    public void testTablesWhenNoTablesExist() {
        var tables = client.tables().tables();

        assertEquals(0, tables.size());
    }

    @Test
    public void testCreateTable() {
        var clientTable = client.tables().createTable("t1", t -> t.changeReplicas(2));
        assertEquals("t1", clientTable.tableName());

        var serverTables = server.tables().tables();
        assertEquals(1, serverTables.size());

        var serverTable = serverTables.get(0);
        assertEquals("t1", serverTable.tableName());
        assertEquals(((TableImpl) serverTable).tableId(), ((TableImpl) clientTable).tableId());
    }

    @Test
    public void testGetOrCreateTable() {
        // TODO
    }

    @Test
    public void testGetTableByName() {
        // TODO
    }

    @Test
    public void testDropTable() {
        server.tables().createTable("t", t -> t.changeReplicas(0));

        client.tables().dropTable("t");

        assertEquals(0, server.tables().tables().size());
    }

    @Test
    public void testDropTableInvalidName() {
        client.tables().dropTable("foo");
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

        var module = new ClientHandlerModule(server, NOPLogger.NOP_LOGGER);

        module.prepareStart(registry);

        return module.start();
    }
}
