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

package org.apache.ignite.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;
import java.util.concurrent.CompletionException;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.internal.table.TableView;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests client tables.
 */
public class ClientTablesTest extends AbstractClientTest {
    @Test
    public void testTablesWhenTablesExist() {
        ((FakeIgniteTables) server.tables()).createTable(DEFAULT_TABLE);
        ((FakeIgniteTables) server.tables()).createTable("t");

        var tables = client.tables().tables();
        assertEquals(2, tables.size());

        tables.sort(Comparator.comparing(Table::name));
        assertEquals(DEFAULT_TABLE, tables.get(0).name());
        assertEquals("t", tables.get(1).name());
    }

    @Test
    public void testTablesWhenNoTablesExist() {
        var tables = client.tables().tables();

        assertEquals(0, tables.size());
    }

    @Test
    public void testTableReturnsInstanceWhenExists() {
        ((FakeIgniteTables) server.tables()).createTable(DEFAULT_TABLE);
        Table table = client.tables().table(DEFAULT_TABLE);

        assertEquals(DEFAULT_TABLE, table.name());
    }

    @Test
    public void testTableReturnsNullWhenDoesNotExist() {
        Table table = client.tables().table("non-existent-table");

        assertNull(table);
    }

    @Test
    @Disabled("IGNITE-15179")
    public void testCreateTable() {
        var clientTable = ((FakeIgniteTables) server.tables()).createTable("t1");
        assertEquals("t1", clientTable.name());

        var serverTables = server.tables().tables();
        assertEquals(1, serverTables.size());

        var serverTable = serverTables.get(0);
        assertEquals("t1", serverTable.name());
        assertEquals(((TableView) serverTable).tableId(), ((ClientTable) clientTable).tableId());
    }

    @Test
    @Disabled("IGNITE-15179")
    public void testCreateTableWhenExists() {
        ((FakeIgniteTables) server.tables()).createTable(DEFAULT_TABLE);

        var ex = assertThrows(CompletionException.class,
                () -> ((FakeIgniteTables) server.tables()).createTable(DEFAULT_TABLE));

        assertTrue(ex.getMessage().endsWith(FakeIgniteTables.TABLE_EXISTS));
        assertEquals(TableAlreadyExistsException.class, ex.getCause().getClass());

        var serverTables = server.tables().tables();
        assertEquals(1, serverTables.size());
    }
}
