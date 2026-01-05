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

package org.apache.ignite.internal.runner.app.client;

import static org.apache.ignite.lang.ErrorGroups.Table.TABLE_NOT_FOUND_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests thin client connecting to a real server node.
 */
@SuppressWarnings("resource")
@ExtendWith(WorkDirectoryExtension.class)
public class ItThinClientConnectionTest extends ItAbstractThinClientTest {
    /**
     * Check that thin client can connect to any server node and work with table API.
     */
    @Test
    void testThinClientConnectsToServerNodesAndExecutesBasicTableOperations() {
        for (var addr : getClientAddresses()) {
            try (var client = IgniteClient.builder().addresses(addr).build()) {
                List<Table> tables = client.tables().tables();
                assertEquals(1, tables.size());

                Table table = tables.get(0);
                assertEquals(TABLE_NAME, table.qualifiedName().objectName());

                var tuple = Tuple.create().set(COLUMN_KEY, 1).set(COLUMN_VAL, "Hello");
                var keyTuple = Tuple.create().set(COLUMN_KEY, 1);

                RecordView<Tuple> recView = table.recordView();

                recView.upsert(null, tuple);
                assertEquals("Hello", recView.get(null, keyTuple).stringValue(COLUMN_VAL));

                var kvView = table.keyValueView();
                assertEquals("Hello", kvView.get(null, keyTuple).stringValue(COLUMN_VAL));

                var pojoView = table.recordView(TestPojo.class);
                assertEquals("Hello", pojoView.get(null, new TestPojo(1)).val);

                assertTrue(recView.delete(null, keyTuple));

                List<ClusterNode> nodes = client.connections();
                assertEquals(1, nodes.size());
                assertThat(nodes.get(0).name(), startsWith("itcct_n_"));
            }
        }
    }

    @SuppressWarnings("resource")
    @Test
    void testAccessDroppedTableThrowsTableDoesNotExistsError() {
        IgniteSql sql = client().sql();
        sql.execute(null, "CREATE TABLE IF NOT EXISTS DELME (key INTEGER PRIMARY KEY)");

        var table = client().tables().table("DELME");
        sql.execute(null, "DROP TABLE DELME");

        IgniteException ex = assertThrows(IgniteException.class, () -> table.recordView(Integer.class).delete(null, 1));
        assertEquals(TABLE_NOT_FOUND_ERR, ex.code(), ex.getMessage());
    }

    @Test
    void clusterName() {
        assertThat(((TcpIgniteClient) client()).clusterName(), is("cluster"));
    }

    @Test
    void testHeartbeat() {
        var client = (TcpIgniteClient) client();

        List<ClientChannel> channels = client.channel().channels();

        assertEquals(2, channels.size());

        for (var channel : channels) {
            assertFalse(channel.closed());

            channel.heartbeatAsync(null).join();
            channel.heartbeatAsync(w -> w.out().packString("foo-bar")).join();
            channel.heartbeatAsync(w -> w.out().writePayload(new byte[]{1, 2, 3})).join();
        }
    }

    @Test
    void testExceptionHasHint() {
        // Execute on all nodes to collect all types of exception.
        List<String> causes = IntStream.range(0, client().configuration().addresses().length)
                .mapToObj(i -> {
                    IgniteException ex = assertThrows(IgniteException.class, () -> client().sql().execute(null, "select x from bad"));

                    return ex.getCause().getCause().getCause().getCause().getMessage();
                })
                .collect(Collectors.toList());

        assertThat(causes,
                hasItem(containsString("To see the full stack trace, set clientConnector.sendServerExceptionStackTraceToClient:true on the server")));
    }

    @Test
    void testServerReturnsActualTableName() {
        // Quoting is not necessary.
        Table table = client().tables().table("tbl1");
        assertEquals("TBL1", table.qualifiedName().objectName());

        // Quoting is necessary.
        client().sql().execute(null, "CREATE TABLE IF NOT EXISTS \"tbl-2\" (key INTEGER PRIMARY KEY)");

        try {
            Table table2 = client().tables().table("\"tbl-2\"");
            assertEquals("tbl-2", table2.qualifiedName().objectName());
        } finally {
            client().sql().execute(null, "DROP TABLE \"tbl-2\"");
        }
    }
}
