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

import io.netty.util.ResourceLeakDetector;
import java.util.ArrayList;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.client.fakes.FakeInternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests partition awareness.
 */
public class PartitionAwarenessTest extends AbstractClientTest {
    protected static TestServer testServer2;

    protected static Ignite server2;

    protected static IgniteClient client2;

    protected static int serverPort2;

    private TableImpl table1;

    private TableImpl table2;

    private String lastOp;

    private String lastOpServerName;

    /**
     * Before all.
     */
    @BeforeAll
    public static void beforeAll() {
        AbstractClientTest.beforeAll();

        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        server2 = new FakeIgnite("server-2");
        testServer2 = startServer(10800, 10, 0, server2, "server-2");
        serverPort2 = testServer2.port();

        client2 = startClient("127.0.0.1:" + serverPort, "127.0.0.1:" + serverPort2);
    }

    /**
     * After all.
     */
    @AfterAll
    public static void afterAll() throws Exception {
        AbstractClientTest.afterAll();

        testServer2.close();
    }

    @BeforeEach
    public void beforeEach() {
        dropTables(server);
        dropTables(server2);

        initPartitionAssignment(null);
    }

    @Test
    public void testGetRoutesRequestToPrimaryNode() {
        RecordView<Tuple> recordView = defaultTable().recordView();

        assertOpOnNode("server-1", "get", x -> recordView.get(null, Tuple.create().set("ID", 0L)));
        assertOpOnNode("server-2", "get", x -> recordView.get(null, Tuple.create().set("ID", 1L)));
        assertOpOnNode("server-1", "get", x -> recordView.get(null, Tuple.create().set("ID", 2L)));
        assertOpOnNode("server-2", "get", x -> recordView.get(null, Tuple.create().set("ID", 3L)));
    }

    @Test
    public void testNonNullTxDisablesPartitionAwareness() {
        RecordView<Tuple> recordView = defaultTable().recordView();
        var tx = client2.transactions().begin();

        assertOpOnNode("server-2", "get", x -> recordView.get(tx, Tuple.create().set("ID", 0L)));
        assertOpOnNode("server-2", "get", x -> recordView.get(tx, Tuple.create().set("ID", 1L)));
        assertOpOnNode("server-2", "get", x -> recordView.get(tx, Tuple.create().set("ID", 2L)));
    }

    @Test
    public void testClientReceivesPartitionAssignmentUpdates() {
        // Check default assignment.
        RecordView<Tuple> recordView = defaultTable().recordView();

        assertOpOnNode("server-1", "get", x -> recordView.get(null, Tuple.create().set("ID", 0L)));
        assertOpOnNode("server-2", "get", x -> recordView.get(null, Tuple.create().set("ID", 1L)));

        // Update partition assignment.
        var assignments = new ArrayList<String>();

        assignments.add(testServer2.nodeId());
        assignments.add(testServer.nodeId());

        initPartitionAssignment(assignments);

        // Perform one request to receive a change notification flag and drop cached assignment.
        recordView.get(null, Tuple.create().set("ID", 0L));

        // Check new assignment.
        assertOpOnNode("server-2", "get", x -> recordView.get(null, Tuple.create().set("ID", 0L)));
        assertOpOnNode("server-1", "get", x -> recordView.get(null, Tuple.create().set("ID", 1L)));
    }

    @Test
    public void testCustomColocationKey() {
        RecordView<Tuple> recordView = table(FakeIgniteTables.TABLE_COLOCATION_KEY).recordView();

        // COLO-2 is nullable and not set.
        assertOpOnNode("server-2", "get", x -> recordView.get(null, Tuple.create().set("ID", 0).set("COLO-1", "0")));
        assertOpOnNode("server-2", "get", x -> recordView.get(null, Tuple.create().set("ID", 2).set("COLO-1", "0")));
        assertOpOnNode("server-2", "get", x -> recordView.get(null, Tuple.create().set("ID", 3).set("COLO-1", "0")));
        assertOpOnNode("server-1", "get", x -> recordView.get(null, Tuple.create().set("ID", 3).set("COLO-1", "1")));

        // COLO-2 is set.
        assertOpOnNode("server-2", "get", x -> recordView.get(null, Tuple.create().set("ID", 0).set("COLO-1", "0").set("COLO-2", 1)));
        assertOpOnNode("server-1", "get", x -> recordView.get(null, Tuple.create().set("ID", 0).set("COLO-1", "0").set("COLO-2", 2)));
    }

    @Test
    public void testCompositeKey() {
        RecordView<Tuple> recordView = table(FakeIgniteTables.TABLE_COMPOSITE_KEY).recordView();

        assertOpOnNode("server-1", "get", x -> recordView.get(null, Tuple.create().set("ID1", 0).set("ID2", "0")));
        assertOpOnNode("server-2", "get", x -> recordView.get(null, Tuple.create().set("ID1", 1).set("ID2", "0")));
        assertOpOnNode("server-1", "get", x -> recordView.get(null, Tuple.create().set("ID1", 0).set("ID2", "1")));
        assertOpOnNode("server-1", "get", x -> recordView.get(null, Tuple.create().set("ID1", 1).set("ID2", "1")));
        assertOpOnNode("server-2", "get", x -> recordView.get(null, Tuple.create().set("ID1", 1).set("ID2", "2")));
    }

    @Test
    public void testAllRecordViewOperations() {
        // TODO: Separate ticket?
        assertTrue(false, "TODO");
    }

    @Test
    public void testAllRecordBinaryViewOperations() {
        // TODO: Separate ticket.
        assertTrue(false, "TODO");
    }

    @Test
    public void testAllKeyValueViewOperations() {
        // TODO: Separate ticket.
        assertTrue(false, "TODO");
    }

    @Test
    public void testAllKeyValueBinaryViewOperations() {
        // TODO: Separate ticket.
        assertTrue(false, "TODO");
    }

    private void assertOpOnNode(String expectedNode, String expectedOp, Consumer<Void> op) {
        lastOpServerName = null;
        lastOp = null;

        op.accept(null);

        assertEquals(expectedNode, lastOpServerName);
        assertEquals(expectedOp, lastOp);
    }

    private Table defaultTable() {
        return table(DEFAULT_TABLE);
    }

    private Table table(String name) {
        // Create table on both servers with the same ID.
        var tableId = UUID.randomUUID();

        table1 = createTable(server, tableId, name);
        table2 = createTable(server2, tableId, name);

        return client2.tables().table(name);
    }

    private TableImpl createTable(Ignite ignite, UUID id, String name) {
        FakeIgniteTables tables = (FakeIgniteTables) ignite.tables();
        TableImpl tableImpl = tables.createTable(name, id);

        ((FakeInternalTable)tableImpl.internalTable()).setDataAccessListener((op, data) -> {
            lastOp = op;
            lastOpServerName = ignite.name();
        });

        return tableImpl;
    }

    private void initPartitionAssignment(ArrayList<String> assignments) {
        initPartitionAssignment(server, assignments);
        initPartitionAssignment(server2, assignments);
    }

    private void initPartitionAssignment(Ignite ignite, ArrayList<String> assignments) {
        if (assignments == null) {
            assignments = new ArrayList<>();

            assignments.add(testServer.nodeId());
            assignments.add(testServer2.nodeId());
            assignments.add(testServer.nodeId());
            assignments.add(testServer2.nodeId());
        }

        FakeIgniteTables tables = (FakeIgniteTables) ignite.tables();

        tables.setPartitionAssignments(assignments);
    }
}
