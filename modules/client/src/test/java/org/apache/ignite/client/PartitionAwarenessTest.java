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
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests partition awareness.
 */
public class PartitionAwarenessTest extends AbstractClientTest {
    protected static TestServer testServer2;

    protected static Ignite server2;

    protected static IgniteClient client2;

    protected static int serverPort2;

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

        var clientBuilder = IgniteClient.builder()
                .addresses("127.0.0.1:" + serverPort, "127.0.0.1:" + serverPort2)
                .heartbeatInterval(200);

        client2 = clientBuilder.build();
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
    public void testGetTupleRoutesRequestToPrimaryNode() {
        RecordView<Tuple> recordView = defaultTable().recordView();

        assertOpOnNode("server-1", "get", x -> recordView.get(null, Tuple.create().set("ID", 0L)));
        assertOpOnNode("server-2", "get", x -> recordView.get(null, Tuple.create().set("ID", 1L)));
        assertOpOnNode("server-1", "get", x -> recordView.get(null, Tuple.create().set("ID", 2L)));
        assertOpOnNode("server-2", "get", x -> recordView.get(null, Tuple.create().set("ID", 3L)));
    }

    @Test
    public void testGetRecordRoutesRequestToPrimaryNode() {
        // TODO: Refactor everything to be based on BinaryTuple? That way affinity calc is the same.
        // But perf might suffer - we'll have to deserialize column values back to compute affinity.
        // On the other hand, it might be faster to deserialize than to retrieve by name from tuple or object.
        // The fastest way would be to combine serialization and affinity calculation - by wrapping BinaryTupleBuilder in another class (or derived)
        RecordView<AbstractClientTableTest.PersonPojo> recordView = defaultTable().recordView(Mapper.of(AbstractClientTableTest.PersonPojo.class));

        assertOpOnNode("server-1", "get", x -> recordView.get(null, new AbstractClientTableTest.PersonPojo(0L)));
        assertOpOnNode("server-2", "get", x -> recordView.get(null, new AbstractClientTableTest.PersonPojo(1L)));
        assertOpOnNode("server-1", "get", x -> recordView.get(null, new AbstractClientTableTest.PersonPojo(2L)));
        assertOpOnNode("server-2", "get", x -> recordView.get(null, new AbstractClientTableTest.PersonPojo(3L)));
    }

    @Test
    public void testNonNullTxDisablesPartitionAwareness() {
        RecordView<Tuple> recordView = defaultTable().recordView();
        var tx = client2.transactions().begin();

        assertOpOnNode("server-2", "get", x -> recordView.get(tx, Tuple.create().set("ID", 0L)));
        assertOpOnNode("server-2", "get", x -> recordView.get(tx, Tuple.create().set("ID", 1L)));
        assertOpOnNode("server-2", "get", x -> recordView.get(tx, Tuple.create().set("ID", 2L)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testClientReceivesPartitionAssignmentUpdates(boolean useHeartbeat) throws InterruptedException {
        // Check default assignment.
        RecordView<Tuple> recordView = defaultTable().recordView();

        assertOpOnNode("server-1", "get", x -> recordView.get(null, Tuple.create().set("ID", 0L)));
        assertOpOnNode("server-2", "get", x -> recordView.get(null, Tuple.create().set("ID", 1L)));

        // Update partition assignment.
        var assignments = new ArrayList<String>();

        assignments.add(testServer2.nodeId());
        assignments.add(testServer.nodeId());

        initPartitionAssignment(assignments);

        if (useHeartbeat) {
            // Wait for heartbeat message to receive change notification flag.
            Thread.sleep(500);
        } else {
            // Perform one request on the default channel to receive change notification flag.
            client2.tables().tables();
        }

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
        // TODO IGNITE-17739 Add Partition Awareness to all table APIs
    }

    @Test
    public void testAllRecordBinaryViewOperations() {
        // TODO IGNITE-17739 Add Partition Awareness to all table APIs
    }

    @Test
    public void testAllKeyValueViewOperations() {
        // TODO IGNITE-17739 Add Partition Awareness to all table APIs
    }

    @Test
    public void testAllKeyValueBinaryViewOperations() {
        // TODO IGNITE-17739 Add Partition Awareness to all table APIs
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

        createTable(server, tableId, name);
        createTable(server2, tableId, name);

        return client2.tables().table(name);
    }

    private void createTable(Ignite ignite, UUID id, String name) {
        FakeIgniteTables tables = (FakeIgniteTables) ignite.tables();
        TableImpl tableImpl = tables.createTable(name, id);

        ((FakeInternalTable) tableImpl.internalTable()).setDataAccessListener((op, data) -> {
            lastOp = op;
            lastOpServerName = ignite.name();
        });
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
