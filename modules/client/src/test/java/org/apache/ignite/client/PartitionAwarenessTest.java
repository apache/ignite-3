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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.util.ResourceLeakDetector;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.AbstractClientTableTest.PersonPojo;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.client.fakes.FakeInternalTable;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;
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
    private static TestServer testServer2;

    private static Ignite server2;

    private static IgniteClient client2;

    private @Nullable String lastOp;

    private @Nullable String lastOpServerName;

    private final AtomicInteger nextTableId = new AtomicInteger(101);

    /**
     * Before all.
     */
    @BeforeAll
    public static void beforeAll() {
        AbstractClientTest.beforeAll();

        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        server2 = new FakeIgnite("server-2");
        testServer2 = new TestServer(0, server2, null, null, "server-2", clusterId, null, serverPort + 1);

        var clientBuilder = IgniteClient.builder()
                .addresses("127.0.0.1:" + serverPort, "127.0.0.1:" + testServer2.port())
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
    @Override
    public void beforeEach() throws InterruptedException {
        super.beforeEach();

        dropTables(server);
        dropTables(server2);

        initPartitionAssignment(null);

        assertTrue(IgniteTestUtils.waitForCondition(() -> client2.connections().size() == 2, 3000));
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
        RecordView<PersonPojo> pojoView = defaultTable().recordView(Mapper.of(PersonPojo.class));

        assertOpOnNode("server-1", "get", x -> pojoView.get(null, new PersonPojo(0L)));
        assertOpOnNode("server-2", "get", x -> pojoView.get(null, new PersonPojo(1L)));
        assertOpOnNode("server-1", "get", x -> pojoView.get(null, new PersonPojo(2L)));
        assertOpOnNode("server-2", "get", x -> pojoView.get(null, new PersonPojo(3L)));
    }

    @Test
    public void testGetKeyValueRoutesRequestToPrimaryNode() {
        KeyValueView<Long, String> kvView = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        assertOpOnNode("server-1", "get", x -> kvView.get(null, 0L));
        assertOpOnNode("server-2", "get", x -> kvView.get(null, 1L));
        assertOpOnNode("server-1", "get", x -> kvView.get(null, 2L));
        assertOpOnNode("server-2", "get", x -> kvView.get(null, 3L));
    }

    @Test
    public void testGetKeyValueBinaryRoutesRequestToPrimaryNode() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();

        assertOpOnNode("server-1", "get", x -> kvView.get(null, Tuple.create().set("ID", 0L)));
        assertOpOnNode("server-2", "get", x -> kvView.get(null, Tuple.create().set("ID", 1L)));
        assertOpOnNode("server-1", "get", x -> kvView.get(null, Tuple.create().set("ID", 2L)));
        assertOpOnNode("server-2", "get", x -> kvView.get(null, Tuple.create().set("ID", 3L)));
    }

    @Test
    public void testNonNullTxDisablesPartitionAwareness() {
        RecordView<Tuple> recordView = defaultTable().recordView();
        var tx = client2.transactions().begin();

        assertOpOnNode("server-1", "get", x -> recordView.get(tx, Tuple.create().set("ID", 0L)));
        assertOpOnNode("server-1", "get", x -> recordView.get(tx, Tuple.create().set("ID", 1L)));
        assertOpOnNode("server-1", "get", x -> recordView.get(tx, Tuple.create().set("ID", 2L)));
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
            // Perform a request on the default channel to receive change notification flag.
            // Use two requests because of round-robin.
            client2.tables().tables();
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
        RecordView<PersonPojo> pojoView = defaultTable().recordView(
                Mapper.of(PersonPojo.class));

        var t1 = new PersonPojo(0L);
        var t2 = new PersonPojo(1L);

        assertOpOnNode("server-1", "insert", x -> pojoView.insert(null, t1));
        assertOpOnNode("server-2", "insert", x -> pojoView.insert(null, t2));

        assertOpOnNode("server-1", "insertAll", x -> pojoView.insertAll(null, List.of(t1)));
        assertOpOnNode("server-2", "insertAll", x -> pojoView.insertAll(null, List.of(t2)));

        assertOpOnNode("server-1", "upsert", x -> pojoView.upsert(null, t1));
        assertOpOnNode("server-2", "upsert", x -> pojoView.upsert(null, t2));

        assertOpOnNode("server-1", "upsertAll", x -> pojoView.upsertAll(null, List.of(t1)));
        assertOpOnNode("server-2", "upsertAll", x -> pojoView.upsertAll(null, List.of(t2)));

        assertOpOnNode("server-1", "get", x -> pojoView.get(null, t1));
        assertOpOnNode("server-2", "get", x -> pojoView.get(null, t2));

        assertOpOnNode("server-1", "getAll", x -> pojoView.getAll(null, List.of(t1)));
        assertOpOnNode("server-2", "getAll", x -> pojoView.getAll(null, List.of(t2)));

        assertOpOnNode("server-1", "getAndUpsert", x -> pojoView.getAndUpsert(null, t1));
        assertOpOnNode("server-2", "getAndUpsert", x -> pojoView.getAndUpsert(null, t2));

        assertOpOnNode("server-1", "getAndReplace", x -> pojoView.getAndReplace(null, t1));
        assertOpOnNode("server-2", "getAndReplace", x -> pojoView.getAndReplace(null, t2));

        assertOpOnNode("server-1", "getAndDelete", x -> pojoView.getAndDelete(null, t1));
        assertOpOnNode("server-2", "getAndDelete", x -> pojoView.getAndDelete(null, t2));

        assertOpOnNode("server-1", "replace", x -> pojoView.replace(null, t1));
        assertOpOnNode("server-2", "replace", x -> pojoView.replace(null, t2));

        assertOpOnNode("server-1", "replace", x -> pojoView.replace(null, t1, t1));
        assertOpOnNode("server-2", "replace", x -> pojoView.replace(null, t2, t2));

        assertOpOnNode("server-1", "delete", x -> pojoView.delete(null, t1));
        assertOpOnNode("server-2", "delete", x -> pojoView.delete(null, t2));

        assertOpOnNode("server-1", "deleteExact", x -> pojoView.deleteExact(null, t1));
        assertOpOnNode("server-2", "deleteExact", x -> pojoView.deleteExact(null, t2));

        assertOpOnNode("server-1", "deleteAll", x -> pojoView.deleteAll(null, List.of(t1)));
        assertOpOnNode("server-2", "deleteAll", x -> pojoView.deleteAll(null, List.of(t2)));

        assertOpOnNode("server-1", "deleteAllExact", x -> pojoView.deleteAllExact(null, List.of(t1)));
        assertOpOnNode("server-2", "deleteAllExact", x -> pojoView.deleteAllExact(null, List.of(t2)));
    }

    @Test
    public void testAllRecordBinaryViewOperations() {
        RecordView<Tuple> recordView = defaultTable().recordView();

        Tuple t1 = Tuple.create().set("ID", 0L);
        Tuple t2 = Tuple.create().set("ID", 1L);

        assertOpOnNode("server-1", "insert", x -> recordView.insert(null, t1));
        assertOpOnNode("server-2", "insert", x -> recordView.insert(null, t2));

        assertOpOnNode("server-1", "insertAll", x -> recordView.insertAll(null, List.of(t1)));
        assertOpOnNode("server-2", "insertAll", x -> recordView.insertAll(null, List.of(t2)));

        assertOpOnNode("server-1", "upsert", x -> recordView.upsert(null, t1));
        assertOpOnNode("server-2", "upsert", x -> recordView.upsert(null, t2));

        assertOpOnNode("server-1", "upsertAll", x -> recordView.upsertAll(null, List.of(t1)));
        assertOpOnNode("server-2", "upsertAll", x -> recordView.upsertAll(null, List.of(t2)));

        assertOpOnNode("server-1", "get", x -> recordView.get(null, t1));
        assertOpOnNode("server-2", "get", x -> recordView.get(null, t2));

        assertOpOnNode("server-1", "getAll", x -> recordView.getAll(null, List.of(t1)));
        assertOpOnNode("server-2", "getAll", x -> recordView.getAll(null, List.of(t2)));

        assertOpOnNode("server-1", "getAndUpsert", x -> recordView.getAndUpsert(null, t1));
        assertOpOnNode("server-2", "getAndUpsert", x -> recordView.getAndUpsert(null, t2));

        assertOpOnNode("server-1", "getAndReplace", x -> recordView.getAndReplace(null, t1));
        assertOpOnNode("server-2", "getAndReplace", x -> recordView.getAndReplace(null, t2));

        assertOpOnNode("server-1", "getAndDelete", x -> recordView.getAndDelete(null, t1));
        assertOpOnNode("server-2", "getAndDelete", x -> recordView.getAndDelete(null, t2));

        assertOpOnNode("server-1", "replace", x -> recordView.replace(null, t1));
        assertOpOnNode("server-2", "replace", x -> recordView.replace(null, t2));

        assertOpOnNode("server-1", "replace", x -> recordView.replace(null, t1, t1));
        assertOpOnNode("server-2", "replace", x -> recordView.replace(null, t2, t2));

        assertOpOnNode("server-1", "delete", x -> recordView.delete(null, t1));
        assertOpOnNode("server-2", "delete", x -> recordView.delete(null, t2));

        assertOpOnNode("server-1", "deleteExact", x -> recordView.deleteExact(null, t1));
        assertOpOnNode("server-2", "deleteExact", x -> recordView.deleteExact(null, t2));

        assertOpOnNode("server-1", "deleteAll", x -> recordView.deleteAll(null, List.of(t1)));
        assertOpOnNode("server-2", "deleteAll", x -> recordView.deleteAll(null, List.of(t2)));

        assertOpOnNode("server-1", "deleteAllExact", x -> recordView.deleteAllExact(null, List.of(t1)));
        assertOpOnNode("server-2", "deleteAllExact", x -> recordView.deleteAllExact(null, List.of(t2)));
    }

    @Test
    public void testAllKeyValueViewOperations() {
        KeyValueView<Long, String> kvView = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        var k1 = 0L;
        var k2 = 1L;
        var v = "v";

        assertOpOnNode("server-1", "insert", x -> kvView.putIfAbsent(null, k1, v));
        assertOpOnNode("server-2", "insert", x -> kvView.putIfAbsent(null, k2, v));

        assertOpOnNode("server-1", "upsert", x -> kvView.put(null, k1, v));
        assertOpOnNode("server-2", "upsert", x -> kvView.put(null, k2, v));

        assertOpOnNode("server-1", "upsertAll", x -> kvView.putAll(null, Map.of(k1, v)));
        assertOpOnNode("server-2", "upsertAll", x -> kvView.putAll(null, Map.of(k2, v)));

        assertOpOnNode("server-1", "get", x -> kvView.get(null, k1));
        assertOpOnNode("server-2", "get", x -> kvView.get(null, k2));

        assertOpOnNode("server-1", "get", x -> kvView.contains(null, k1));
        assertOpOnNode("server-2", "get", x -> kvView.contains(null, k2));

        assertOpOnNode("server-1", "getAll", x -> kvView.getAll(null, List.of(k1)));
        assertOpOnNode("server-2", "getAll", x -> kvView.getAll(null, List.of(k2)));

        assertOpOnNode("server-1", "getAndUpsert", x -> kvView.getAndPut(null, k1, v));
        assertOpOnNode("server-2", "getAndUpsert", x -> kvView.getAndPut(null, k2, v));

        assertOpOnNode("server-1", "getAndReplace", x -> kvView.getAndReplace(null, k1, v));
        assertOpOnNode("server-2", "getAndReplace", x -> kvView.getAndReplace(null, k2, v));

        assertOpOnNode("server-1", "getAndDelete", x -> kvView.getAndRemove(null, k1));
        assertOpOnNode("server-2", "getAndDelete", x -> kvView.getAndRemove(null, k2));

        assertOpOnNode("server-1", "replace", x -> kvView.replace(null, k1, v));
        assertOpOnNode("server-2", "replace", x -> kvView.replace(null, k2, v));

        assertOpOnNode("server-1", "replace", x -> kvView.replace(null, k1, v, v));
        assertOpOnNode("server-2", "replace", x -> kvView.replace(null, k2, v, v));

        assertOpOnNode("server-1", "delete", x -> kvView.remove(null, k1));
        assertOpOnNode("server-2", "delete", x -> kvView.remove(null, k2));

        assertOpOnNode("server-1", "deleteExact", x -> kvView.remove(null, k1, v));
        assertOpOnNode("server-2", "deleteExact", x -> kvView.remove(null, k2, v));

        assertOpOnNode("server-1", "deleteAll", x -> kvView.removeAll(null, List.of(k1)));
        assertOpOnNode("server-2", "deleteAll", x -> kvView.removeAll(null, List.of(k2)));
    }

    @Test
    public void testAllKeyValueBinaryViewOperations() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();

        Tuple t1 = Tuple.create().set("ID", 0L);
        Tuple t2 = Tuple.create().set("ID", 1L);

        assertOpOnNode("server-1", "insert", x -> kvView.putIfAbsent(null, t1, t1));
        assertOpOnNode("server-2", "insert", x -> kvView.putIfAbsent(null, t2, t2));

        assertOpOnNode("server-1", "upsert", x -> kvView.put(null, t1, t1));
        assertOpOnNode("server-2", "upsert", x -> kvView.put(null, t2, t2));

        assertOpOnNode("server-1", "upsertAll", x -> kvView.putAll(null, Map.of(t1, t1)));
        assertOpOnNode("server-2", "upsertAll", x -> kvView.putAll(null, Map.of(t2, t2)));

        assertOpOnNode("server-1", "get", x -> kvView.get(null, t1));
        assertOpOnNode("server-2", "get", x -> kvView.get(null, t2));

        assertOpOnNode("server-1", "get", x -> kvView.contains(null, t1));
        assertOpOnNode("server-2", "get", x -> kvView.contains(null, t2));

        assertOpOnNode("server-1", "getAll", x -> kvView.getAll(null, List.of(t1)));
        assertOpOnNode("server-2", "getAll", x -> kvView.getAll(null, List.of(t2)));

        assertOpOnNode("server-1", "getAndUpsert", x -> kvView.getAndPut(null, t1, t1));
        assertOpOnNode("server-2", "getAndUpsert", x -> kvView.getAndPut(null, t2, t2));

        assertOpOnNode("server-1", "getAndReplace", x -> kvView.getAndReplace(null, t1, t1));
        assertOpOnNode("server-2", "getAndReplace", x -> kvView.getAndReplace(null, t2, t2));

        assertOpOnNode("server-1", "getAndDelete", x -> kvView.getAndRemove(null, t1));
        assertOpOnNode("server-2", "getAndDelete", x -> kvView.getAndRemove(null, t2));

        assertOpOnNode("server-1", "replace", x -> kvView.replace(null, t1, t1));
        assertOpOnNode("server-2", "replace", x -> kvView.replace(null, t2, t2));

        assertOpOnNode("server-1", "replace", x -> kvView.replace(null, t1, t1, t1));
        assertOpOnNode("server-2", "replace", x -> kvView.replace(null, t2, t2, t2));

        assertOpOnNode("server-1", "delete", x -> kvView.remove(null, t1));
        assertOpOnNode("server-2", "delete", x -> kvView.remove(null, t2));

        assertOpOnNode("server-1", "deleteExact", x -> kvView.remove(null, t1, t1));
        assertOpOnNode("server-2", "deleteExact", x -> kvView.remove(null, t2, t2));

        assertOpOnNode("server-1", "deleteAll", x -> kvView.removeAll(null, List.of(t1)));
        assertOpOnNode("server-2", "deleteAll", x -> kvView.removeAll(null, List.of(t2)));
    }

    @Test
    public void testExecuteColocatedTupleKeyRoutesRequestToPrimaryNode() {
        Table table = defaultTable();

        Tuple t1 = Tuple.create().set("ID", 0L);
        Tuple t2 = Tuple.create().set("ID", 1L);

        assertEquals("server-1", compute().executeColocated(table.name(), t1, List.of(), "job").join());
        assertEquals("server-2", compute().executeColocated(table.name(), t2, List.of(), "job").join());
    }

    @Test
    public void testExecuteColocatedObjectKeyRoutesRequestToPrimaryNode() {
        var mapper = Mapper.of(Long.class);
        Table table = defaultTable();

        assertEquals("server-1", compute().executeColocated(table.name(), 0L, mapper, List.of(), "job").join());
        assertEquals("server-2", compute().executeColocated(table.name(), 1L, mapper, List.of(), "job").join());
    }

    @Test
    public void testDataStreamerRecordBinaryView() {
        RecordView<Tuple> recordView = defaultTable().recordView();

        Consumer<Tuple> stream = t -> {
            SubmissionPublisher<Tuple> publisher = new SubmissionPublisher<>();
            var fut = recordView.streamData(publisher, null);
            publisher.submit(t);
            publisher.close();
            fut.join();
        };

        assertOpOnNode("server-1", "upsertAll", x -> stream.accept(Tuple.create().set("ID", 0L)));
        assertOpOnNode("server-2", "upsertAll", x -> stream.accept(Tuple.create().set("ID", 1L)));
        assertOpOnNode("server-1", "upsertAll", x -> stream.accept(Tuple.create().set("ID", 2L)));
        assertOpOnNode("server-2", "upsertAll", x -> stream.accept(Tuple.create().set("ID", 3L)));
    }

    @Test
    public void testDataStreamerRecordView() {
        RecordView<PersonPojo> pojoView = defaultTable().recordView(Mapper.of(PersonPojo.class));

        Consumer<PersonPojo> stream = t -> {
            SubmissionPublisher<PersonPojo> publisher = new SubmissionPublisher<>();
            var fut = pojoView.streamData(publisher, null);
            publisher.submit(t);
            publisher.close();
            fut.join();
        };

        assertOpOnNode("server-1", "upsertAll", x -> stream.accept(new PersonPojo(0L)));
        assertOpOnNode("server-2", "upsertAll", x -> stream.accept(new PersonPojo(1L)));
        assertOpOnNode("server-1", "upsertAll", x -> stream.accept(new PersonPojo(2L)));
        assertOpOnNode("server-2", "upsertAll", x -> stream.accept(new PersonPojo(3L)));
    }

    @Test
    public void testDataStreamerKeyValueBinaryView() {
        KeyValueView<Tuple, Tuple> recordView = defaultTable().keyValueView();

        Consumer<Tuple> stream = t -> {
            SubmissionPublisher<Entry<Tuple, Tuple>> publisher = new SubmissionPublisher<>();
            var fut = recordView.streamData(publisher, null);
            publisher.submit(Map.entry(t, t));
            publisher.close();
            fut.join();
        };

        assertOpOnNode("server-1", "upsertAll", x -> stream.accept(Tuple.create().set("ID", 0L)));
        assertOpOnNode("server-2", "upsertAll", x -> stream.accept(Tuple.create().set("ID", 1L)));
        assertOpOnNode("server-1", "upsertAll", x -> stream.accept(Tuple.create().set("ID", 2L)));
        assertOpOnNode("server-2", "upsertAll", x -> stream.accept(Tuple.create().set("ID", 3L)));
    }

    @Test
    public void testDataStreamerKeyValueView() {
        KeyValueView<Long, String> kvView = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        Consumer<Long> stream = t -> {
            SubmissionPublisher<Entry<Long, String>> publisher = new SubmissionPublisher<>();
            var fut = kvView.streamData(publisher, null);
            publisher.submit(Map.entry(t, t.toString()));
            publisher.close();
            fut.join();
        };

        assertOpOnNode("server-1", "upsertAll", x -> stream.accept(0L));
        assertOpOnNode("server-2", "upsertAll", x -> stream.accept(1L));
        assertOpOnNode("server-1", "upsertAll", x -> stream.accept(2L));
        assertOpOnNode("server-2", "upsertAll", x -> stream.accept(3L));
    }

    @Test
    public void testDataStreamerReceivesPartitionAssignmentUpdates() throws InterruptedException {
        DataStreamerOptions options = DataStreamerOptions.builder()
                .batchSize(1)
                .perNodeParallelOperations(1)
                .autoFlushFrequency(50)
                .build();

        RecordView<Tuple> recordView = defaultTable().recordView();
        SubmissionPublisher<Tuple> publisher = new SubmissionPublisher<>();
        var fut = recordView.streamData(publisher, options);

        Consumer<Long> submit = id -> {
            try {
                publisher.submit(Tuple.create().set("ID", id));
                assertTrue(IgniteTestUtils.waitForCondition(() -> lastOpServerName != null, 1000));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };

        assertOpOnNode("server-1", "upsertAll", x -> submit.accept(0L));
        assertOpOnNode("server-2", "upsertAll", x -> submit.accept(1L));

        // Update partition assignment.
        var assignments = new ArrayList<String>();

        assignments.add(testServer2.nodeId());
        assignments.add(testServer.nodeId());

        initPartitionAssignment(assignments);

        // Send some batches so that the client receives updated assignment.
        lastOpServerName = null;
        submit.accept(0L);
        assertTrue(IgniteTestUtils.waitForCondition(() -> lastOpServerName != null, 1000));

        lastOpServerName = null;
        submit.accept(1L);
        assertTrue(IgniteTestUtils.waitForCondition(() -> lastOpServerName != null, 1000));

        // Check updated assignment.
        assertOpOnNode("server-2", "upsertAll", x -> submit.accept(0L));
        assertOpOnNode("server-1", "upsertAll", x -> submit.accept(1L));

        publisher.close();
        fut.join();
    }

    private void assertOpOnNode(String expectedNode, String expectedOp, Consumer<Void> op) {
        lastOpServerName = null;
        lastOp = null;

        op.accept(null);

        assertEquals(expectedOp, lastOp);
        assertEquals(expectedNode, lastOpServerName, "Operation " + expectedOp + " was not executed on expected node");
    }

    private Table defaultTable() {
        return table(DEFAULT_TABLE);
    }

    private Table table(String name) {
        // Create table on both servers with the same ID.
        int tableId = nextTableId.getAndIncrement();

        createTable(server, tableId, name);
        createTable(server2, tableId, name);

        return client2.tables().table(name);
    }

    private static IgniteCompute compute() {
        return client2.compute();
    }

    private void createTable(Ignite ignite, int id, String name) {
        FakeIgniteTables tables = (FakeIgniteTables) ignite.tables();
        TableImpl tableImpl = tables.createTable(name, id);

        ((FakeInternalTable) tableImpl.internalTable()).setDataAccessListener((op, data) -> {
            lastOp = op;
            lastOpServerName = ignite.name();
        });
    }

    private static void initPartitionAssignment(@Nullable ArrayList<String> assignments) {
        initPartitionAssignment(server, assignments);
        initPartitionAssignment(server2, assignments);
    }

    private static void initPartitionAssignment(Ignite ignite, @Nullable ArrayList<String> assignments) {
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
