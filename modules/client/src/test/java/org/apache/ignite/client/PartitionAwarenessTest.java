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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.util.ResourceLeakDetector;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.AbstractClientTableTest.PersonPojo;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.client.fakes.FakeInternalTable;
import org.apache.ignite.client.handler.FakePlacementDriver;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.tx.ClientLazyTransaction;
import org.apache.ignite.internal.client.tx.ClientTransaction;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.streamer.SimplePublisher;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.table.DataStreamerItem;
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
    private static final String nodeKey0 = "server-2";

    private static final String nodeKey1 = "server-2";

    private static final String nodeKey2 = "server-1";

    private static final String nodeKey3 = "server-2";

    private static TestServer testServer2;

    private static Ignite server2;

    private static IgniteClient client2;

    private volatile @Nullable String lastOp;

    private volatile @Nullable String lastOpServerName;

    private static final AtomicInteger nextTableId = new AtomicInteger(101);

    /**
     * Before all.
     */
    @BeforeAll
    public static void startServer2() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        server2 = new FakeIgnite("server-2");
        testServer2 = new TestServer(0, server2, null, null, "server-2", clusterId, null, null);

        var clientBuilder = IgniteClient.builder()
                .addresses("127.0.0.1:" + serverPort, "127.0.0.1:" + testServer2.port())
                .heartbeatInterval(200);

        client2 = clientBuilder.build();
    }

    /**
     * After all.
     */
    @AfterAll
    public static void stopServer2() throws Exception {
        IgniteUtils.closeAll(client2, testServer2);
    }

    @BeforeEach
    public void initReplicas() throws InterruptedException {
        dropTables(server2);

        initPrimaryReplicas(null);

        assertTrue(IgniteTestUtils.waitForCondition(() -> client2.connections().size() == 2, 3000));
    }

    @Test
    public void testGetTupleRoutesRequestToPrimaryNode() {
        RecordView<Tuple> recordView = defaultTable().recordView();

        assertOpOnNode(nodeKey0, "get", x -> recordView.get(null, Tuple.create().set("ID", 0L)));
        assertOpOnNode(nodeKey1, "get", x -> recordView.get(null, Tuple.create().set("ID", 1L)));
        assertOpOnNode(nodeKey2, "get", x -> recordView.get(null, Tuple.create().set("ID", 2L)));
        assertOpOnNode(nodeKey3, "get", x -> recordView.get(null, Tuple.create().set("ID", 3L)));
    }

    @Test
    public void testGetRecordRoutesRequestToPrimaryNode() {
        RecordView<PersonPojo> pojoView = defaultTable().recordView(Mapper.of(PersonPojo.class));

        assertOpOnNode(nodeKey0, "get", x -> pojoView.get(null, new PersonPojo(0L)));
        assertOpOnNode(nodeKey1, "get", x -> pojoView.get(null, new PersonPojo(1L)));
        assertOpOnNode(nodeKey2, "get", x -> pojoView.get(null, new PersonPojo(2L)));
        assertOpOnNode(nodeKey3, "get", x -> pojoView.get(null, new PersonPojo(3L)));
    }

    @Test
    public void testGetKeyValueRoutesRequestToPrimaryNode() {
        KeyValueView<Long, String> kvView = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        assertOpOnNode(nodeKey0, "get", x -> kvView.get(null, 0L));
        assertOpOnNode(nodeKey1, "get", x -> kvView.get(null, 1L));
        assertOpOnNode(nodeKey2, "get", x -> kvView.get(null, 2L));
        assertOpOnNode(nodeKey3, "get", x -> kvView.get(null, 3L));
    }

    @Test
    public void testGetKeyValueBinaryRoutesRequestToPrimaryNode() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();

        assertOpOnNode(nodeKey0, "get", x -> kvView.get(null, Tuple.create().set("ID", 0L)));
        assertOpOnNode(nodeKey1, "get", x -> kvView.get(null, Tuple.create().set("ID", 1L)));
        assertOpOnNode(nodeKey2, "get", x -> kvView.get(null, Tuple.create().set("ID", 2L)));
        assertOpOnNode(nodeKey3, "get", x -> kvView.get(null, Tuple.create().set("ID", 3L)));
    }

    @Test
    public void testNonNullTxDisablesPartitionAwareness() {
        RecordView<Tuple> recordView = defaultTable().recordView();
        var tx = (ClientLazyTransaction) client2.transactions().begin();
        client2.sql().execute(tx, "SELECT 1").close(); // Force lazy tx init.

        String expectedNode = tx.nodeName();
        assertNotNull(expectedNode);

        assertOpOnNode(expectedNode, "get", x -> recordView.get(tx, Tuple.create().set("ID", 0L)));
        assertOpOnNode(expectedNode, "get", x -> recordView.get(tx, Tuple.create().set("ID", 1L)));
        assertOpOnNode(expectedNode, "get", x -> recordView.get(tx, Tuple.create().set("ID", 2L)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testClientReceivesPartitionAssignmentUpdates(boolean useHeartbeat) throws InterruptedException {
        ReliableChannel ch = IgniteTestUtils.getFieldValue(client2, "ch");

        // Check default assignment.
        RecordView<Tuple> recordView = defaultTable().recordView();

        assertOpOnNode(nodeKey1, "get", x -> recordView.get(null, Tuple.create().set("ID", 1L)));
        assertOpOnNode(nodeKey2, "get", x -> recordView.get(null, Tuple.create().set("ID", 2L)));

        // Update partition assignment.
        var oldTs = ch.partitionAssignmentTimestamp();
        initPrimaryReplicas(reversedReplicas());

        if (useHeartbeat) {
            // Wait for heartbeat message to receive change notification flag.
            assertTrue(IgniteTestUtils.waitForCondition(() -> ch.partitionAssignmentTimestamp() > oldTs, 3000));
        } else {
            // Perform requests to receive change notification flag.
            int maxRequests = 50;
            while (ch.partitionAssignmentTimestamp() <= oldTs && maxRequests-- > 0) {
                client2.tables().tables();
            }

            assertThat("Failed to receive assignment update", maxRequests, greaterThan(0));
        }

        // Check new assignment.
        assertThat(ch.partitionAssignmentTimestamp(), greaterThan(oldTs));

        assertOpOnNode(nodeKey2, "get", x -> recordView.get(null, Tuple.create().set("ID", 1L)));
        assertOpOnNode(nodeKey1, "get", x -> recordView.get(null, Tuple.create().set("ID", 2L)));
    }

    @Test
    public void testCustomColocationKey() {
        RecordView<Tuple> recordView = table(FakeIgniteTables.TABLE_COLOCATION_KEY).recordView();

        assertOpOnNode("server-2", "get", x -> recordView.get(null, Tuple.create().set("ID", 0).set("COLO-1", "0").set("COLO-2", 4L)));
        assertOpOnNode("server-1", "get", x -> recordView.get(null, Tuple.create().set("ID", 0).set("COLO-1", "0").set("COLO-2", 8L)));
    }

    @Test
    public void testCompositeKey() {
        RecordView<Tuple> recordView = table(FakeIgniteTables.TABLE_COMPOSITE_KEY).recordView();

        assertOpOnNode("server-2", "get", x -> recordView.get(null, Tuple.create().set("ID1", 0).set("ID2", "0")));
        assertOpOnNode("server-1", "get", x -> recordView.get(null, Tuple.create().set("ID1", 1).set("ID2", "0")));
        assertOpOnNode("server-2", "get", x -> recordView.get(null, Tuple.create().set("ID1", 0).set("ID2", "1")));
        assertOpOnNode("server-1", "get", x -> recordView.get(null, Tuple.create().set("ID1", 1).set("ID2", "1")));
        assertOpOnNode("server-2", "get", x -> recordView.get(null, Tuple.create().set("ID1", 1).set("ID2", "2")));
    }

    @Test
    public void testAllRecordViewOperations() {
        RecordView<PersonPojo> pojoView = defaultTable().recordView(
                Mapper.of(PersonPojo.class));

        var t1 = new PersonPojo(0L);
        var t2 = new PersonPojo(1L);

        assertOpOnNode(nodeKey0, "insert", x -> pojoView.insert(null, t1));
        assertOpOnNode(nodeKey1, "insert", x -> pojoView.insert(null, t2));

        assertOpOnNode(nodeKey0, "insertAll", x -> pojoView.insertAll(null, List.of(t1)));
        assertOpOnNode(nodeKey1, "insertAll", x -> pojoView.insertAll(null, List.of(t2)));

        assertOpOnNode(nodeKey0, "upsert", x -> pojoView.upsert(null, t1));
        assertOpOnNode(nodeKey1, "upsert", x -> pojoView.upsert(null, t2));

        assertOpOnNode(nodeKey0, "upsertAll", x -> pojoView.upsertAll(null, List.of(t1)));
        assertOpOnNode(nodeKey1, "upsertAll", x -> pojoView.upsertAll(null, List.of(t2)));

        assertOpOnNode(nodeKey0, "get", x -> pojoView.get(null, t1));
        assertOpOnNode(nodeKey1, "get", x -> pojoView.get(null, t2));

        assertOpOnNode(nodeKey0, "getAll", x -> pojoView.getAll(null, List.of(t1)));
        assertOpOnNode(nodeKey1, "getAll", x -> pojoView.getAll(null, List.of(t2)));

        assertOpOnNode(nodeKey0, "getAndUpsert", x -> pojoView.getAndUpsert(null, t1));
        assertOpOnNode(nodeKey1, "getAndUpsert", x -> pojoView.getAndUpsert(null, t2));

        assertOpOnNode(nodeKey0, "getAndReplace", x -> pojoView.getAndReplace(null, t1));
        assertOpOnNode(nodeKey1, "getAndReplace", x -> pojoView.getAndReplace(null, t2));

        assertOpOnNode(nodeKey0, "getAndDelete", x -> pojoView.getAndDelete(null, t1));
        assertOpOnNode(nodeKey1, "getAndDelete", x -> pojoView.getAndDelete(null, t2));

        assertOpOnNode(nodeKey0, "replace", x -> pojoView.replace(null, t1));
        assertOpOnNode(nodeKey1, "replace", x -> pojoView.replace(null, t2));

        assertOpOnNode(nodeKey0, "replace", x -> pojoView.replace(null, t1, t1));
        assertOpOnNode(nodeKey1, "replace", x -> pojoView.replace(null, t2, t2));

        assertOpOnNode(nodeKey0, "delete", x -> pojoView.delete(null, t1));
        assertOpOnNode(nodeKey1, "delete", x -> pojoView.delete(null, t2));

        assertOpOnNode(nodeKey0, "deleteExact", x -> pojoView.deleteExact(null, t1));
        assertOpOnNode(nodeKey1, "deleteExact", x -> pojoView.deleteExact(null, t2));

        assertOpOnNode(nodeKey0, "deleteAll", x -> pojoView.deleteAll(null, List.of(t1)));
        assertOpOnNode(nodeKey1, "deleteAll", x -> pojoView.deleteAll(null, List.of(t2)));

        assertOpOnNode(nodeKey0, "deleteAllExact", x -> pojoView.deleteAllExact(null, List.of(t1)));
        assertOpOnNode(nodeKey1, "deleteAllExact", x -> pojoView.deleteAllExact(null, List.of(t2)));
    }

    @Test
    public void testAllRecordBinaryViewOperations() {
        RecordView<Tuple> recordView = defaultTable().recordView();

        Tuple t1 = Tuple.create().set("ID", 1L);
        Tuple t2 = Tuple.create().set("ID", 2L);

        assertOpOnNode(nodeKey1, "insert", x -> recordView.insert(null, t1));
        assertOpOnNode(nodeKey2, "insert", x -> recordView.insert(null, t2));

        assertOpOnNode(nodeKey1, "insertAll", x -> recordView.insertAll(null, List.of(t1)));
        assertOpOnNode(nodeKey2, "insertAll", x -> recordView.insertAll(null, List.of(t2)));

        assertOpOnNode(nodeKey1, "upsert", x -> recordView.upsert(null, t1));
        assertOpOnNode(nodeKey2, "upsert", x -> recordView.upsert(null, t2));

        assertOpOnNode(nodeKey1, "upsertAll", x -> recordView.upsertAll(null, List.of(t1)));
        assertOpOnNode(nodeKey2, "upsertAll", x -> recordView.upsertAll(null, List.of(t2)));

        assertOpOnNode(nodeKey1, "get", x -> recordView.get(null, t1));
        assertOpOnNode(nodeKey2, "get", x -> recordView.get(null, t2));

        assertOpOnNode(nodeKey1, "getAll", x -> recordView.getAll(null, List.of(t1)));
        assertOpOnNode(nodeKey2, "getAll", x -> recordView.getAll(null, List.of(t2)));

        assertOpOnNode(nodeKey1, "getAndUpsert", x -> recordView.getAndUpsert(null, t1));
        assertOpOnNode(nodeKey2, "getAndUpsert", x -> recordView.getAndUpsert(null, t2));

        assertOpOnNode(nodeKey1, "getAndReplace", x -> recordView.getAndReplace(null, t1));
        assertOpOnNode(nodeKey2, "getAndReplace", x -> recordView.getAndReplace(null, t2));

        assertOpOnNode(nodeKey1, "getAndDelete", x -> recordView.getAndDelete(null, t1));
        assertOpOnNode(nodeKey2, "getAndDelete", x -> recordView.getAndDelete(null, t2));

        assertOpOnNode(nodeKey1, "replace", x -> recordView.replace(null, t1));
        assertOpOnNode(nodeKey2, "replace", x -> recordView.replace(null, t2));

        assertOpOnNode(nodeKey1, "replace", x -> recordView.replace(null, t1, t1));
        assertOpOnNode(nodeKey2, "replace", x -> recordView.replace(null, t2, t2));

        assertOpOnNode(nodeKey1, "delete", x -> recordView.delete(null, t1));
        assertOpOnNode(nodeKey2, "delete", x -> recordView.delete(null, t2));

        assertOpOnNode(nodeKey1, "deleteExact", x -> recordView.deleteExact(null, t1));
        assertOpOnNode(nodeKey2, "deleteExact", x -> recordView.deleteExact(null, t2));

        assertOpOnNode(nodeKey1, "deleteAll", x -> recordView.deleteAll(null, List.of(t1)));
        assertOpOnNode(nodeKey2, "deleteAll", x -> recordView.deleteAll(null, List.of(t2)));

        assertOpOnNode(nodeKey1, "deleteAllExact", x -> recordView.deleteAllExact(null, List.of(t1)));
        assertOpOnNode(nodeKey2, "deleteAllExact", x -> recordView.deleteAllExact(null, List.of(t2)));
    }

    @Test
    public void testAllKeyValueViewOperations() {
        KeyValueView<Long, String> kvView = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        var k1 = 1L;
        var k2 = 2L;
        var v = "v";

        assertOpOnNode(nodeKey1, "insert", x -> kvView.putIfAbsent(null, k1, v));
        assertOpOnNode(nodeKey2, "insert", x -> kvView.putIfAbsent(null, k2, v));

        assertOpOnNode(nodeKey1, "upsert", x -> kvView.put(null, k1, v));
        assertOpOnNode(nodeKey2, "upsert", x -> kvView.put(null, k2, v));

        assertOpOnNode(nodeKey1, "upsertAll", x -> kvView.putAll(null, Map.of(k1, v)));
        assertOpOnNode(nodeKey2, "upsertAll", x -> kvView.putAll(null, Map.of(k2, v)));

        assertOpOnNode(nodeKey1, "get", x -> kvView.get(null, k1));
        assertOpOnNode(nodeKey2, "get", x -> kvView.get(null, k2));

        assertOpOnNode(nodeKey1, "get", x -> kvView.contains(null, k1));
        assertOpOnNode(nodeKey2, "get", x -> kvView.contains(null, k2));

        assertOpOnNode(nodeKey1, "getAll", x -> kvView.getAll(null, List.of(k1)));
        assertOpOnNode(nodeKey2, "getAll", x -> kvView.getAll(null, List.of(k2)));

        assertOpOnNode(nodeKey1, "getAndUpsert", x -> kvView.getAndPut(null, k1, v));
        assertOpOnNode(nodeKey2, "getAndUpsert", x -> kvView.getAndPut(null, k2, v));

        assertOpOnNode(nodeKey1, "getAndReplace", x -> kvView.getAndReplace(null, k1, v));
        assertOpOnNode(nodeKey2, "getAndReplace", x -> kvView.getAndReplace(null, k2, v));

        assertOpOnNode(nodeKey1, "getAndDelete", x -> kvView.getAndRemove(null, k1));
        assertOpOnNode(nodeKey2, "getAndDelete", x -> kvView.getAndRemove(null, k2));

        assertOpOnNode(nodeKey1, "replace", x -> kvView.replace(null, k1, v));
        assertOpOnNode(nodeKey2, "replace", x -> kvView.replace(null, k2, v));

        assertOpOnNode(nodeKey1, "replace", x -> kvView.replace(null, k1, v, v));
        assertOpOnNode(nodeKey2, "replace", x -> kvView.replace(null, k2, v, v));

        assertOpOnNode(nodeKey1, "delete", x -> kvView.remove(null, k1));
        assertOpOnNode(nodeKey2, "delete", x -> kvView.remove(null, k2));

        assertOpOnNode(nodeKey1, "deleteExact", x -> kvView.remove(null, k1, v));
        assertOpOnNode(nodeKey2, "deleteExact", x -> kvView.remove(null, k2, v));

        assertOpOnNode(nodeKey1, "deleteAll", x -> kvView.removeAll(null, List.of(k1)));
        assertOpOnNode(nodeKey2, "deleteAll", x -> kvView.removeAll(null, List.of(k2)));
    }

    @Test
    public void testAllKeyValueBinaryViewOperations() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();

        Tuple t1 = Tuple.create().set("ID", 1L);
        Tuple t2 = Tuple.create().set("ID", 2L);
        Tuple val = Tuple.create();

        assertOpOnNode(nodeKey1, "insert", x -> kvView.putIfAbsent(null, t1, val));
        assertOpOnNode(nodeKey2, "insert", x -> kvView.putIfAbsent(null, t2, val));

        assertOpOnNode(nodeKey1, "upsert", x -> kvView.put(null, t1, val));
        assertOpOnNode(nodeKey2, "upsert", x -> kvView.put(null, t2, val));

        assertOpOnNode(nodeKey1, "upsertAll", x -> kvView.putAll(null, Map.of(t1, val)));
        assertOpOnNode(nodeKey2, "upsertAll", x -> kvView.putAll(null, Map.of(t2, val)));

        assertOpOnNode(nodeKey1, "get", x -> kvView.get(null, t1));
        assertOpOnNode(nodeKey2, "get", x -> kvView.get(null, t2));

        assertOpOnNode(nodeKey1, "get", x -> kvView.contains(null, t1));
        assertOpOnNode(nodeKey2, "get", x -> kvView.contains(null, t2));

        assertOpOnNode(nodeKey1, "getAll", x -> kvView.getAll(null, List.of(t1)));
        assertOpOnNode(nodeKey2, "getAll", x -> kvView.getAll(null, List.of(t2)));

        assertOpOnNode(nodeKey1, "getAndUpsert", x -> kvView.getAndPut(null, t1, val));
        assertOpOnNode(nodeKey2, "getAndUpsert", x -> kvView.getAndPut(null, t2, val));

        assertOpOnNode(nodeKey1, "getAndReplace", x -> kvView.getAndReplace(null, t1, val));
        assertOpOnNode(nodeKey2, "getAndReplace", x -> kvView.getAndReplace(null, t2, val));

        assertOpOnNode(nodeKey1, "getAndDelete", x -> kvView.getAndRemove(null, t1));
        assertOpOnNode(nodeKey2, "getAndDelete", x -> kvView.getAndRemove(null, t2));

        assertOpOnNode(nodeKey1, "replace", x -> kvView.replace(null, t1, val));
        assertOpOnNode(nodeKey2, "replace", x -> kvView.replace(null, t2, val));

        assertOpOnNode(nodeKey1, "replace", x -> kvView.replace(null, t1, val, val));
        assertOpOnNode(nodeKey2, "replace", x -> kvView.replace(null, t2, val, val));

        assertOpOnNode(nodeKey1, "delete", x -> kvView.remove(null, t1));
        assertOpOnNode(nodeKey2, "delete", x -> kvView.remove(null, t2));

        assertOpOnNode(nodeKey1, "deleteExact", x -> kvView.remove(null, t1, val));
        assertOpOnNode(nodeKey2, "deleteExact", x -> kvView.remove(null, t2, val));

        assertOpOnNode(nodeKey1, "deleteAll", x -> kvView.removeAll(null, List.of(t1)));
        assertOpOnNode(nodeKey2, "deleteAll", x -> kvView.removeAll(null, List.of(t2)));
    }

    @Test
    public void testExecuteColocatedTupleKeyRoutesRequestToPrimaryNode() {
        Table table = defaultTable();

        Tuple t1 = Tuple.create().set("ID", 1L);
        Tuple t2 = Tuple.create().set("ID", 2L);

        assertThat(compute().executeColocatedAsync(table.name(), t1, List.of(), "job"), willBe(nodeKey1));
        assertThat(compute().executeColocatedAsync(table.name(), t2, List.of(), "job"), willBe(nodeKey2));
    }

    @Test
    public void testExecuteColocatedObjectKeyRoutesRequestToPrimaryNode() {
        var mapper = Mapper.of(Long.class);
        Table table = defaultTable();

        assertThat(compute().executeColocatedAsync(table.name(), 1L, mapper, List.of(), "job"), willBe(nodeKey1));
        assertThat(compute().executeColocatedAsync(table.name(), 2L, mapper, List.of(), "job"), willBe(nodeKey2));
    }

    @Test
    public void testDataStreamerRecordBinaryView() {
        RecordView<Tuple> recordView = defaultTable().recordView();

        Consumer<Tuple> stream = t -> {
            CompletableFuture<Void> fut;

            try (SimplePublisher<Tuple> publisher = new SimplePublisher<>()) {
                fut = recordView.streamData(publisher, null);
                publisher.submit(t);
            }

            fut.join();
        };

        assertOpOnNode(nodeKey0, "updateAll", x -> stream.accept(Tuple.create().set("ID", 0L)));
        assertOpOnNode(nodeKey1, "updateAll", x -> stream.accept(Tuple.create().set("ID", 1L)));
        assertOpOnNode(nodeKey2, "updateAll", x -> stream.accept(Tuple.create().set("ID", 2L)));
        assertOpOnNode(nodeKey3, "updateAll", x -> stream.accept(Tuple.create().set("ID", 3L)));
    }

    @Test
    public void testDataStreamerRecordView() {
        RecordView<PersonPojo> pojoView = defaultTable().recordView(Mapper.of(PersonPojo.class));

        Consumer<PersonPojo> stream = t -> {
            CompletableFuture<Void> fut;

            try (SimplePublisher<PersonPojo> publisher = new SimplePublisher<>()) {
                fut = pojoView.streamData(publisher, null);
                publisher.submit(t);
            }

            fut.join();
        };

        assertOpOnNode(nodeKey0, "updateAll", x -> stream.accept(new PersonPojo(0L)));
        assertOpOnNode(nodeKey1, "updateAll", x -> stream.accept(new PersonPojo(1L)));
        assertOpOnNode(nodeKey2, "updateAll", x -> stream.accept(new PersonPojo(2L)));
        assertOpOnNode(nodeKey3, "updateAll", x -> stream.accept(new PersonPojo(3L)));
    }

    @Test
    public void testDataStreamerKeyValueBinaryView() {
        KeyValueView<Tuple, Tuple> recordView = defaultTable().keyValueView();

        Consumer<Tuple> stream = t -> {
            CompletableFuture<Void> fut;

            try (SimplePublisher<Entry<Tuple, Tuple>> publisher = new SimplePublisher<>()) {
                fut = recordView.streamData(publisher, null);
                publisher.submit(Map.entry(t, Tuple.create()));
            }

            fut.join();
        };

        assertOpOnNode(nodeKey0, "updateAll", x -> stream.accept(Tuple.create().set("ID", 0L)));
        assertOpOnNode(nodeKey1, "updateAll", x -> stream.accept(Tuple.create().set("ID", 1L)));
        assertOpOnNode(nodeKey2, "updateAll", x -> stream.accept(Tuple.create().set("ID", 2L)));
        assertOpOnNode(nodeKey3, "updateAll", x -> stream.accept(Tuple.create().set("ID", 3L)));
    }

    @Test
    public void testDataStreamerKeyValueView() {
        KeyValueView<Long, String> kvView = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        Consumer<Long> stream = t -> {
            CompletableFuture<Void> fut;

            try (SimplePublisher<Entry<Long, String>> publisher = new SimplePublisher<>()) {
                fut = kvView.streamData(publisher, null);
                publisher.submit(Map.entry(t, t.toString()));
            }

            fut.join();
        };

        assertOpOnNode(nodeKey0, "updateAll", x -> stream.accept(0L));
        assertOpOnNode(nodeKey1, "updateAll", x -> stream.accept(1L));
        assertOpOnNode(nodeKey2, "updateAll", x -> stream.accept(2L));
        assertOpOnNode(nodeKey3, "updateAll", x -> stream.accept(3L));
    }

    @Test
    public void testDataStreamerReceivesPartitionAssignmentUpdates() {
        DataStreamerOptions options = DataStreamerOptions.builder()
                .pageSize(1)
                .perPartitionParallelOperations(1)
                .autoFlushFrequency(50)
                .build();

        CompletableFuture<Void> fut;

        RecordView<Tuple> recordView = defaultTable().recordView();
        try (SubmissionPublisher<DataStreamerItem<Tuple>> publisher = new SubmissionPublisher<>()) {
            fut = recordView.streamData(publisher, options);

            Consumer<Long> submit = id -> {
                try {
                    lastOpServerName = null;
                    publisher.submit(DataStreamerItem.of(Tuple.create().set("ID", id)));
                    assertTrue(IgniteTestUtils.waitForCondition(() -> lastOpServerName != null, 1000));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            };

            assertOpOnNode(nodeKey1, "updateAll", x -> submit.accept(1L));
            assertOpOnNode(nodeKey2, "updateAll", x -> submit.accept(2L));

            // Update partition assignment.
            initPrimaryReplicas(reversedReplicas());

            // Send some batches so that the client receives updated assignment.
            for (long i = 0; i < 10; i++) {
                submit.accept(i);
            }

            // Check updated assignment.
            assertOpOnNode(nodeKey2, "updateAll", x -> submit.accept(1L));
            assertOpOnNode(nodeKey1, "updateAll", x -> submit.accept(2L));
        }

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
        TableViewInternal tableView = tables.createTable(name, id);

        ((FakeInternalTable) tableView.internalTable()).setDataAccessListener((op, data) -> {
            lastOp = op;
            lastOpServerName = ignite.name();
        });
    }

    private static void initPrimaryReplicas(@Nullable List<String> replicas) {
        long leaseStartTime = new HybridClockImpl().nowLong();

        initPrimaryReplicas(testServer.placementDriver(), replicas, leaseStartTime);
        initPrimaryReplicas(testServer2.placementDriver(), replicas, leaseStartTime);
    }

    private static void initPrimaryReplicas(FakePlacementDriver placementDriver, @Nullable List<String> replicas, long leaseStartTime) {
        if (replicas == null) {
            replicas = defaultReplicas();
        }

        placementDriver.setReplicas(replicas, nextTableId.get() - 1, leaseStartTime);
    }

    private static List<String> defaultReplicas() {
        return List.of(testServer.nodeName(), testServer2.nodeName(), testServer.nodeName(), testServer2.nodeName());
    }

    private static List<String> reversedReplicas() {
        return List.of(testServer2.nodeName(), testServer.nodeName(), testServer2.nodeName(), testServer.nodeName());
    }
}
