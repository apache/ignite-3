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

import static java.util.Collections.emptyList;
import static java.util.Comparator.comparing;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.ClientTransactionInflights;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.internal.client.tx.ClientLazyTransaction;
import org.apache.ignite.internal.client.tx.ClientTransaction;
import org.apache.ignite.internal.table.partition.HashPartition;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.table.partition.PartitionManager;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

/**
 * Thin client transactions integration test.
 */
public class ItThinClientTransactionsTest extends ItAbstractThinClientTest {
    private static final String INFLIGHTS_FIELD_NAME = "inflights";

    @Test
    void testKvViewOperations() {
        KeyValueView<Integer, String> kvView = kvView();

        int key = 1;
        kvView.put(null, key, "1");

        Transaction tx = client().transactions().begin();
        kvView.put(tx, key, "22");

        assertTrue(kvView.contains(tx, key));
        assertFalse(kvView.remove(tx, key, "1"));
        assertFalse(kvView.putIfAbsent(tx, key, "111"));
        assertEquals("22", kvView.get(tx, key));
        assertEquals("22", kvView.getAndPut(tx, key, "33"));
        assertEquals("33", kvView.getAndReplace(tx, key, "44"));
        assertTrue(kvView.replace(tx, key, "55"));
        assertEquals("55", kvView.getAndRemove(tx, key));
        assertFalse(kvView.contains(tx, key));
        assertFalse(kvView.remove(tx, key));

        kvView.putAll(tx, Map.of(key, "6", 2, "7"));
        assertEquals(2, kvView.getAll(tx, List.of(key, 2, 3)).size());

        tx.rollback();
        assertEquals("1", kvView.get(null, key));
    }

    @Test
    void testKvViewBinaryOperations() {
        KeyValueView<Tuple, Tuple> kvView = table().keyValueView();

        Tuple key = key(1);
        kvView.put(null, key, val("1"));

        Transaction tx = client().transactions().begin();
        kvView.put(tx, key, val("22"));

        assertTrue(kvView.contains(tx, key));
        assertFalse(kvView.remove(tx, key, val("1")));
        assertFalse(kvView.putIfAbsent(tx, key, val("111")));
        assertEquals(val("22"), kvView.get(tx, key));
        assertEquals(val("22"), kvView.getAndPut(tx, key, val("33")));
        assertEquals(val("33"), kvView.getAndReplace(tx, key, val("44")));
        assertTrue(kvView.replace(tx, key, val("55")));
        assertEquals(val("55"), kvView.getAndRemove(tx, key));
        assertFalse(kvView.contains(tx, key));
        assertFalse(kvView.remove(tx, key));

        kvView.putAll(tx, Map.of(key, val("6"), key(2), val("7")));
        assertEquals(2, kvView.getAll(tx, List.of(key, key(2), key(3))).size());

        tx.rollback();
        assertEquals(val("1"), kvView.get(null, key));
    }

    @Test
    void testRecordViewOperations() {
        RecordView<Rec> recordView = table().recordView(Mapper.of(Rec.class));
        Rec key = rec(1, null);
        recordView.upsert(null, rec(1, "1"));

        Transaction tx = client().transactions().begin();
        recordView.upsert(tx, rec(1, "22"));

        assertFalse(recordView.deleteExact(tx, rec(1, "1")));
        assertFalse(recordView.insert(tx, rec(1, "111")));
        assertEquals(rec(1, "22"), recordView.get(tx, key));
        assertEquals(rec(1, "22"), recordView.getAndUpsert(tx, rec(1, "33")));
        assertEquals(rec(1, "33"), recordView.getAndReplace(tx, rec(1, "44")));
        assertTrue(recordView.replace(tx, rec(1, "55")));
        assertEquals(rec(1, "55"), recordView.getAndDelete(tx, key));
        assertFalse(recordView.delete(tx, key));

        recordView.upsertAll(tx, List.of(rec(1, "6"), rec(2, "7")));
        List<Rec> res = recordView.getAll(tx, List.of(key, rec(3, null), rec(2, null)));
        assertEquals(3, res.size());
        assertEquals(rec(1, "6"), res.get(0));
        assertNull(res.get(1));
        assertEquals(rec(2, "7"), res.get(2));

        tx.rollback();
        assertEquals(rec(1, "1"), recordView.get(null, key));
    }

    @Test
    void testRecordViewBinaryOperations() {
        RecordView<Tuple> recordView = table().recordView();

        Tuple key = key(1);
        recordView.upsert(null, kv(1, "1"));

        Transaction tx = client().transactions().begin();
        recordView.upsert(tx, kv(1, "22"));

        assertFalse(recordView.deleteExact(tx, kv(1, "1")));
        assertFalse(recordView.insert(tx, kv(1, "111")));
        assertEquals(kv(1, "22"), recordView.get(tx, key));
        assertEquals(kv(1, "22"), recordView.getAndUpsert(tx, kv(1, "33")));
        assertEquals(kv(1, "33"), recordView.getAndReplace(tx, kv(1, "44")));
        assertTrue(recordView.replace(tx, kv(1, "55")));
        assertEquals(kv(1, "55"), recordView.getAndDelete(tx, key));
        assertFalse(recordView.delete(tx, key));

        recordView.upsertAll(tx, List.of(kv(1, "6"), kv(2, "7")));
        assertEquals(3, recordView.getAll(tx, List.of(key, key(2), key(3))).size());

        tx.rollback();
        assertEquals(kv(1, "1"), recordView.get(null, key));
    }

    @Test
    void testCommitUpdatesData() {
        KeyValueView<Integer, String> kvView = kvView();
        kvView.put(null, 1, "1");

        Transaction tx = client().transactions().begin();
        assertEquals("1", kvView.get(null, 1));
        assertEquals("1", kvView.get(tx, 1));

        kvView.put(tx, 1, "2");
        assertEquals("2", kvView.get(tx, 1));

        tx.commit();
        assertEquals("2", kvView.get(null, 1));
    }

    @Test
    void testRollbackDoesNotUpdateData() {
        KeyValueView<Integer, String> kvView = kvView();
        kvView.put(null, 1, "1");

        Transaction tx = client().transactions().begin();
        assertEquals("1", kvView.get(null, 1));
        assertEquals("1", kvView.get(tx, 1));

        kvView.put(tx, 1, "2");
        assertEquals("2", kvView.get(tx, 1));

        tx.rollback();
        assertEquals("1", kvView.get(null, 1));
    }

    @Test
    void testAccessLockedKeyTimesOut() throws Exception {
        KeyValueView<Integer, String> kvView = kvView();

        Transaction tx1 = client().transactions().begin();
        client().sql().execute(tx1, "SELECT 1").close(); // Force lazy tx init.

        // Here we guarantee that tx2 will strictly after tx2 even if the transactions start in different server nodes.
        // TODO: https://issues.apache.org/jira/browse/IGNITE-19900 Client should participate in RW TX clock adjustment
        Thread.sleep(50);

        // Lock the key in tx2.
        Transaction tx2 = client().transactions().begin();

        try {
            kvView.put(tx2, -100, "1");

            // Get the key in tx1 - time out.
            assertThrows(TimeoutException.class, () -> kvView.getAsync(tx1, -100).get(1, TimeUnit.SECONDS));
        } finally {
            tx2.rollback();
        }
    }

    @Test
    void testCommitRollbackSameTxDoesNotThrow() {
        Transaction tx = client().transactions().begin();
        tx.commit();

        assertDoesNotThrow(tx::rollback, "Unexpected exception was thrown.");
        assertDoesNotThrow(tx::commit, "Unexpected exception was thrown.");
        assertDoesNotThrow(tx::rollback, "Unexpected exception was thrown.");
    }

    @Test
    void testRollbackCommitSameTxDoesNotThrow() {
        Transaction tx = client().transactions().begin();
        tx.rollback();

        assertDoesNotThrow(tx::commit, "Unexpected exception was thrown.");
        assertDoesNotThrow(tx::rollback, "Unexpected exception was thrown.");
        assertDoesNotThrow(tx::commit, "Unexpected exception was thrown.");
    }

    @Test
    void testCustomTransactionInterfaceThrows() {
        var tx = new Transaction() {
            @Override
            public void commit() throws TransactionException {
            }

            @Override
            public CompletableFuture<Void> commitAsync() {
                return null;
            }

            @Override
            public void rollback() throws TransactionException {
            }

            @Override
            public CompletableFuture<Void> rollbackAsync() {
                return null;
            }

            @Override
            public boolean isReadOnly() {
                return false;
            }
        };

        var ex = assertThrows(IgniteException.class, () -> kvView().put(tx, 1, "1"));

        String expected = "Unsupported transaction implementation: "
                + "'class org.apache.ignite.internal.runner.app.client.ItThinClientTransactionsTest";

        assertThat(ex.getMessage(), containsString(expected));
    }

    @Test
    void testTransactionFromAnotherChannelThrows() {
        Transaction tx = client().transactions().begin();
        client().sql().execute(tx, "SELECT 1").close(); // Force lazy tx init.

        try (IgniteClient client2 = IgniteClient.builder().addresses(getNodeAddress()).build()) {
            RecordView<Tuple> recordView = client2.tables().tables().get(0).recordView();

            var ex = assertThrows(IgniteException.class, () -> recordView.upsert(tx, Tuple.create()));

            assertThat(ex.getMessage(), containsString("Transaction context has been lost due to connection errors"));
            assertEquals(ErrorGroups.Client.CONNECTION_ERR, ex.code());
        }
    }

    @Test
    void testReadOnlyTxSeesOldDataAfterUpdate() {
        KeyValueView<Integer, String> kvView = kvView();
        kvView.put(null, 1, "1");

        Transaction tx = client().transactions().begin(new TransactionOptions().readOnly(true));
        assertEquals("1", kvView.get(tx, 1));

        // Update data in a different tx.
        Transaction tx2 = client().transactions().begin();
        kvView.put(tx2, 1, "2");
        tx2.commit();

        // Old tx sees old data.
        assertEquals("1", kvView.get(tx, 1));

        // New tx sees new data
        Transaction tx3 = client().transactions().begin(new TransactionOptions().readOnly(true));
        assertEquals("2", kvView.get(tx3, 1));
    }

    @Test
    void testUpdateInReadOnlyTxThrows() {
        KeyValueView<Integer, String> kvView = kvView();
        kvView.put(null, 1, "1");

        Transaction tx = client().transactions().begin(new TransactionOptions().readOnly(true));
        var ex = assertThrows(TransactionException.class, () -> kvView.put(tx, 1, "2"));

        assertThat(ex.getMessage(), containsString("Failed to enlist read-write operation into read-only transaction"));
        assertEquals(ErrorGroups.Transactions.TX_FAILED_READ_WRITE_OPERATION_ERR, ex.code());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCommitRollbackReadOnlyTxDoesNothing(boolean commit) {
        KeyValueView<Integer, String> kvView = kvView();
        kvView.put(null, 10, "1");

        Transaction tx = client().transactions().begin(new TransactionOptions().readOnly(true));
        assertEquals("1", kvView.get(tx, 10));

        if (commit) {
            tx.commit();
        } else {
            tx.rollback();
        }
    }

    @Test
    void testReadOnlyTxAttributes() {
        Transaction tx = client().transactions().begin(new TransactionOptions().readOnly(true));

        assertTrue(tx.isReadOnly());

        tx.rollback();
    }

    @Test
    void testReadWriteTxAttributes() {
        Transaction tx = client().transactions().begin();

        assertFalse(tx.isReadOnly());

        tx.rollback();
    }

    @Test
    void testKillTransaction() {
        @SuppressWarnings("resource") IgniteClient client = client();
        KeyValueView<Integer, String> kvView = kvView();

        Transaction tx = client.transactions().begin();
        kvView.put(tx, 1, "1");

        try (ResultSet<SqlRow> cursor = client.sql().execute(null, "SELECT TRANSACTION_ID FROM SYSTEM.TRANSACTIONS")) {
            cursor.forEachRemaining(r -> {
                String txId = r.stringValue("TRANSACTION_ID");
                client.sql().executeScript("KILL TRANSACTION '" + txId + "'");
            });
        }

        TransactionException ex = assertThrows(TransactionException.class, tx::commit);

        assertThat(ex.getMessage(), startsWith("Transaction is killed"));
        assertEquals("IGN-TX-13", ex.codeAsString());
    }

    @Test
    void testTxWithTimeout() throws InterruptedException {
        KeyValueView<Tuple, Tuple> kvView = table().keyValueView();

        Transaction tx = client().transactions().begin(new TransactionOptions().timeoutMillis(450));

        // Load partition map to ensure all entries are directly mapped.
        Map<Partition, ClusterNode> map = table().partitionManager().primaryReplicasAsync().join();

        assertEquals(PARTITIONS, map.size());

        int k = 111; // Avoid intersection with previous tests.

        Map<Tuple, Tuple> txMap = new HashMap<>();

        Tuple k1 = key(k);
        Partition p1 = table().partitionManager().partitionAsync(k1).join();
        Tuple v1 = val(String.valueOf(k));
        kvView.put(tx, k1, v1);
        txMap.put(k1, v1);

        ClientTransaction tx0 = ClientLazyTransaction.get(tx).startedTx();

        UUID txId = tx0.txId();

        int coordIdx = -1;

        for (int i = 0; i < 2; i++) {
            Ignite server = server(i);
            if (server.name().equals(map.get(p1).name())) {
                coordIdx = i;
                break;
            }
        }

        assertTrue(coordIdx != -1);

        IgniteImpl coord = TestWrappers.unwrapIgniteImpl(server(coordIdx));
        assertNotNull(coord.txManager().stateMeta(txId), "Transaction expected to be colocated with enlistment");

        IgniteImpl other = TestWrappers.unwrapIgniteImpl(server(1 - coordIdx));

        do {
            k++;
            Tuple kt = key(k);
            Tuple vt = val(String.valueOf(k));
            kvView.put(tx, kt, vt);
            txMap.put(kt, vt);

            // Stop then a tx enlisted on both nodes (in direct mapping mode).
        } while (other.txManager().stateMeta(txId) == null);

        assertEquals(TxState.PENDING, coord.txManager().stateMeta(txId).txState());
        assertEquals(TxState.PENDING, other.txManager().stateMeta(txId).txState());

        assertTrue(IgniteTestUtils.waitForCondition(() -> coord.txManager().stateMeta(txId).txState() == TxState.ABORTED, 3_000),
                "Tx is not timed out: " + coord.txManager().stateMeta(txId));

        assertEquals(TxState.PENDING, other.txManager().stateMeta(txId).txState());

        // Enlist after timeout should fail.
        while (true) {
            Tuple t = key(k);
            Tuple v = val(String.valueOf(k));
            try {
                kvView.put(tx, t, v);

                Thread.sleep(50);
            } catch (TransactionException e) {
                assertEquals(Transactions.TX_ALREADY_FINISHED_WITH_TIMEOUT_ERR, e.code());
                break;
            }
        }

        // A separate transaction should not wait for locks.
        Transaction tx2 = client().transactions().begin();
        Map<Tuple, Tuple> map2 = kvView.getAll(tx2, txMap.keySet()); // Triggers recovery.
        assertEquals(0, map2.size());

        assertTrue(IgniteTestUtils.waitForCondition(() -> coord.txManager().stateMeta(txId).txState() == TxState.ABORTED, 3_000),
                "Tx is not timed out: " + coord.txManager().stateMeta(txId));
        assertTrue(IgniteTestUtils.waitForCondition(() -> other.txManager().stateMeta(txId).txState() == TxState.ABORTED, 3_000),
                "Tx is not timed out: " + other.txManager().stateMeta(txId));

        tx2.commit();
    }

    static List<Tuple> generateKeysForNode(
            int start,
            int count,
            Map<Partition, ClusterNode> map,
            ClusterNode clusterNode,
            Table table
    ) {
        String clusterNodeName = clusterNode.name();
        if (map.values().stream().noneMatch(x -> Objects.equals(x.name(), clusterNodeName))) {
            return emptyList();
        }

        List<Tuple> keys = new ArrayList<>();
        PartitionManager partitionManager = table.partitionManager();

        int k = start;
        while (keys.size() != count) {
            k++;
            Tuple t = key(k);

            Partition part = partitionManager.partitionAsync(t).orTimeout(5, TimeUnit.SECONDS).join();
            ClusterNode node = map.get(part);

            if (node.name().equals(clusterNodeName)) {
                keys.add(t);
            }
        }

        return keys;
    }

    static List<Tuple> generateKeysForPartition(
            int start,
            int count,
            Map<Partition, ClusterNode> map,
            int partId,
            Table table
    ) {
        List<Tuple> keys = new ArrayList<>();
        PartitionManager partitionManager = table.partitionManager();

        int k = start;
        while (keys.size() != count) {
            k++;
            Tuple t = key(k);

            Partition part = partitionManager.partitionAsync(t).orTimeout(5, TimeUnit.SECONDS).join();
            HashPartition hashPart = (HashPartition) part;

            if (hashPart.partitionId() == partId) {
                keys.add(t);
            }
        }

        return keys;
    }

    private static Integer partitions(Collection<Tuple> keys, Table table) {
        PartitionManager partitionManager = table.partitionManager();

        Set<Partition> count = new HashSet<>();

        for (Tuple key : keys) {
            Partition part = partitionManager.partitionAsync(key).orTimeout(5, TimeUnit.SECONDS).join();
            count.add(part);
        }

        return count.size();
    }

    private List<Tuple> retainSinglePartitionKeys(List<Tuple> list, int count) {
        List<Tuple> keys = new ArrayList<>();

        Partition part0 = null;

        for (Tuple t : list) {
            Partition part = table().partitionManager().partitionAsync(t).join();

            if (part0 == null) {
                part0 = part;
            }

            if (part.equals(part0)) {
                keys.add(t);
            }

            if (keys.size() == count) {
                break;
            }
        }

        return keys;
    }

    @Test
    void testAssignmentLoadedDuringTransaction() {
        // Wait for assignments.
        table().partitionManager().primaryReplicasAsync().join();

        ClientTable table = (ClientTable) table();

        ClientTable spyTable = Mockito.spy(table);

        Map<Partition, ClusterNode> map = table().partitionManager().primaryReplicasAsync().join();

        List<String> origPartMap = map.entrySet().stream().sorted(comparing(e -> {
            HashPartition part = (HashPartition) e.getKey();
            return part.partitionId();
        })).map(e -> e.getValue().name()).collect(Collectors.toList());

        List<String> emptyPartMap = new ArrayList<>(Collections.nCopies(10, null));

        AtomicReference<List<String>> holder = new AtomicReference<>(emptyPartMap);

        Mockito.doAnswer(inv -> CompletableFuture.completedFuture(holder.get())).when(spyTable).getPartitionAssignment();

        ClientLazyTransaction tx0 = (ClientLazyTransaction) client().transactions().begin();

        IgniteImpl server0 = TestWrappers.unwrapIgniteImpl(server(0));

        List<Tuple> tuples0 = generateKeysForNode(200, 2, map, server0.cluster().localNode(), table);

        Tuple key = tuples0.get(0);
        Tuple val = val(key.intValue(0) + "");
        spyTable.keyValueView().put(tx0, key, val);

        ClientTransaction tx = tx0.startedTx();

        assertFalse(tx.hasCommitPartition(), "Expected proxy mode");

        // Next enlistment uses loaded partition map.
        holder.set(origPartMap);

        Tuple key2 = tuples0.get(1);
        Tuple val2 = val(key2.intValue(0) + "");
        spyTable.keyValueView().put(tx0, key2, val2);

        assertEquals(0, tx.enlistedCount(), "Expecting proxy mode");

        tx.commit();
    }

    @Test
    void testMixedMappingScenario1() {
        Map<Partition, ClusterNode> map = table().partitionManager().primaryReplicasAsync().join();

        ClientTable table = (ClientTable) table();

        IgniteImpl server0 = TestWrappers.unwrapIgniteImpl(server(0));
        IgniteImpl server1 = TestWrappers.unwrapIgniteImpl(server(1));

        List<Tuple> tuples0 = generateKeysForNode(300, 1, map, server0.cluster().localNode(), table);
        List<Tuple> tuples1 = generateKeysForNode(310, 1, map, server1.cluster().localNode(), table);

        Map<Tuple, Tuple> data = new HashMap<>();

        data.put(tuples0.get(0), val(tuples0.get(0).intValue(0) + ""));
        data.put(tuples1.get(0), val(tuples1.get(0).intValue(0) + ""));

        ClientLazyTransaction tx0 = (ClientLazyTransaction) client().transactions().begin();

        table.keyValueView().putAll(tx0, data);

        for (Entry<Tuple, Tuple> entry : data.entrySet()) {
            table.keyValueView().put(tx0, entry.getKey(), entry.getValue());
        }

        tx0.commit();

        for (Entry<Tuple, Tuple> entry : data.entrySet()) {
            table.keyValueView().put(null, entry.getKey(), entry.getValue());
        }
    }

    @Test
    void testMixedMappingScenario2() {
        Map<Partition, ClusterNode> map = table().partitionManager().primaryReplicasAsync().join();

        ClientTable table = (ClientTable) table();

        IgniteImpl server0 = TestWrappers.unwrapIgniteImpl(server(0));
        IgniteImpl server1 = TestWrappers.unwrapIgniteImpl(server(1));

        List<Tuple> tuples0 = generateKeysForNode(400, 1, map, server0.cluster().localNode(), table);
        List<Tuple> tuples1 = generateKeysForNode(410, 1, map, server1.cluster().localNode(), table);

        Map<Tuple, Tuple> data = new HashMap<>();

        ClientLazyTransaction tx0 = (ClientLazyTransaction) client().transactions().begin();

        Tuple k1 = tuples0.get(0);
        Tuple v1 = val(tuples0.get(0).intValue(0) + "");
        data.put(k1, v1);
        table.keyValueView().put(tx0, k1, v1);

        Tuple k2 = tuples1.get(0);
        Tuple v2 = val(tuples1.get(0).intValue(0) + "");
        data.put(k2, v2);
        table.keyValueView().put(tx0, k2, v2);

        table.keyValueView().putAll(tx0, data);

        tx0.commit();

        for (Entry<Tuple, Tuple> entry : data.entrySet()) {
            table.keyValueView().put(null, entry.getKey(), entry.getValue());
        }
    }

    @Test
    void testMixedMappingScenarioWithNoopEnlistment() throws Exception {
        // Inject spied instance.
        TcpIgniteClient clent0 = (TcpIgniteClient) client();
        Field inflightsField = clent0.channel().getClass().getDeclaredField(INFLIGHTS_FIELD_NAME);
        inflightsField.setAccessible(true);
        ClientTransactionInflights inflights = clent0.channel().inflights();
        ClientTransactionInflights spyed = Mockito.spy(inflights);
        inflightsField.set(clent0.channel(), spyed);

        for (ClientChannel channel : clent0.channel().channels()) {
            Field f = channel.getClass().getDeclaredField(INFLIGHTS_FIELD_NAME);
            f.setAccessible(true);
            f.set(channel, spyed);
        }

        Map<Partition, ClusterNode> map = table().partitionManager().primaryReplicasAsync().join();

        ClientTable table = (ClientTable) table();

        IgniteImpl server0 = TestWrappers.unwrapIgniteImpl(server(0));
        IgniteImpl server1 = TestWrappers.unwrapIgniteImpl(server(1));

        List<Tuple> tuples0 = generateKeysForNode(500, 1, map, server0.cluster().localNode(), table);
        List<Tuple> tuples1 = generateKeysForNode(510, 80, map, server1.cluster().localNode(), table);

        ClientLazyTransaction tx0 = (ClientLazyTransaction) client().transactions().begin();

        // First operation is colocated with txn coordinator and not enlisted via remote transaction.
        Tuple k = tuples0.get(0);
        Tuple v = val(tuples0.get(0).intValue(0) + "");

        KeyValueView<Tuple, Tuple> view = table.keyValueView();
        view.put(tx0, k, v);

        // All other operations are directly mapped.
        int val = 0;
        Tuple k1 = tuples1.get(val);
        Tuple v1 = val(tuples1.get(val).intValue(0) + "");

        val++;
        Tuple k2 = tuples1.get(val);
        Tuple v2 = val(tuples1.get(val).intValue(0) + "");

        int c = 5;
        int c0 = c;

        Map<Tuple, Tuple> batch0 = new HashMap<>();
        while (c-- > 0) {
            val++;
            batch0.put(tuples1.get(val), val(tuples1.get(val).intValue(0) + ""));
        }

        view.put(tx0, k1, v1); // Write
        assertTrue(Tuple.equals(v1, view.get(tx0, k1))); // Read
        assertTrue(view.putIfAbsent(tx0, k2, v2)); // Write
        assertFalse(view.putIfAbsent(tx0, k2, v2)); // No-op write.

        int partitions = partitions(batch0.keySet(), table);
        view.putAll(tx0, batch0); // Write in batch split mode.
        Map<Tuple, Tuple> readBack = view.getAll(tx0, batch0.keySet()); // Read.
        assertEquals(c0, readBack.size());

        assertTrue(Tuple.equals(v1, view.getAndPut(tx0, k1, v2))); // Write
        assertTrue(Tuple.equals(v2, view.get(tx0, k1))); // Read

        assertTrue(Tuple.equals(v2, view.getAndRemove(tx0, k1))); // Write
        assertNull(view.get(tx0, k1)); // Read
        assertNull(view.getAndRemove(tx0, k1)); // No-op write

        assertNull(view.getAndReplace(tx0, k1, v1)); // No-op write
        assertTrue(Tuple.equals(v2, view.getAndReplace(tx0, k2, v1))); // Write
        assertTrue(Tuple.equals(v1, view.get(tx0, k2))); // Read

        assertFalse(view.remove(tx0, k1)); // No-op write
        Tuple keyRmv = batch0.keySet().iterator().next();
        Tuple rmv = batch0.remove(keyRmv);
        assertTrue(view.remove(tx0, keyRmv)); // Write
        Tuple keyRmv2 = batch0.keySet().iterator().next();
        Tuple rmv2 = batch0.remove(keyRmv2);
        assertFalse(view.remove(tx0, keyRmv2, v1)); // No-op write
        assertTrue(view.remove(tx0, keyRmv2, rmv2)); // Write

        Tuple keyRmv3 = batch0.keySet().iterator().next();
        batch0.remove(keyRmv3);
        partitions += partitions(batch0.keySet(), table);
        assertEquals(0, view.removeAll(tx0, batch0.keySet()).size()); // Batch split write
        partitions += partitions(batch0.keySet(), table);
        assertEquals(2, view.removeAll(tx0, batch0.keySet()).size()); // Batch split no-op write

        assertTrue(view.replace(tx0, keyRmv3, v1)); // Write
        assertFalse(view.replace(tx0, keyRmv2, v1)); // No-op write

        assertTrue(view.replace(tx0, keyRmv3, v1, v2)); // Write
        assertFalse(view.replace(tx0, keyRmv3, v1, v2)); // No-op write

        Tuple rec0 = kv(keyRmv.intValue(0), rmv.stringValue(0));

        RecordView<Tuple> recView = table.recordView();
        assertTrue(recView.insert(tx0, rec0)); // Write
        assertFalse(recView.insert(tx0, rec0)); // No-op write

        List<Tuple> recs = new ArrayList<>();
        for (Entry<Tuple, Tuple> entry : batch0.entrySet()) {
            recs.add(kv(entry.getKey().intValue(0), entry.getValue().stringValue(0)));
        }

        partitions += partitions(batch0.keySet(), table);
        assertEquals(0, recView.insertAll(tx0, recs).size()); // Batch split write
        partitions += partitions(batch0.keySet(), table);
        assertEquals(recs.size(), recView.insertAll(tx0, recs).size()); // Batch split no-op write

        partitions += partitions(batch0.keySet(), table);
        assertEquals(0, recView.deleteAllExact(tx0, recs).size()); // Batch split write
        partitions += partitions(batch0.keySet(), table);
        assertEquals(recs.size(), recView.deleteAllExact(tx0, recs).size()); // Batch split no-op write

        assertTrue(recView.deleteExact(tx0, rec0)); // Write
        assertFalse(recView.deleteExact(tx0, rec0)); // No-op write

        tx0.commit();

        // Expecting each write operation to trigger add/remove events.
        int exp = 20 + partitions;
        Mockito.verify(spyed, Mockito.times(exp)).addInflight(tx0.startedTx().txId());
        Mockito.verify(spyed, Mockito.times(exp)).removeInflight(Mockito.eq(tx0.startedTx().txId()), Mockito.any());

        // Check if all locks are released.
        Map<Tuple, Tuple> batch = new HashMap<>();

        for (Tuple tup : tuples0) {
            batch.put(tup, val(tup.intValue(0) + ""));
        }

        for (Tuple tup : tuples1) {
            batch.put(tup, val(tup.intValue(0) + ""));
        }

        view.putAll(null, batch);
    }

    @Test
    void testBatchScenarioWithNoopEnlistmentImplicit() {
        Map<Partition, ClusterNode> map = table().partitionManager().primaryReplicasAsync().join();

        ClientTable table = (ClientTable) table();

        IgniteImpl server0 = TestWrappers.unwrapIgniteImpl(server(0));
        IgniteImpl server1 = TestWrappers.unwrapIgniteImpl(server(1));

        List<Tuple> tuples0 = generateKeysForNode(600, 50, map, server0.cluster().localNode(), table);
        List<Tuple> tuples1 = generateKeysForNode(610, 50, map, server1.cluster().localNode(), table);

        Map<Tuple, Tuple> batch = new HashMap<>();

        for (Tuple tup : tuples0) {
            batch.put(tup, val(tup.intValue(0) + ""));
        }

        for (Tuple tup : tuples1) {
            batch.put(tup, val(tup.intValue(0) + ""));
        }

        KeyValueView<Tuple, Tuple> view = table.keyValueView();
        view.putAll(null, batch);

        assertEquals(batch.size(), view.getAll(null, batch.keySet()).size());

        assertEquals(0, view.removeAll(null, batch.keySet()).size());
        assertEquals(batch.size(), view.removeAll(null, batch.keySet()).size());
    }

    @Test
    void testImplicitDirectMapping() {
        Map<Partition, ClusterNode> map = table().partitionManager().primaryReplicasAsync().join();

        ClientTable table = (ClientTable) table();

        IgniteImpl server0 = TestWrappers.unwrapIgniteImpl(server(0));
        IgniteImpl server1 = TestWrappers.unwrapIgniteImpl(server(1));

        List<Tuple> tuples0 = generateKeysForNode(600, 2, map, server0.cluster().localNode(), table);
        List<Tuple> tuples1 = generateKeysForNode(610, 1, map, server1.cluster().localNode(), table);

        assertEquals(2, tuples0.size());
        assertEquals(1, tuples1.size());

        Map<Tuple, Tuple> batch = new HashMap<>();

        for (Tuple tup : tuples0) {
            batch.put(tup, val(tup.intValue(0) + ""));
        }

        for (Tuple tup : tuples1) {
            batch.put(tup, val(tup.intValue(0) + ""));
        }

        KeyValueView<Tuple, Tuple> view = table.keyValueView();
        Transaction tx = client().transactions().begin();
        view.putAll(tx, batch);

        // Should retry until timeout.
        CompletableFuture<Map<Tuple, Tuple>> fut = view.getAllAsync(null, batch.keySet());

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        assertFalse(fut.isDone());
        tx.commit();

        assertEquals(batch.size(), fut.join().size(), "Implicit tx should be retried until timeout");

        // Retry transaction without other locker.
        assertEquals(batch.size(), view.getAll(null, batch.keySet()).size());

        // Retry expliti transaction.
        Transaction tx1 = client().transactions().begin();
        assertEquals(batch.size(), view.getAll(tx1, batch.keySet()).size());
        tx1.commit();

        // Test if we don't stuck in locks in subsequent rw txn.
        CompletableFuture<Void> fut0 = CompletableFuture.runAsync(() -> {
            Transaction tx0 = client().transactions().begin();
            view.put(tx0, tuples0.get(0), val("newval0"));
            tx0.commit();
        });

        CompletableFuture<Void> fut1 = CompletableFuture.runAsync(() -> {
            view.put(null, tuples1.get(0), val("newval1"));
        });

        fut0.join();
        fut1.join();
    }

    @Test
    void testImplicitRecordDirectMapping() {
        Map<Partition, ClusterNode> map = table().partitionManager().primaryReplicasAsync().join();

        ClientTable table = (ClientTable) table();

        IgniteImpl server0 = TestWrappers.unwrapIgniteImpl(server(0));
        IgniteImpl server1 = TestWrappers.unwrapIgniteImpl(server(1));

        List<Tuple> keys0 = generateKeysForNode(600, 2, map, server0.cluster().localNode(), table);
        List<Tuple> keys1 = generateKeysForNode(610, 1, map, server1.cluster().localNode(), table);

        assertEquals(2, keys0.size());
        assertEquals(1, keys1.size());

        List<Tuple> keys = new ArrayList<>();
        List<Tuple> recsBatch = new ArrayList<>();

        for (Tuple tup : keys0) {
            recsBatch.add(kv(tup.intValue(0), tup.intValue(0) + ""));
            keys.add(tup);
        }

        for (Tuple tup : keys1) {
            recsBatch.add(kv(tup.intValue(0), tup.intValue(0) + ""));
            keys.add(tup);
        }

        RecordView<Tuple> view = table.recordView();
        Transaction tx = client().transactions().begin();
        view.upsertAll(tx, recsBatch);

        // Should retry until timeout.
        CompletableFuture<List<Tuple>> fut = view.getAllAsync(null, keys);

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        assertFalse(fut.isDone());
        tx.commit();

        assertEquals(recsBatch.size(), fut.join().size(), "Implicit tx should be retried until timeout");

        // Retry transaction without other locker.
        assertEquals(recsBatch.size(), view.getAll(null, keys).size());

        // Retry explitit transaction.
        Transaction tx1 = client().transactions().begin();
        assertEquals(recsBatch.size(), view.getAll(tx1, keys).size());
        tx1.commit();

        // Test if we don't stuck in locks in subsequent rw txn.
        CompletableFuture.runAsync(() -> {
            Transaction tx0 = client().transactions().begin();
            view.upsert(tx0, keys0.get(0));
            tx0.commit();
        }).join();

        CompletableFuture.runAsync(() -> {
            view.upsert(null, keys1.get(0));
        }).join();
    }

    @Test
    void testBatchScenarioWithNoopEnlistmentExplicit() {
        Map<Partition, ClusterNode> map = table().partitionManager().primaryReplicasAsync().join();

        ClientTable table = (ClientTable) table();

        IgniteImpl server0 = TestWrappers.unwrapIgniteImpl(server(0));
        IgniteImpl server1 = TestWrappers.unwrapIgniteImpl(server(1));

        List<Tuple> tuples0 = generateKeysForNode(700, 50, map, server0.cluster().localNode(), table);
        List<Tuple> tuples1 = generateKeysForNode(710, 50, map, server1.cluster().localNode(), table);

        Map<Tuple, Tuple> batch = new HashMap<>();

        for (Tuple tup : tuples0) {
            batch.put(tup, val(tup.intValue(0) + ""));
        }

        for (Tuple tup : tuples1) {
            batch.put(tup, val(tup.intValue(0) + ""));
        }

        KeyValueView<Tuple, Tuple> tupleView = table.keyValueView();

        Transaction tx = client().transactions().begin();
        tupleView.putAll(tx, batch);
        tx.commit();

        tx = client().transactions().begin();
        assertEquals(batch.size(), tupleView.getAll(tx, batch.keySet()).size());
        tx.commit();

        tx = client().transactions().begin();
        assertEquals(0, tupleView.removeAll(tx, batch.keySet()).size());
        tx.commit();

        tx = client().transactions().begin();
        assertEquals(batch.size(), tupleView.removeAll(tx, batch.keySet()).size());
        tx.commit();

        RecordView<Tuple> recordView = table.recordView();
        List<Tuple> recs = new ArrayList<>(batch.size());
        for (Entry<Tuple, Tuple> entry : batch.entrySet()) {
            recs.add(kv(entry.getKey().intValue(0), entry.getValue().stringValue(0)));
        }

        tx = client().transactions().begin();
        assertEquals(0, recordView.insertAll(tx, recs).size());
        tx.commit();

        tx = client().transactions().begin();
        assertEquals(recs.size(), recordView.insertAll(tx, recs).size());
        tx.commit();

        tx = client().transactions().begin();
        assertEquals(0, recordView.deleteAllExact(tx, recs).size());
        tx.commit();

        tx = client().transactions().begin();
        assertEquals(recs.size(), recordView.deleteAllExact(tx, recs).size());
        tx.commit();

        tx = client().transactions().begin();
        tupleView.putAll(tx, batch);
        tx.commit();

        tx = client().transactions().begin();
        assertEquals(0, recordView.deleteAll(tx, batch.keySet()).size());
        tx.commit();

        tx = client().transactions().begin();
        assertEquals(batch.size(), recordView.deleteAll(tx, batch.keySet()).size());
        tx.commit();
    }

    @Test
    void testExplicitReadWriteTransaction() {
        ClientTable table = (ClientTable) table();

        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

        // Load partition map to ensure all entries are directly mapped.
        Map<Partition, ClusterNode> map = table.partitionManager().primaryReplicasAsync().join();

        IgniteImpl server0 = TestWrappers.unwrapIgniteImpl(server(0));
        IgniteImpl server1 = TestWrappers.unwrapIgniteImpl(server(1));

        List<Tuple> tuples0 = generateKeysForNode(600, 20, map, server0.cluster().localNode(), table);
        List<Tuple> tuples1 = generateKeysForNode(600, 20, map, server1.cluster().localNode(), table);

        Tuple k1 = tuples0.get(0);
        Tuple v1 = val(tuples0.get(0).intValue(0) + "");

        Tuple k2 = tuples1.get(1);
        Tuple v2 = val(tuples1.get(1).intValue(0) + "");

        Transaction tx0 = client().transactions().begin();
        kvView.put(tx0, k1, v1);
        kvView.put(tx0, k2, v2);
        tx0.commit();

        Map<Tuple, Tuple> crossPartitionBatch = new HashMap<>();
        crossPartitionBatch.put(k1, v1);
        crossPartitionBatch.put(k2, v2);

        Map<Tuple, Tuple> res = kvView.getAll(null, crossPartitionBatch.keySet());

        assertEquals(crossPartitionBatch, res);

        assertEquals(v1, kvView.get(null, k1));
        assertEquals(v2, kvView.get(null, k2));
    }

    @Test
    void testExplicitReadOnlyTransaction() {
        ClientTable table = (ClientTable) table();

        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

        // Load partition map to ensure all entries are directly mapped.
        Map<Partition, ClusterNode> map = table.partitionManager().primaryReplicasAsync().join();

        IgniteImpl server0 = TestWrappers.unwrapIgniteImpl(server(0));
        IgniteImpl server1 = TestWrappers.unwrapIgniteImpl(server(1));

        List<Tuple> tuples0 = generateKeysForNode(600, 20, map, server0.cluster().localNode(), table);
        List<Tuple> tuples1 = generateKeysForNode(600, 20, map, server1.cluster().localNode(), table);

        Tuple k1 = tuples0.get(0);
        Tuple v1 = val(tuples0.get(0).intValue(0) + "");

        Tuple k2 = tuples1.get(1);
        Tuple v2 = val(tuples1.get(1).intValue(0) + "");

        kvView.put(null, k1, v1);
        kvView.put(null, k2, v2);

        // Create pending locks before RO gets.
        Transaction tx0 = client().transactions().begin();
        kvView.put(tx0, k1, v1);
        kvView.put(tx0, k2, v2);

        Transaction tx1 = client().transactions().begin(new TransactionOptions().readOnly(true));

        assertTrue(Tuple.equals(v1, kvView.get(tx1, k1)));
        assertTrue(Tuple.equals(v2, kvView.get(tx1, k2)));

        tx1.commit();
        tx0.commit();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testReadOnlyTxDoesNotSeeUpdatesAfterStart(boolean server) {
        //noinspection resource
        Ignite ignite = server ? server() : client();
        KeyValueView<Integer, String> kvView = ignite.tables().table(TABLE_NAME).keyValueView(Integer.class, String.class);

        // Start RO TX, don't access anything yet.
        // Lazy client TX should record the start time at this point.
        Transaction tx = ignite.transactions().begin(new TransactionOptions().readOnly(true));

        // Put outside of TX.
        kvView.put(null, 123, "123");

        // RO tx does not see the value.
        String val = kvView.get(tx, 123);
        assertNull(val, "Read-only transaction should not see values committed after its start");
    }

    @AfterEach
    protected void validateInflights() throws NoSuchFieldException {
        System.out.println("DBG: validateInflights");
        TcpIgniteClient clent0 = (TcpIgniteClient) client();
        Field inflightsField = clent0.channel().getClass().getDeclaredField(INFLIGHTS_FIELD_NAME);
        inflightsField.setAccessible(true);
        ClientTransactionInflights inflights = clent0.channel().inflights();
        assertEquals(0, inflights.map().size());
    }

    private KeyValueView<Integer, String> kvView() {
        return table().keyValueView(Mapper.of(Integer.class), Mapper.of(String.class));
    }

    private Table table() {
        return client().tables().tables().get(0);
    }

    private static Tuple val(String v) {
        return Tuple.create().set(COLUMN_VAL, v);
    }

    private static Tuple key(Integer k) {
        return Tuple.create().set(COLUMN_KEY, k);
    }

    private static Tuple kv(Integer k, String v) {
        return Tuple.create().set(COLUMN_KEY, k).set(COLUMN_VAL, v);
    }

    private static Rec rec(int key, String val) {
        var r = new Rec();

        r.key = key;
        r.val = val;

        return r;
    }

    private static class Rec {
        int key;
        String val;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Rec rec = (Rec) o;
            return key == rec.key && Objects.equals(val, rec.val);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, val);
        }
    }
}
