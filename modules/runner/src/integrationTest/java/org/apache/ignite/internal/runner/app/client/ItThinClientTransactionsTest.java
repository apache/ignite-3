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

import static java.util.Comparator.comparing;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
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
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.apache.ignite.tx.TransactionOptions;
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
        assertEquals(3, recordView.getAll(tx, List.of(key, rec(2, null), rec(3, null))).size());

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
        assertNotNull(coord.txManager().stateMeta(txId), "Transaction expected to be collocated with enlistment");

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

        assertEquals(TxState.ABORTED, coord.txManager().stateMeta(txId).txState());
        assertEquals(TxState.ABORTED, other.txManager().stateMeta(txId).txState());

        tx2.commit();
    }

    private List<Tuple> generateKeysForNode(int start, int count, Map<Partition, ClusterNode> map, ClusterNode clusterNode) {
        List<Tuple> keys = new ArrayList<>();

        int k = start;
        while (keys.size() != count) {
            k++;
            Tuple t = key(k);

            Partition part = table().partitionManager().partitionAsync(t).join();
            ClusterNode node = map.get(part);

            if (node.name().equals(clusterNode.name())) {
                keys.add(t);
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

        List<Tuple> tuples0 = generateKeysForNode(200, 2, map, server0.clusterService().topologyService().localMember());

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

        List<Tuple> tuples0 = generateKeysForNode(300, 1, map, server0.clusterService().topologyService().localMember());
        List<Tuple> tuples1 = generateKeysForNode(310, 1, map, server1.clusterService().topologyService().localMember());

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

        List<Tuple> tuples0 = generateKeysForNode(400, 1, map, server0.clusterService().topologyService().localMember());
        List<Tuple> tuples1 = generateKeysForNode(410, 1, map, server1.clusterService().topologyService().localMember());

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

        List<Tuple> tuples0 = generateKeysForNode(500, 1, map, server0.clusterService().topologyService().localMember());
        List<Tuple> tuples1 = generateKeysForNode(510, 10, map, server1.clusterService().topologyService().localMember());

        Map<Tuple, Tuple> data = new HashMap<>();

        ClientLazyTransaction tx0 = (ClientLazyTransaction) client().transactions().begin();

        // First operation is collocated with txn coordinator and not directly mapped.
        Tuple k = tuples0.get(0);
        Tuple v = val(tuples0.get(0).intValue(0) + "");
        table.keyValueView().put(tx0, k, v);

        // All other operations are directly mapped.
        Tuple k1 = tuples1.get(0);
        Tuple v1 = val(tuples1.get(0).intValue(0) + "");

        Tuple k2 = tuples1.get(1);
        Tuple v2 = val(tuples1.get(1).intValue(0) + "");

        Map<Tuple, Tuple> batch0 = new HashMap<>();
        batch0.put(tuples1.get(2), val(tuples1.get(2).intValue(0) + ""));
        batch0.put(tuples1.get(3), val(tuples1.get(3).intValue(0) + ""));

        table.keyValueView().put(tx0, k1, v1);
        assertTrue(Tuple.equals(v1, table.keyValueView().get(tx0, k1)));
        assertTrue(table.keyValueView().putIfAbsent(tx0, k2, v2));

        tx0.commit();

        Mockito.verify(spyed, Mockito.times(2)).addInflight(tx0.startedTx().txId());
        Mockito.verify(spyed, Mockito.times(2)).removeInflight(Mockito.eq(tx0.startedTx().txId()), Mockito.any());

        for (Entry<Tuple, Tuple> entry : data.entrySet()) {
            table.keyValueView().put(null, entry.getKey(), entry.getValue());
        }
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
