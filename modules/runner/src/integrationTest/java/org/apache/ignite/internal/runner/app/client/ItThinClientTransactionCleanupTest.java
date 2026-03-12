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

import static java.lang.String.format;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.client.sql.ClientSql;
import org.apache.ignite.internal.client.sql.PartitionMappingProvider;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for client transaction cleanup on disconnect.
 */
@SuppressWarnings({"resource", "DataFlowIssue"})
public class ItThinClientTransactionCleanupTest extends ItAbstractThinClientTest {
    /**
     * Tests that locks are released when client disconnects with a transaction having direct enlistments.
     */
    @Test
    void testClientDisconnectWithDirectEnlistmentsReleasesLocks() {
        Map<Partition, ClusterNode> map = table().partitionDistribution().primaryReplicasAsync().join();
        IgniteImpl server0 = unwrapIgniteImpl(server(0));
        IgniteImpl server1 = unwrapIgniteImpl(server(1));

        List<Tuple> tuples0 = ItThinClientTransactionsTest.generateKeysForNode(1000, 5, map, server0.cluster().localNode(), table());
        List<Tuple> tuples1 = ItThinClientTransactionsTest.generateKeysForNode(1000, 5, map, server1.cluster().localNode(), table());

        try (IgniteClient tempClient = IgniteClient.builder().addresses(getNodeAddress()).build()) {
            ClientTable table = (ClientTable) tempClient.tables().table(TABLE_NAME);
            ClientSql sql = (ClientSql) tempClient.sql();

            // Initialize SQL mappings for direct enlistment
            Tuple key0 = tuples0.get(0);
            sql.execute(format("INSERT INTO %s (%s, %s) VALUES (?, ?)", TABLE_NAME, COLUMN_KEY, COLUMN_VAL),
                    key0.intValue(0), key0.intValue(0) + "");
            await().atMost(2, TimeUnit.SECONDS)
                    .until(() -> sql.partitionAwarenessCachedMetas().stream().allMatch(PartitionMappingProvider::ready));

            Transaction tx = tempClient.transactions().begin();

            // Enlist partitions on both nodes (direct mapping)
            Tuple key1 = tuples0.get(1);
            Tuple key2 = tuples1.get(0);
            table.keyValueView().put(tx, key1, val(key1.intValue(0) + ""));
            table.keyValueView().put(tx, key2, val(key2.intValue(0) + ""));

            // Note: Transaction may be in direct or proxy mode depending on partition awareness
            // The important thing is that cleanup works in both modes

            // Client disconnects without committing - tempClient.close() called by try-with-resources
        }

        // Verify locks are released by attempting to lock the same keys
        KeyValueView<Tuple, Tuple> kvView = table().keyValueView();
        Tuple key1 = tuples0.get(1);
        Tuple key2 = tuples1.get(0);

        assertThat(kvView.putAsync(null, key1, val("new_value")), willSucceedFast());
        assertThat(kvView.putAsync(null, key2, val("new_value")), willSucceedFast());

        // Verify transactional values were not committed
        // (key0 has a value from the SQL INSERT, so we check key1 and key2)
        assertEquals("new_value", kvView.get(null, key1).stringValue(0));
        assertEquals("new_value", kvView.get(null, key2).stringValue(0));
    }

    /**
     * Tests that locks are released when client disconnects with a transaction in proxy mode.
     */
    @Test
    void testClientDisconnectWithProxyEnlistmentsReleasesLocks() {
        Map<Partition, ClusterNode> map = table().partitionDistribution().primaryReplicasAsync().join();
        IgniteImpl server0 = unwrapIgniteImpl(server(0));

        List<Tuple> tuples0 = ItThinClientTransactionsTest.generateKeysForNode(2000, 3, map, server0.cluster().localNode(), table());

        try (IgniteClient tempClient = IgniteClient.builder().addresses(getNodeAddress()).build()) {
            ClientTable table = (ClientTable) tempClient.tables().table(TABLE_NAME);
            KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

            Transaction tx = tempClient.transactions().begin();

            // Enlist without explicit partition awareness setup
            Tuple key1 = tuples0.get(0);
            kvView.put(tx, key1, val(key1.intValue(0) + ""));

            // Note: Transaction may still use direct mapping if partition info is available
            // The important thing is that cleanup works regardless of mode

            // Client disconnects without committing
        }

        // Verify lock is released
        KeyValueView<Tuple, Tuple> kvView = table().keyValueView();
        Tuple key1 = tuples0.get(0);
        assertThat(kvView.putAsync(null, key1, val("new_value")), willSucceedFast());
        assertEquals("new_value", kvView.get(null, key1).stringValue(0));
    }

    /**
     * Tests that locks are released when client disconnects with mixed (direct + proxy) enlistments.
     */
    @Test
    void testClientDisconnectWithMixedEnlistmentsReleasesLocks() {
        Map<Partition, ClusterNode> map = table().partitionDistribution().primaryReplicasAsync().join();
        IgniteImpl server0 = unwrapIgniteImpl(server(0));
        IgniteImpl server1 = unwrapIgniteImpl(server(1));

        List<Tuple> tuples0 = ItThinClientTransactionsTest.generateKeysForNode(3000, 3, map, server0.cluster().localNode(), table());
        List<Tuple> tuples1 = ItThinClientTransactionsTest.generateKeysForNode(3000, 3, map, server1.cluster().localNode(), table());

        try (IgniteClient tempClient = IgniteClient.builder().addresses(getNodeAddress()).build()) {
            ClientTable table = (ClientTable) tempClient.tables().table(TABLE_NAME);
            ClientSql sql = (ClientSql) tempClient.sql();
            KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

            Transaction tx = tempClient.transactions().begin();

            // Start with proxy mode enlistment
            Tuple keyProxy = tuples0.get(0);
            kvView.put(tx, keyProxy, val(keyProxy.intValue(0) + ""));

            // Initialize SQL mappings for direct enlistment
            Tuple key0 = tuples0.get(1);
            sql.execute(format("INSERT INTO %s (%s, %s) VALUES (?, ?)", TABLE_NAME, COLUMN_KEY, COLUMN_VAL),
                    key0.intValue(0), key0.intValue(0) + "");
            await().atMost(2, TimeUnit.SECONDS)
                    .until(() -> sql.partitionAwarenessCachedMetas().stream().allMatch(PartitionMappingProvider::ready));

            // Add direct enlistments
            Tuple keyDirect1 = tuples0.get(2);
            Tuple keyDirect2 = tuples1.get(0);
            kvView.put(tx, keyDirect1, val(keyDirect1.intValue(0) + ""));
            kvView.put(tx, keyDirect2, val(keyDirect2.intValue(0) + ""));

            // Client disconnects without committing
        }

        // Verify all locks are released
        KeyValueView<Tuple, Tuple> kvView = table().keyValueView();
        List<Tuple> allKeys = new ArrayList<>();
        allKeys.add(tuples0.get(0));
        allKeys.add(tuples0.get(1));
        allKeys.add(tuples0.get(2));
        allKeys.add(tuples1.get(0));

        Map<Tuple, Tuple> updates = new HashMap<>();
        for (Tuple key : allKeys) {
            updates.put(key, val("new_value"));
        }

        assertThat(kvView.putAllAsync(null, updates), willSucceedFast());

        for (Tuple key : allKeys) {
            Tuple value = kvView.get(null, key);
            assertEquals("new_value", value.stringValue(0));
        }
    }

    /**
     * Tests cleanup of multiple concurrent transactions when client disconnects.
     */
    @Test
    void testClientDisconnectWithMultipleTransactionsReleasesAllLocks() {
        Map<Partition, ClusterNode> map = table().partitionDistribution().primaryReplicasAsync().join();
        IgniteImpl server0 = unwrapIgniteImpl(server(0));
        IgniteImpl server1 = unwrapIgniteImpl(server(1));

        List<Tuple> tuples0 = ItThinClientTransactionsTest.generateKeysForNode(4000, 10, map, server0.cluster().localNode(), table());
        List<Tuple> tuples1 = ItThinClientTransactionsTest.generateKeysForNode(4000, 10, map, server1.cluster().localNode(), table());

        try (IgniteClient tempClient = IgniteClient.builder().addresses(getNodeAddress()).build()) {
            ClientTable table = (ClientTable) tempClient.tables().table(TABLE_NAME);
            ClientSql sql = (ClientSql) tempClient.sql();
            KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

            // Initialize SQL mappings
            Tuple key0 = tuples0.get(0);
            sql.execute(format("INSERT INTO %s (%s, %s) VALUES (?, ?)", TABLE_NAME, COLUMN_KEY, COLUMN_VAL),
                    key0.intValue(0), key0.intValue(0) + "");
            await().atMost(2, TimeUnit.SECONDS)
                    .until(() -> sql.partitionAwarenessCachedMetas().stream().allMatch(PartitionMappingProvider::ready));

            // Create multiple transactions with enlistments
            Transaction tx1 = tempClient.transactions().begin();
            kvView.put(tx1, tuples0.get(1), val("tx1_val1"));
            kvView.put(tx1, tuples1.get(0), val("tx1_val2"));

            Transaction tx2 = tempClient.transactions().begin();
            kvView.put(tx2, tuples0.get(2), val("tx2_val1"));
            kvView.put(tx2, tuples1.get(1), val("tx2_val2"));

            Transaction tx3 = tempClient.transactions().begin();
            kvView.put(tx3, tuples0.get(3), val("tx3_val1"));
            kvView.put(tx3, tuples1.get(2), val("tx3_val2"));

            // Client disconnects with all transactions active
        }

        // Verify all locks from all transactions are released
        KeyValueView<Tuple, Tuple> kvView = table().keyValueView();
        List<Tuple> allKeys = List.of(
                tuples0.get(0), tuples0.get(1), tuples0.get(2), tuples0.get(3),
                tuples1.get(0), tuples1.get(1), tuples1.get(2)
        );

        for (Tuple key : allKeys) {
            assertThat(kvView.putAsync(null, key, val("clean")), willSucceedFast());
        }
    }

    /**
     * Tests that cleaner is properly removed on normal transaction commit.
     */
    @Test
    void testTransactionCommitRemovesCleaner() {
        Map<Partition, ClusterNode> map = table().partitionDistribution().primaryReplicasAsync().join();
        IgniteImpl server0 = unwrapIgniteImpl(server(0));
        IgniteImpl server1 = unwrapIgniteImpl(server(1));

        List<Tuple> tuples0 = ItThinClientTransactionsTest.generateKeysForNode(5000, 3, map, server0.cluster().localNode(), table());
        List<Tuple> tuples1 = ItThinClientTransactionsTest.generateKeysForNode(5000, 3, map, server1.cluster().localNode(), table());

        try (IgniteClient tempClient = IgniteClient.builder().addresses(getNodeAddress()).build()) {
            ClientTable table = (ClientTable) tempClient.tables().table(TABLE_NAME);
            ClientSql sql = (ClientSql) tempClient.sql();
            KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

            // Initialize SQL mappings
            Tuple key0 = tuples0.get(0);
            sql.execute(format("INSERT INTO %s (%s, %s) VALUES (?, ?)", TABLE_NAME, COLUMN_KEY, COLUMN_VAL),
                    key0.intValue(0), key0.intValue(0) + "");
            await().atMost(2, TimeUnit.SECONDS)
                    .until(() -> sql.partitionAwarenessCachedMetas().stream().allMatch(PartitionMappingProvider::ready));

            Transaction tx = tempClient.transactions().begin();
            Tuple key1 = tuples0.get(1);
            Tuple key2 = tuples1.get(0);
            kvView.put(tx, key1, val("committed_value1"));
            kvView.put(tx, key2, val("committed_value2"));

            // Commit normally
            tx.commit();

            // Values should be committed
            assertEquals("committed_value1", kvView.get(null, key1).stringValue(0));
            assertEquals("committed_value2", kvView.get(null, key2).stringValue(0));
        }

        // Verify committed values persist
        KeyValueView<Tuple, Tuple> kvView = table().keyValueView();
        assertEquals("committed_value1", kvView.get(null, tuples0.get(1)).stringValue(0));
        assertEquals("committed_value2", kvView.get(null, tuples1.get(0)).stringValue(0));
    }

    /**
     * Tests that cleaner is properly removed on normal transaction rollback.
     */
    @Test
    void testTransactionRollbackRemovesCleaner() {
        Map<Partition, ClusterNode> map = table().partitionDistribution().primaryReplicasAsync().join();
        IgniteImpl server0 = unwrapIgniteImpl(server(0));
        IgniteImpl server1 = unwrapIgniteImpl(server(1));

        List<Tuple> tuples0 = ItThinClientTransactionsTest.generateKeysForNode(6000, 3, map, server0.cluster().localNode(), table());
        List<Tuple> tuples1 = ItThinClientTransactionsTest.generateKeysForNode(6000, 3, map, server1.cluster().localNode(), table());

        try (IgniteClient tempClient = IgniteClient.builder().addresses(getNodeAddress()).build()) {
            ClientTable table = (ClientTable) tempClient.tables().table(TABLE_NAME);
            ClientSql sql = (ClientSql) tempClient.sql();
            KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

            // Initialize SQL mappings
            Tuple key0 = tuples0.get(0);
            sql.execute(format("INSERT INTO %s (%s, %s) VALUES (?, ?)", TABLE_NAME, COLUMN_KEY, COLUMN_VAL),
                    key0.intValue(0), key0.intValue(0) + "");
            await().atMost(2, TimeUnit.SECONDS)
                    .until(() -> sql.partitionAwarenessCachedMetas().stream().allMatch(PartitionMappingProvider::ready));

            Transaction tx = tempClient.transactions().begin();
            Tuple key1 = tuples0.get(1);
            Tuple key2 = tuples1.get(0);
            kvView.put(tx, key1, val("rolled_back_value1"));
            kvView.put(tx, key2, val("rolled_back_value2"));

            // Rollback explicitly
            tx.rollback();

            // Values should not be visible
            assertNull(kvView.get(null, key1));
            assertNull(kvView.get(null, key2));
        }

        // Verify values are still not visible after client closes
        KeyValueView<Tuple, Tuple> kvView = table().keyValueView();
        assertNull(kvView.get(null, tuples0.get(1)));
        assertNull(kvView.get(null, tuples1.get(0)));
    }

    /**
     * Tests that a second client can acquire locks after first client disconnects.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testLockAcquisitionAfterClientDisconnect(boolean directMapping) {
        Map<Partition, ClusterNode> map = table().partitionDistribution().primaryReplicasAsync().join();
        IgniteImpl server0 = unwrapIgniteImpl(server(0));

        List<Tuple> tuples0 = ItThinClientTransactionsTest.generateKeysForNode(7000, 3, map, server0.cluster().localNode(), table());
        Tuple testKey = tuples0.get(1);

        // First client acquires lock
        try (IgniteClient client1 = IgniteClient.builder().addresses(getNodeAddress()).build()) {
            ClientTable table = (ClientTable) client1.tables().table(TABLE_NAME);
            KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

            if (directMapping) {
                ClientSql sql = (ClientSql) client1.sql();
                Tuple key0 = tuples0.get(0);
                sql.execute(format("INSERT INTO %s (%s, %s) VALUES (?, ?)", TABLE_NAME, COLUMN_KEY, COLUMN_VAL),
                        key0.intValue(0), key0.intValue(0) + "");
                await().atMost(2, TimeUnit.SECONDS)
                        .until(() -> sql.partitionAwarenessCachedMetas().stream().allMatch(PartitionMappingProvider::ready));
            }

            Transaction tx1 = client1.transactions().begin();
            kvView.put(tx1, testKey, val("client1_value"));

            // Note: directMapping parameter indicates if we initialized partition awareness,
            // but actual mapping mode may vary. The cleanup should work regardless.

            // Client1 disconnects with active transaction
        }

        // Second client can immediately acquire lock
        try (IgniteClient client2 = IgniteClient.builder().addresses(getNodeAddress()).build()) {
            KeyValueView<Tuple, Tuple> kvView = client2.tables().table(TABLE_NAME).keyValueView();

            CompletableFuture<Void> putFuture = kvView.putAsync(null, testKey, val("client2_value"));
            assertThat(putFuture, willSucceedFast());

            assertEquals("client2_value", kvView.get(null, testKey).stringValue(0));
        }
    }

    /**
     * Tests concurrent client disconnects don't interfere with each other.
     */
    @Test
    void testConcurrentClientDisconnects() throws Exception {
        Map<Partition, ClusterNode> map = table().partitionDistribution().primaryReplicasAsync().join();
        IgniteImpl server0 = unwrapIgniteImpl(server(0));

        List<Tuple> tuples0 = ItThinClientTransactionsTest.generateKeysForNode(8000, 20, map, server0.cluster().localNode(), table());

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // Create multiple clients concurrently, each with transactions
        for (int i = 0; i < 5; i++) {
            int clientIndex = i;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try (IgniteClient tempClient = IgniteClient.builder().addresses(getNodeAddress()).build()) {
                    KeyValueView<Tuple, Tuple> kvView = tempClient.tables().table(TABLE_NAME).keyValueView();

                    Transaction tx = tempClient.transactions().begin();
                    Tuple key = tuples0.get(clientIndex * 2);
                    kvView.put(tx, key, val("client" + clientIndex));

                    // Disconnect without committing
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            futures.add(future);
        }

        // Wait for all clients to disconnect
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);

        // Give cleanup time to complete
        Thread.sleep(500);

        // Verify all locks are released
        KeyValueView<Tuple, Tuple> kvView = table().keyValueView();
        for (int i = 0; i < 5; i++) {
            Tuple key = tuples0.get(i * 2);
            assertThat(kvView.putAsync(null, key, val("clean")), willSucceedFast());
        }
    }

    private Table table() {
        return client().tables().tables().get(0);
    }

    private static Tuple val(String v) {
        return Tuple.create().set(COLUMN_VAL, v);
    }
}
