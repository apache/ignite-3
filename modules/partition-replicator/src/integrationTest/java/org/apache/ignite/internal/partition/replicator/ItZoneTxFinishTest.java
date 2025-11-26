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

package org.apache.ignite.internal.partition.replicator;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.partition.replicator.fixtures.Node;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

// TODO: remove after switching to per-zone partitions https://issues.apache.org/jira/browse/IGNITE-22522
class ItZoneTxFinishTest extends ItAbstractColocationTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void txFinishCommandGetsReplicated(boolean commit) throws Exception {
        startCluster(3);

        Node node0 = getNode(0);

        // Create a zone with a single partition on every node.
        int zoneId = createZone(node0, TEST_ZONE_NAME, 1, cluster.size());

        createTable(node0, TEST_ZONE_NAME, TEST_TABLE_NAME1);
        createTable(node0, TEST_ZONE_NAME, TEST_TABLE_NAME2);

        cluster.forEach(Node::waitForMetadataCompletenessAtNow);

        Node node = cluster.get(0);

        KeyValueView<Long, Integer> kvView1 = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Long.class, Integer.class);
        KeyValueView<Long, Integer> kvView2 = node.tableManager.table(TEST_TABLE_NAME2).keyValueView(Long.class, Integer.class);

        Transaction transaction = node.transactions().begin();
        kvView1.put(transaction, 42L, 69);
        kvView2.put(transaction, 142L, 169);
        if (commit) {
            transaction.commit();
        } else {
            transaction.rollback();
        }

        for (Node currentNode : cluster) {
            assertTrue(waitForCondition(() -> {
                TxStatePartitionStorage txStorage = currentNode.txStatePartitionStorage(zoneId, 0);

                return txStorage != null && !txStatesInPartitionStorage(txStorage).isEmpty();
            }, SECONDS.toMillis(10)));
        }

        List<Executable> assertions = new ArrayList<>();
        for (int i = 0; i < cluster.size(); i++) {
            int finalI = i;
            Node currentNode = cluster.get(finalI);

            assertions.add(() -> assertTxStateStorageAsExpected(
                    "Node " + finalI + " zone",
                    requireNonNull(currentNode.txStatePartitionStorage(zoneId, 0)),
                    1,
                    commit
            ));
        }

        assertAll(assertions);
    }

    private static void assertTxStateStorageAsExpected(
            String storageName,
            TxStatePartitionStorage txStatePartitionStorage,
            int expectedCount,
            boolean commit
    ) {
        List<TxState> txStates = txStatesInPartitionStorage(txStatePartitionStorage);

        assertThat("For " + storageName, txStates, hasSize(expectedCount));
        assertThat(txStates, everyItem(is(commit ? TxState.COMMITTED : TxState.ABORTED)));
    }

    private static List<TxState> txStatesInPartitionStorage(TxStatePartitionStorage txStatePartitionStorage) {
        return IgniteTestUtils.bypassingThreadAssertions(() -> {
            try (Cursor<IgniteBiTuple<UUID, TxMeta>> cursor = txStatePartitionStorage.scan()) {
                return cursor.stream()
                        .map(pair -> pair.get2().txState())
                        .collect(toList());
            }
        });
    }

    @ParameterizedTest(name = "commit={0}")
    @ValueSource(booleans = {false, true})
    void writeIntentSwitchGetsReplicated(boolean commit) throws Exception {
        startCluster(3);

        Node node0 = getNode(0);

        // Create a zone with a single partition on every node.
        createZone(node0, TEST_ZONE_NAME, 1, cluster.size());

        createTable(node0, TEST_ZONE_NAME, TEST_TABLE_NAME1);
        createTable(node0, TEST_ZONE_NAME, TEST_TABLE_NAME2);

        cluster.forEach(Node::waitForMetadataCompletenessAtNow);

        Node node = cluster.get(0);

        KeyValueView<Long, Integer> kvView1 = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Long.class, Integer.class);
        KeyValueView<Long, Integer> kvView2 = node.tableManager.table(TEST_TABLE_NAME2).keyValueView(Long.class, Integer.class);

        Transaction transaction = node.transactions().begin();

        kvView1.put(transaction, 42L, 69);
        waitTillOneWriteIntentAppearsOnAllNodes(TEST_TABLE_NAME1);

        kvView2.put(transaction, 142L, 169);
        waitTillOneWriteIntentAppearsOnAllNodes(TEST_TABLE_NAME2);

        if (commit) {
            transaction.commit();

            waitTillAllWriteIntentsSwitchOnAllNodes(TEST_TABLE_NAME1);
            waitTillAllWriteIntentsSwitchOnAllNodes(TEST_TABLE_NAME2);
        } else {
            transaction.rollback();

            waitTillAllWriteIntentsGetRemovedOnAllNodes(TEST_TABLE_NAME1);
            waitTillAllWriteIntentsGetRemovedOnAllNodes(TEST_TABLE_NAME2);
        }
    }

    private void waitTillOneWriteIntentAppearsOnAllNodes(String tableName) throws InterruptedException {
        waitOnAllNodes("A write intent should appear on every node", tableName, storage -> {
            List<ReadResult> readResults = readAll(storage);
            return readResults.size() == 1 && readResults.stream().allMatch(ReadResult::isWriteIntent);
        });
    }

    private void waitTillAllWriteIntentsSwitchOnAllNodes(String tableName) throws InterruptedException {
        waitOnAllNodes("All write intents should turn into committed values", tableName, storage -> {
            List<ReadResult> readResults = readAll(storage);
            return !readResults.isEmpty() && readResults.stream().noneMatch(ReadResult::isWriteIntent);
        });
    }

    private void waitTillAllWriteIntentsGetRemovedOnAllNodes(String tableName) throws InterruptedException {
        waitOnAllNodes("Write intents should be removed from all nodes", tableName, storage -> readAll(storage).isEmpty());
    }

    private void waitOnAllNodes(String expectation, String tableName, Predicate<MvPartitionStorage> storageTest)
            throws InterruptedException {
        for (Node node : cluster) {
            InternalTable internalTable = unwrapTableImpl(node.tableManager.table(tableName)).internalTable();
            MvPartitionStorage storage = internalTable.storage().getMvPartition(0);
            assertNotNull(storage);

            assertTrue(
                    waitForCondition(() -> storageTest.test(storage), SECONDS.toMillis(10)),
                    expectation
            );
        }
    }

    private static List<ReadResult> readAll(MvPartitionStorage storage) {
        try (PartitionTimestampCursor cursor = storage.scan(HybridTimestamp.MAX_VALUE)) {
            return cursor.stream().collect(toList());
        }
    }

    @Test
    void zoneIdIsWrittenAsCommitZoneIdToWriteIntents() throws Exception {
        startCluster(3);

        Node node0 = getNode(0);

        // Create a zone with a single partition on every node.
        int zoneId = createZone(node0, TEST_ZONE_NAME, 1, cluster.size());

        createTable(node0, TEST_ZONE_NAME, TEST_TABLE_NAME1);
        createTable(node0, TEST_ZONE_NAME, TEST_TABLE_NAME2);

        cluster.forEach(Node::waitForMetadataCompletenessAtNow);

        Node node = cluster.get(0);

        KeyValueView<Long, Integer> kvView1 = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Long.class, Integer.class);
        KeyValueView<Long, Integer> kvView2 = node.tableManager.table(TEST_TABLE_NAME2).keyValueView(Long.class, Integer.class);

        Transaction transaction = node.transactions().begin();

        kvView1.put(transaction, 42L, 69);
        waitTillOneWriteIntentAppearsOnAllNodesWithCommitZoneId(TEST_TABLE_NAME1, zoneId);

        kvView2.put(transaction, 142L, 169);
        waitTillOneWriteIntentAppearsOnAllNodesWithCommitZoneId(TEST_TABLE_NAME2, zoneId);
    }

    private void waitTillOneWriteIntentAppearsOnAllNodesWithCommitZoneId(String tableName, int commitZoneId) throws InterruptedException {
        waitOnAllNodes("A write intent should appear on every node with expected commitZoneId", tableName, storage -> {
            List<ReadResult> readResults = readAll(storage);
            return readResults.size() == 1
                    && readResults.stream().allMatch(version -> Objects.equals(version.commitZoneId(), commitZoneId));
        });
    }
}
