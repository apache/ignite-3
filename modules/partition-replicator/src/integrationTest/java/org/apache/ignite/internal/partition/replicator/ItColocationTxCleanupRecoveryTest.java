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

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.bypassingThreadAssertions;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.partition.replicator.fixtures.Node;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.message.TxCleanupMessage;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

// TODO: remove after switching to per-zone partitions https://issues.apache.org/jira/browse/IGNITE-22522
class ItColocationTxCleanupRecoveryTest extends ItAbstractColocationTest {
    @Test
    void txGetsCleanedUpOnPrimaryChange() throws Exception {
        startCluster(3);

        Node node0 = cluster.get(0);

        // Create a zone with a single partition on every node.
        int zoneId = createZone(node0, TEST_ZONE_NAME, 1, cluster.size());

        createTable(node0, TEST_ZONE_NAME, TEST_TABLE_NAME1);

        cluster.forEach(Node::waitForMetadataCompletenessAtNow);

        ReplicaMeta primaryReplica = node0.getPrimaryReplica(zoneId);
        Node primaryReplicaNode = cluster.stream()
                .filter(node -> node.name.equals(primaryReplica.getLeaseholder()))
                .findAny().orElseThrow();

        // We'll use this node as transaction coordinator, so let's forbid it sending cleanup messages.
        disallowTxCleanupMessagesFrom(primaryReplicaNode);

        KeyValueView<Long, Integer> kvView = node0.tableManager
                .table(TEST_TABLE_NAME1)
                .keyValueView(Long.class, Integer.class);

        Transaction transaction = primaryReplicaNode.transactions().begin();

        kvView.put(transaction, 42L, 69);
        waitTillOneWriteIntentAppearsOnAllNodes(TEST_TABLE_NAME1);
        transaction.rollbackAsync();

        waitTillTxStateAppearsOnAllNodes(zoneId);

        primaryReplicaNode.stop();
        cluster.remove(primaryReplicaNode);

        waitTillAllWriteIntentsGetRemovedOnAllNodes(TEST_TABLE_NAME1);
    }

    private static void disallowTxCleanupMessagesFrom(Node primaryReplicaNode) {
        primaryReplicaNode.dropMessages((destinationName, message) -> message instanceof TxCleanupMessage);
    }

    private void waitTillOneWriteIntentAppearsOnAllNodes(String tableName) throws InterruptedException {
        waitOnAllNodes("A write intent should appear on every node", tableName, storage -> {
            List<ReadResult> readResults = readAll(storage);
            return readResults.size() == 1 && readResults.stream().allMatch(ReadResult::isWriteIntent);
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

    private void waitTillTxStateAppearsOnAllNodes(int zoneId) throws InterruptedException {
        for (Node node : cluster) {
            TxStatePartitionStorage txStatePartitionStorage = node.txStatePartitionStorage(zoneId, 0);

            assertTrue(
                    waitForCondition(() -> !isEmpty(txStatePartitionStorage), SECONDS.toMillis(10)),
                    "Did not see any tx states on node " + node.name + " in time"
            );
        }
    }

    private static boolean isEmpty(TxStatePartitionStorage txStatePartitionStorage) {
        return bypassingThreadAssertions(() -> {
            try (Cursor<IgniteBiTuple<UUID, TxMeta>> cursor = txStatePartitionStorage.scan()) {
                return !cursor.hasNext();
            }
        });
    }
}
