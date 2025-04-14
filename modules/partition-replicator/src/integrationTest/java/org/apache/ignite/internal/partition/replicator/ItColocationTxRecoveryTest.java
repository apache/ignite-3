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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.partition.replicator.fixtures.Node;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

// TODO: remove after switching to per-zone partitions https://issues.apache.org/jira/browse/IGNITE-22522
class ItColocationTxRecoveryTest extends ItAbstractColocationTest {
    private static final long KEY = 1;

    /**
     * Tests that tx recovery works. Scenario:
     *
     * <ol>
     *     <li>A transaction tx1 is started, it takes a shared lock on a key and never gets finished</li>
     *     <li>Its coordinator (different from the node hosting the touched partition primary) is stopped, so the transaction becomes
     *     abandoned</li>
     *     <li>Transaction tx2 tries to write to the same key, founds an incompatible lock, realizes that it's held by an abandoned
     *     transaction, and does tx recovery to remove the lock on the partition primary</li>
     *     <li>tx2 should succeed</li>
     * </ol>
     */
    @Test
    void abandonedTransactionGetsAbortedOnTouch(
            @InjectConfiguration("mock.properties.txnAbandonedCheckTs=\"600000\"") SystemDistributedConfiguration systemCfg)
            throws Exception {
        this.systemDistributedConfiguration = systemCfg;

        startCluster(3);

        Node node0 = getNode(0);

        // Create a zone with a single partition on every node.
        int zoneId = createZone(node0, TEST_ZONE_NAME, 1, cluster.size());

        createTable(node0, TEST_ZONE_NAME, TEST_TABLE_NAME1);

        cluster.forEach(Node::waitForMetadataCompletenessAtNow);

        putInitialValue(node0);

        ReplicaMeta primaryReplica = node0.getPrimaryReplica(zoneId);

        Node coordinatorNodeToBeStopped = findAnyOtherNode(primaryReplica);
        Transaction txToBeAbandoned = coordinatorNodeToBeStopped.transactions().begin();
        // Trigger a shared lock to be taken on the key.
        coordinatorNodeToBeStopped.tableManager.table(TEST_TABLE_NAME1)
                .keyValueView(Long.class, Integer.class)
                .get(txToBeAbandoned, KEY);

        coordinatorNodeToBeStopped.stop();
        cluster.remove(coordinatorNodeToBeStopped);

        Node runningNode = cluster.get(0);

        KeyValueView<Long, Integer> kvView = runningNode.tableManager.table(TEST_TABLE_NAME1).keyValueView(Long.class, Integer.class);

        Transaction conflictingTx = runningNode.transactions().begin();
        assertDoesNotThrow(() -> kvView.put(conflictingTx, KEY, 111));
    }

    private static void putInitialValue(Node node) {
        node.tableManager
                .table(TEST_TABLE_NAME1)
                .keyValueView(Long.class, Integer.class)
                .put(null, KEY, 42);
    }

    private Node findAnyOtherNode(ReplicaMeta primaryReplica) {
        return cluster.stream()
                .filter(node -> !node.name.equals(primaryReplica.getLeaseholder()))
                .findAny()
                .orElseThrow();
    }
}
