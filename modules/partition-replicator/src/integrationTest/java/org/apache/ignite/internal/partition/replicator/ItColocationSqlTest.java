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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.util.function.Consumer;
import org.apache.ignite.internal.partition.replicator.fixtures.Node;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Class containing tests related to Raft-based replication for the Colocation feature.
 */
public class ItColocationSqlTest extends AbstractZoneReplicationTest {
    @ParameterizedTest(name = "useTx={0}")
    @ValueSource(booleans = {true, false})
    void sqlDmlsWork(boolean useTx) throws Exception {
        startCluster(3);

        // Create a zone with a single partition on every node.
        int zoneId = createZone(TEST_ZONE_NAME, 1, cluster.size());

        createTable(TEST_ZONE_NAME, TEST_TABLE_NAME1);
        createTable(TEST_ZONE_NAME, TEST_TABLE_NAME2);

        var zonePartitionId = new ZonePartitionId(zoneId, 0);

        cluster.forEach(Node::waitForMetadataCompletenessAtNow);

        Node node = cluster.get(0);

        KeyValueView<Integer, Integer> kvView1 = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Integer.class, Integer.class);
        KeyValueView<Integer, Integer> kvView2 = node.tableManager.table(TEST_TABLE_NAME2).keyValueView(Integer.class, Integer.class);

        // Inserts.
        setPrimaryReplica(node, zonePartitionId);

        Consumer<Transaction> sqlInserts = tx -> {
            node.sql().execute(tx, "INSERT INTO " + TEST_TABLE_NAME1 + " (KEY, VAL) VALUES (1, 11), (2, 22)");
            node.sql().execute(tx, "INSERT INTO " + TEST_TABLE_NAME2 + " (KEY, VAL) VALUES (3, 33), (4, 44)");
        };

        if (useTx) {
            node.transactions().runInTransaction(sqlInserts);
        } else {
            sqlInserts.accept(null);
        }

        for (Node n : cluster) {
            setPrimaryReplica(n, zonePartitionId);

            assertThat(n.name, kvView1.get(null, 1), is(11));
            assertThat(n.name, kvView1.get(null, 2), is(22));

            assertThat(n.name, kvView2.get(null, 3), is(33));
            assertThat(n.name, kvView2.get(null, 4), is(44));
        }

        // Updates.
        setPrimaryReplica(node, zonePartitionId);

        Consumer<Transaction> sqlUpdates = tx -> {
            node.sql().execute(tx, "UPDATE " + TEST_TABLE_NAME1 + " SET VAL = -VAL");
            node.sql().execute(tx, "UPDATE " + TEST_TABLE_NAME2 + " SET VAL = -VAL");
        };

        if (useTx) {
            node.transactions().runInTransaction(sqlUpdates);
        } else {
            sqlUpdates.accept(null);
        }

        for (Node n : cluster) {
            setPrimaryReplica(n, zonePartitionId);

            assertThat(n.name, kvView1.get(null, 1), is(-11));
            assertThat(n.name, kvView1.get(null, 2), is(-22));

            assertThat(n.name, kvView2.get(null, 3), is(-33));
            assertThat(n.name, kvView2.get(null, 4), is(-44));
        }

        // Deletes.
        setPrimaryReplica(node, zonePartitionId);

        Consumer<Transaction> sqlDeletes = tx -> {
            node.sql().execute(tx, "DELETE FROM " + TEST_TABLE_NAME1);
            node.sql().execute(tx, "DELETE FROM " + TEST_TABLE_NAME2);
        };

        if (useTx) {
            node.transactions().runInTransaction(sqlDeletes);
        } else {
            sqlDeletes.accept(null);
        }

        for (Node n : cluster) {
            setPrimaryReplica(n, zonePartitionId);

            assertThat(n.name, kvView1.get(null, 1), is(nullValue()));
            assertThat(n.name, kvView1.get(null, 2), is(nullValue()));

            assertThat(n.name, kvView2.get(null, 3), is(nullValue()));
            assertThat(n.name, kvView2.get(null, 4), is(nullValue()));
        }
    }
}
