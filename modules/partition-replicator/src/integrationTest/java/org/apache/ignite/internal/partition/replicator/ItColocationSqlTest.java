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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.internal.partition.replicator.fixtures.Node;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Class containing tests related to Raft-based replication for the Colocation feature.
 */
class ItColocationSqlTest extends ItAbstractColocationTest {
    @ParameterizedTest(name = "useTx={0}")
    @ValueSource(booleans = {true, false})
    void sqlDmlsWork(boolean useTx) throws Exception {
        startCluster(3);

        Node node = getNode(0);

        // Create a zone with a single partition on every node.
        createZone(node, TEST_ZONE_NAME, 1, cluster.size());

        createTable(node, TEST_ZONE_NAME, TEST_TABLE_NAME1);
        createTable(node, TEST_ZONE_NAME, TEST_TABLE_NAME2);

        cluster.forEach(Node::waitForMetadataCompletenessAtNow);

        KeyValueView<Long, Integer> kvView1 = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Long.class, Integer.class);
        KeyValueView<Long, Integer> kvView2 = node.tableManager.table(TEST_TABLE_NAME2).keyValueView(Long.class, Integer.class);

        // Inserts.
        Consumer<Transaction> sqlInserts = tx -> {
            node.sql().execute(tx, "INSERT INTO " + TEST_TABLE_NAME1 + " (KEY, VAL) VALUES (1, 11), (2, 22)").close();
            node.sql().execute(tx, "INSERT INTO " + TEST_TABLE_NAME2 + " (KEY, VAL) VALUES (3, 33), (4, 44)").close();
        };

        if (useTx) {
            node.transactions().runInTransaction(sqlInserts);
        } else {
            sqlInserts.accept(null);
        }

        assertThat(kvView1.get(null, 1L), is(11));
        assertThat(kvView1.get(null, 2L), is(22));

        assertThat(kvView2.get(null, 3L), is(33));
        assertThat(kvView2.get(null, 4L), is(44));

        // Updates.
        Consumer<Transaction> sqlUpdates = tx -> {
            node.sql().execute(tx, "UPDATE " + TEST_TABLE_NAME1 + " SET VAL = -VAL").close();
            node.sql().execute(tx, "UPDATE " + TEST_TABLE_NAME2 + " SET VAL = -VAL").close();
        };

        if (useTx) {
            node.transactions().runInTransaction(sqlUpdates);
        } else {
            sqlUpdates.accept(null);
        }

        assertThat(kvView1.get(null, 1L), is(-11));
        assertThat(kvView1.get(null, 2L), is(-22));

        assertThat(kvView2.get(null, 3L), is(-33));
        assertThat(kvView2.get(null, 4L), is(-44));

        // Deletes.
        Consumer<Transaction> sqlDeletes = tx -> {
            node.sql().execute(tx, "DELETE FROM " + TEST_TABLE_NAME1).close();
            node.sql().execute(tx, "DELETE FROM " + TEST_TABLE_NAME2).close();
        };

        if (useTx) {
            node.transactions().runInTransaction(sqlDeletes);
        } else {
            sqlDeletes.accept(null);
        }

        assertThat(kvView1.get(null, 1L), is(nullValue()));
        assertThat(kvView1.get(null, 2L), is(nullValue()));

        assertThat(kvView2.get(null, 3L), is(nullValue()));
        assertThat(kvView2.get(null, 4L), is(nullValue()));
    }

    @ParameterizedTest(name = "readOnly={0}")
    @ValueSource(booleans = {true, false})
    void sqlSelectsWork(boolean readOnly) throws Exception {
        startCluster(3);

        Node node = getNode(0);

        // Create a zone with a single partition on every node.
        createZone(node, TEST_ZONE_NAME, 1, cluster.size());

        createTable(node, TEST_ZONE_NAME, TEST_TABLE_NAME1);
        createTable(node, TEST_ZONE_NAME, TEST_TABLE_NAME2);

        cluster.forEach(Node::waitForMetadataCompletenessAtNow);

        KeyValueView<Long, Integer> kvView1 = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Long.class, Integer.class);
        KeyValueView<Long, Integer> kvView2 = node.tableManager.table(TEST_TABLE_NAME2).keyValueView(Long.class, Integer.class);

        kvView1.putAll(null, Map.of(1L, 11, 2L, 22));
        kvView2.putAll(null, Map.of(3L, 33, 4L, 44));

        node.transactions().runInTransaction(tx -> {
            try (ResultSet<SqlRow> resultSet = node.sql().execute(tx, "SELECT VAL FROM " + TEST_TABLE_NAME1 + " ORDER BY KEY")) {
                List<Integer> results = extractIntegers(resultSet);
                assertThat(results, contains(11, 22));
            }

            try (ResultSet<SqlRow> resultSet = node.sql().execute(tx, "SELECT VAL FROM " + TEST_TABLE_NAME2 + " ORDER BY KEY")) {
                List<Integer> results = extractIntegers(resultSet);
                assertThat(results, contains(33, 44));
            }
        }, new TransactionOptions().readOnly(readOnly));
    }

    private static List<Integer> extractIntegers(ResultSet<SqlRow> resultSet) {
        List<Integer> results = new ArrayList<>();
        resultSet.forEachRemaining(row -> results.add(row.intValue(0)));
        return results;
    }
}
