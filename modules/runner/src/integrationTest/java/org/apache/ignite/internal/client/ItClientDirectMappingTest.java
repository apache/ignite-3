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

package org.apache.ignite.internal.client;

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClient.Builder;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.client.tx.ClientLazyTransaction;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.table.partition.PartitionDistribution;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test direct mapping for client's transactions.
 */
public class ItClientDirectMappingTest extends ClusterPerClassIntegrationTest {
    /** Table name. */
    private static final String TABLE_NAME = "test_table";

    private static final String ZONE_NAME = "test_zone";

    /** Table name 2. */
    private static final String TABLE_NAME_2 = "test_table_2";

    private static final String ZONE_NAME2 = "test_zone2";

    protected static final String COLUMN_KEY = "key";

    protected static final String COLUMN_VAL = "val";

    protected static final int PARTITIONS = 10;

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE = "ignite {\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} },\n"
            + "  rest.port: {},\n"
            + "  raft: { responseTimeoutMillis: 30000 },"
            + "  failureHandler.dumpThreadsOnFailure: false\n"
            + "}";

    @BeforeAll
    public static void setup() throws Exception {
        String zoneSql = "create zone " + ZONE_NAME + " with partitions=" + PARTITIONS + ", replicas=2, storage_profiles='"
                + DEFAULT_AIPERSIST_PROFILE_NAME + "'";
        String sql = "create table " + TABLE_NAME + " (key int primary key, val varchar(20)) zone " + ZONE_NAME;
        String zoneSql2 = "create zone " + ZONE_NAME2 + " with partitions=" + PARTITIONS + ", replicas=2, storage_profiles='"
                + DEFAULT_AIPERSIST_PROFILE_NAME + "'";
        String sql2 = "create table " + TABLE_NAME_2 + " (key int primary key, val varchar(20)) zone " + ZONE_NAME2;

        CLUSTER.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
            executeUpdate(zoneSql2, session);
            executeUpdate(sql2, session);
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReadOnCoordinatorWithDirectWrite(boolean commit) {
        try (IgniteClient client = clientConnectedToAllNodes()) {
            Table table1 = client.tables().table(TABLE_NAME);
            KeyValueView<Tuple, Tuple> view1 = table1.keyValueView();
            Table table2 = client.tables().table(TABLE_NAME_2);
            KeyValueView<Tuple, Tuple> view2 = table2.keyValueView();

            Map<Partition, ClusterNode> map1 = table1.partitionDistribution().primaryReplicasAsync().join();
            Map<Integer, ClusterNode> mapPartById1 = map1.entrySet().stream().collect(Collectors.toMap(
                    entry -> Math.toIntExact(entry.getKey().id()),
                    Entry::getValue
            ));

            Map<Partition, ClusterNode> map2 = table2.partitionDistribution().primaryReplicasAsync().join();
            Map<Integer, ClusterNode> mapPartById2 = map2.entrySet().stream().collect(Collectors.toMap(
                    entry -> Math.toIntExact(entry.getKey().id()),
                    Entry::getValue
            ));

            // Find a partition which mapped to different primaries.
            int targetPart = -1;

            for (int i = 0; i < PARTITIONS; i++) {
                ClusterNode node1 = mapPartById1.get(i);
                ClusterNode node2 = mapPartById2.get(i);

                if (!node1.equals(node2)) {
                    targetPart = i;
                    break;
                }
            }

            if (targetPart == -1) {
                log.warn("Skipping test due to bad assignment");
                return;
            }

            log.info("DBG: using partition " + targetPart);

            // Tables have the same structure, can reuse keys.
            List<Tuple> keys = generateKeysForPartition(commit ? 0 : 100, 1, map1, targetPart, table1);

            ClientLazyTransaction tx = (ClientLazyTransaction) client.transactions().begin(new TransactionOptions().readOnly(false));

            Tuple key = keys.get(0);
            // Enlist read operation on coordinator (proxy mode).
            if (view2.get(tx, key) != null) {
                fail("Should never happen");

                return;
            }

            // Enlist write operation on other node (direct mode).
            view1.put(tx, key, val("test" + key.intValue(0)));
            if (commit) {
                tx.commit();
            } else {
                tx.rollback();
            }

            if (commit) {
                assertNotNull(view1.get(null, key), "key=" + key);
            } else {
                assertNull(view1.get(null, key), "key=" + key);
            }
        }
    }

    private IgniteClient clientConnectedToAllNodes() {
        Builder builder = IgniteClient.builder();
        String[] addresses = new String[initialNodes()];

        for (int i = 0; i < initialNodes(); i++) {
            addresses[i] = "localhost:" + igniteImpl(i).clientAddress().port();
        }

        builder.addresses(addresses);

        return builder.build();
    }

    /**
     * Returns node bootstrap config template.
     *
     * @return Node bootstrap config template.
     */
    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return NODE_BOOTSTRAP_CFG_TEMPLATE;
    }

    @Override
    protected int initialNodes() {
        return 2;
    }

    private static List<Tuple> generateKeysForPartition(
            int start,
            int count,
            Map<Partition, ClusterNode> map,
            int partId,
            Table table
    ) {
        List<Tuple> keys = new ArrayList<>();
        PartitionDistribution partitionManager = table.partitionDistribution();

        int k = start;
        while (keys.size() != count) {
            k++;
            Tuple t = key(k);

            Partition part = partitionManager.partitionAsync(t).orTimeout(5, TimeUnit.SECONDS).join();

            if (part.id() == partId) {
                keys.add(t);
            }
        }

        return keys;
    }

    private static Tuple val(String v) {
        return Tuple.create().set(COLUMN_VAL, v);
    }

    private static Tuple key(Integer k) {
        return Tuple.create().set(COLUMN_KEY, k);
    }
}
