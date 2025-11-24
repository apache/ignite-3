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
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.withTx;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.withTxVoid;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test direct mapping for client's transactions.
 */
public class ItClientDirectMappingTest extends ClusterPerTestIntegrationTest {
    /** Table name. */
    private static final String TABLE_NAME = "test_table";

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

    @BeforeEach
    public void setup() throws Exception {
        String zoneSql = "create zone test_zone with partitions=5, replicas=1, storage_profiles='" + DEFAULT_AIPERSIST_PROFILE_NAME + "'";
        String sql = "create table " + TABLE_NAME + " (key int primary key, val varchar(20)) zone TEST_ZONE";

        cluster.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
        });
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        super.customizeInitParameters(builder);

        builder.clusterConfiguration("ignite {"
                + "  transaction: {"
                + "      readOnlyTimeoutMillis: 30000,"
                + "      readWriteTimeoutMillis: 30000"
                + "  },"
                + "  replication: {"
                + "      rpcTimeoutMillis: 30000"
                + "  },"
                + "}");
    }

    @Test
    public void testBasicImplicit() {
        try (IgniteClient client0 = clientConnectedToNode(0)) {
            ClientTable table = (ClientTable) client0.tables().table(TABLE_NAME);
            KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

            Tuple key = Tuple.create().set("key", 0);
            Tuple val = Tuple.create().set("val", "test0");

            kvView.put(null, key, val);
            Tuple val0 = kvView.get(null, key);
            assertTrue(Tuple.equals(val, val0));
        }
    }

    @Test
    public void testBasicExplicit() {
        try (IgniteClient client0 = clientConnectedToNode(0)) {
            ClientTable table = (ClientTable) client0.tables().table(TABLE_NAME);
            KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

            Tuple key = Tuple.create().set("key", 0);
            Tuple val = Tuple.create().set("val", "test0");

            withTxVoid(client0.transactions(), tx -> kvView.put(tx, key, val));
            Tuple val0 = withTx(client0.transactions(), tx -> kvView.get(tx, key));
            assertTrue(Tuple.equals(val, val0));
        }
    }

    private IgniteClient clientConnectedToNode(int nodeIndex) {
        return IgniteClient.builder()
                .addresses("localhost:" + igniteImpl(nodeIndex).clientAddress().port())
                .build();
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

    private IgniteImpl findNode(int startRange, int endRange, Predicate<IgniteImpl> filter) {
        return IntStream.range(startRange, endRange)
                .mapToObj(this::node)
                .map(TestWrappers::unwrapIgniteImpl)
                .filter(filter::test)
                .findFirst()
                .get();
    }

    private IgniteImpl findNodeByName(String leaseholder) {
        return findNode(0, initialNodes(), n -> leaseholder.equals(n.name()));
    }

    @Override
    protected int initialNodes() {
        return 2;
    }
}
