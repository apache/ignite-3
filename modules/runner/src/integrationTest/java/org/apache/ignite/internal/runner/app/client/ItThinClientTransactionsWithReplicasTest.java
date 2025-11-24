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

import static org.apache.ignite.internal.runner.app.client.ItThinClientTransactionsTest.generateKeysForNode;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.app.IgniteServerImpl;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.Test;

/**
 * Thin client transactions integration test with multiple replicas.
 */
public class ItThinClientTransactionsWithReplicasTest extends ItAbstractThinClientTest {
    @Test
    void testStaleMapping() {
        Map<Partition, ClusterNode> map = table().partitionManager().primaryReplicasAsync().orTimeout(9, TimeUnit.SECONDS).join();

        ClientTable table = (ClientTable) table();

        IgniteImpl server0 = TestWrappers.unwrapIgniteImpl(server(0));
        IgniteImpl server1 = TestWrappers.unwrapIgniteImpl(server(1));
        IgniteImpl server2 = TestWrappers.unwrapIgniteImpl(server(2));

        List<Tuple> tuples0 = generateKeysForNode(100, 1, map, server0.cluster().localNode(), table);
        List<Tuple> tuples1 = generateKeysForNode(100, 1, map, server1.cluster().localNode(), table);
        List<Tuple> tuples2 = generateKeysForNode(100, 1, map, server2.cluster().localNode(), table);

        if (tuples0.isEmpty() || tuples1.isEmpty() || tuples2.isEmpty()) {
            return; // Skip the test if assignments are bad.
        }

        Transaction tx0 = client().transactions().begin();

        KeyValueView<Tuple, Tuple> view = table.keyValueView();

        Tuple k = tuples0.get(0);
        Tuple v = val(tuples0.get(0).intValue(0) + "");
        view.put(tx0, k, v);

        Tuple k1 = tuples1.get(0);
        Tuple v1 = val(tuples1.get(0).intValue(0) + "");
        view.put(tx0, k1, v1);

        IgniteServerImpl ignite = (IgniteServerImpl) ignite(2);
        ignite.restartAsync().orTimeout(9, TimeUnit.SECONDS).join();

        Table srvTable = server0.tables().table(TABLE_NAME);
        srvTable.partitionManager().primaryReplicasAsync().orTimeout(9, TimeUnit.SECONDS).join();

        Tuple k2 = tuples2.get(0);
        Tuple v2 = val(tuples2.get(0).intValue(0) + "");

        assertThrows(TransactionException.class, () -> view.put(tx0, k2, v2));

        assertNull(view.get(null, k));
        assertNull(view.get(null, k1));
        assertNull(view.get(null, k2));
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

    @Override
    protected int replicas() {
        return 3;
    }

    @Override
    protected int nodes() {
        return 3;
    }
}
