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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.runner.app.client.ItThinClientTransactionsTest.generateKeysForNode;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.internal.client.tx.ClientLazyTransaction;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.partition.Partition;
import org.junit.jupiter.api.Test;

/**
 * Tests for client transaction cleanup on disconnect.
 */
@SuppressWarnings({"resource", "DataFlowIssue"})
public class ItThinClientTransactionCleanupTest extends ItAbstractThinClientTest {
    /**
     * Tests that locks are released when client disconnects with a transaction having direct enlistments.
     */
    @Test
    void testClientDisconnectCleansUpWriteIntents() {
        try (IgniteClient client = IgniteClient.builder().addresses(getClientAddresses().toArray(new String[0])).build()) {
            var table = (ClientTable) client.tables().table(TABLE_NAME);
            Map<Partition, ClusterNode> map = table.partitionDistribution().primaryReplicas();

            IgniteImpl server0 = unwrapIgniteImpl(server(0));
            IgniteImpl server1 = unwrapIgniteImpl(server(1));

            List<Tuple> tuples0 = generateKeysForNode(300, 1, map, server0.cluster().localNode(), table);
            List<Tuple> tuples1 = generateKeysForNode(310, 1, map, server1.cluster().localNode(), table);

            Map<Tuple, Tuple> data = new HashMap<>();

            data.put(tuples0.get(0), val(tuples0.get(0).intValue(0) + ""));
            data.put(tuples1.get(0), val(tuples1.get(0).intValue(0) + ""));

            ClientLazyTransaction tx0 = (ClientLazyTransaction) client().transactions().begin();

            table.keyValueView().putAll(tx0, data);

            for (Entry<Tuple, Tuple> entry : data.entrySet()) {
                table.keyValueView().put(tx0, entry.getKey(), entry.getValue());
            }

            assertThat(txLockCount(), greaterThanOrEqualTo(2));

            // Disconnect without commit or rollback.
        }

        await().atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> assertEquals(0, txLockCount()));
    }

    private int txLockCount() {
        int count = 0;

        for (int i = 0; i < nodes(); i++) {
            IgniteImpl ignite = unwrapIgniteImpl(server(i));
            LockManager lockManager = ignite.txManager().lockManager();

            var iter = lockManager.locks();

            while (iter.hasNext()) {
                iter.next();
                count++;
            }
        }

        return count;
    }

    private static Tuple val(String v) {
        return Tuple.create().set(COLUMN_VAL, v);
    }
}
