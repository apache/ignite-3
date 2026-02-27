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

package org.apache.ignite.internal.tx.impl;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.NodeUtils;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.Test;

/**
 * Test to verify that transaction labels propagate to primary replica.
 */
public class ItTransactionLabelPropagationTest extends ClusterPerTestIntegrationTest {

    @Override
    protected int initialNodes() {
        return 2;
    }

    @Test
    void testTxLabelPropagatedToPrimaryReplica() {
        String zoneName = "LABEL_ZONE";
        String tableName = "LABEL_TABLE";
        String label = "TX-LABEL";

        executeSql("CREATE ZONE " + zoneName + " (PARTITIONS 1, REPLICAS 2) STORAGE PROFILES ['"
                + CatalogService.DEFAULT_STORAGE_PROFILE + "']");
        executeSql("CREATE TABLE " + tableName + " (id INT PRIMARY KEY, val VARCHAR) ZONE " + zoneName);

        TableImpl table = unwrapTableImpl(node(0).tables().table(tableName));
        ZonePartitionId groupId = new ZonePartitionId(table.zoneId(), 0);

        List<IgniteImpl> nodes = runningNodes().map(TestWrappers::unwrapIgniteImpl).collect(toList());
        String primaryName = NodeUtils.leaseholder(igniteImpl(0), groupId).getLeaseholder();

        IgniteImpl primaryNode = nodes.stream().filter(n -> n.name().equals(primaryName)).findFirst().orElseThrow();
        IgniteImpl txCoordinatorFromOtherNode = nodes.stream().filter(n -> !n.name().equals(primaryName)).findFirst().orElseThrow();

        KeyValueView<Integer, String> view = txCoordinatorFromOtherNode.tables().table(tableName).keyValueView(Integer.class, String.class);
        InternalTransaction tx = (InternalTransaction) txCoordinatorFromOtherNode.transactions()
                .begin(new TransactionOptions().label(label));

        view.put(tx, 1, "val");

        await("Expected tx label to be visible on the primary replica node")
                .atMost(Duration.ofSeconds(10))
                .until(() -> {
                    TxStateMeta meta = primaryNode.txManager().stateMeta(tx.id());
                    return meta != null && label.equals(meta.txLabel());
                });

        tx.commit();
    }
}
