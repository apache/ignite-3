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

package org.apache.ignite.tx.distributed;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.findTupleToBeHostedOnNode;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.partitionIdForTuple;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.txId;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.waitAndGetPrimaryReplica;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.zoneId;

import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for transaction abort on coordinator when write-intent resolution happens after primary replica expiration.
 */
public class ItTxAbortOnCoordinatorOnWIResolutionWhenPrimaryExpiredTest extends ClusterPerTestIntegrationTest {
    /** Zone name. */
    private static final String ZONE_NAME = "test_zone";

    /** Table name. */
    private static final String TABLE_NAME = "test_table";

    private static final Tuple INITIAL_TUPLE = Tuple.create().set("key", 1L).set("val", "1");

    private static final Function<Tuple, Tuple> NEXT_TUPLE = t -> Tuple.create()
            .set("key", t.longValue("key") + 1)
            .set("val", "" + (t.longValue("key") + 1));

    @BeforeEach
    public void setup() {
        String zoneSql = "create zone " + ZONE_NAME + " (partitions 20, replicas 3) storage profiles ['" + DEFAULT_STORAGE_PROFILE + "']";
        String sql = "create table " + TABLE_NAME + " (key bigint primary key, val varchar(20)) zone " + ZONE_NAME;

        cluster.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
        });
    }

    @Override
    protected int[] cmgMetastoreNodes() {
        return new int[]{0, 1, 2};
    }

    @Test
    public void test() throws Exception {
        IgniteImpl firstPrimaryNode = anyNode();
        IgniteImpl coordinatorNode = findNode(n -> !n.name().equals(firstPrimaryNode.name()));

        log.info("Test: firstPrimaryNode: {}, firstPrimaryNode id: {}", firstPrimaryNode.name(), firstPrimaryNode.id());
        log.info("Test: coordinatorNode: {}, coordinatorNode id: {}", coordinatorNode.name(), coordinatorNode.id());

        RecordView<Tuple> view = firstPrimaryNode.tables().table(TABLE_NAME).recordView();

        Transaction txx = firstPrimaryNode.transactions().begin();
        Tuple tuple = findTupleToBeHostedOnNode(firstPrimaryNode, TABLE_NAME, txx, INITIAL_TUPLE, NEXT_TUPLE, true);
        int partId = partitionIdForTuple(firstPrimaryNode, TABLE_NAME, tuple, txx);
        var groupId = new ZonePartitionId(zoneId(firstPrimaryNode, TABLE_NAME), partId);
        log.info("Test: groupId: " + groupId);
        view.upsert(txx, tuple);
        txx.commit();

        Transaction tx0 = coordinatorNode.transactions().begin();
        log.info("Test: unfinished tx id: " + txId(tx0));
        view.upsert(tx0, tuple);
        // Don't commit or rollback tx0.

        // for replication
        Thread.sleep(1000);

        UUID firstPrimaryNodeId = firstPrimaryNode.id();

        log.info("Test: node stop: " + firstPrimaryNode.name());
        firstPrimaryNode.stop();

        // for lease to expire
        waitForCondition(() -> {
            ReplicaMeta meta = coordinatorNode.placementDriver().getCurrentPrimaryReplica(groupId, coordinatorNode.clock().current());

            return meta != null && !meta.getLeaseholderId().equals(firstPrimaryNodeId);
        }, 30_000);

        waitAndGetPrimaryReplica(coordinatorNode, groupId);

        RecordView<Tuple> view1 = coordinatorNode.tables().table(TABLE_NAME).recordView();

        Transaction tx = coordinatorNode.transactions().begin();
        log.info("Test: new tx: " + txId(tx));
        log.info("Test: upsert");

        // If coordinator of tx0 doesn't abort it, tx will stumble into write intent and fail with TxIdMismatchException.
        view1.upsert(tx, tuple);

        tx.commit();
    }
}
