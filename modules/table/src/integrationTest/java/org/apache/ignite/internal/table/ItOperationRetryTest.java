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

package org.apache.ignite.internal.table;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.SessionUtils.executeUpdate;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.storage.pagememory.configuration.PageMemoryStorageEngineLocalConfigurationModule.DEFAULT_PROFILE_NAME;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests of transaction operation retry.
 */
public class ItOperationRetryTest extends ClusterPerTestIntegrationTest {
    /** Table name. */
    private static final String TABLE_NAME = "test_table";

    private static final int PART_ID = 0;

    @BeforeEach
    @Override
    public void setup(TestInfo testInfo) throws Exception {
        super.setup(testInfo);

        String zoneSql = "create zone test_zone with partitions=1, replicas=3, storage_profiles='" + DEFAULT_PROFILE_NAME + "'";
        String sql = "create table " + TABLE_NAME + " (key int primary key, val varchar(20)) with primary_zone='TEST_ZONE'";

        cluster.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
        });
    }

    @Test
    public void testLockExceptionRetry() {
        TableImpl tbl = unwrapTableImpl(node(0).tables().table(TABLE_NAME));

        var zoneReplicationGrp = new ZonePartitionId(tbl.internalTable().zoneId(), tbl.tableId(), PART_ID);

        String leaseholder = waitAndGetPrimaryReplica(node(0), zoneReplicationGrp).getLeaseholder();

        IgniteImpl leaseholderNode = findNodeByName(leaseholder);
        IgniteImpl otherNode = findNode(0, initialNodes(), ignite -> !leaseholderNode.equals(ignite.name()));

        log.info("Transactions are executed from a non-primary node [node={}, primary={}].", otherNode.name(), leaseholderNode.name());

        RecordView<Tuple> view = otherNode.tables().table(TABLE_NAME).recordView();

        Transaction tx1 = otherNode.transactions().begin();
        Transaction tx2 = otherNode.transactions().begin();

        view.get(tx1, Tuple.create().set("key", 1));

        DefaultMessagingService messagingService = (DefaultMessagingService) leaseholderNode.clusterService().messagingService();

        AtomicBoolean lockHold = new AtomicBoolean(true);

        messagingService.dropMessages((nodeName, networkMessage) -> {
            if (nodeName.equals(otherNode.name()) && networkMessage instanceof ReplicaResponse && lockHold.compareAndSet(true, false)) {
                log.info("Exceptional response on lock acquisition [resp={}].", networkMessage.getClass().getSimpleName());

                tx1.commit();
            }

            return false;
        });

        view.upsert(tx2, Tuple.create().set("key", 1).set("val", "new value"));

        tx2.commit();

        assertEquals("new value", view.get(null, Tuple.create().set("key", 1)).value("val"));
    }

    private IgniteImpl findNodeByName(String leaseholder) {
        return findNode(0, initialNodes(), n -> leaseholder.equals(n.name()));
    }

    private IgniteImpl findNode(int startRange, int endRange, Predicate<IgniteImpl> filter) {
        return IntStream.range(startRange, endRange)
                .mapToObj(this::node)
                .filter(filter::test)
                .findFirst()
                .get();
    }

    private ReplicaMeta waitAndGetPrimaryReplica(IgniteImpl node, ReplicationGroupId tblReplicationGrp) {
        CompletableFuture<ReplicaMeta> primaryReplicaFut = node.placementDriver().awaitPrimaryReplicaForTable(
                tblReplicationGrp,
                node.clock().now(),
                10,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        return primaryReplicaFut.join();
    }
}
