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

package org.apache.ignite.internal.placementdriver;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.SessionUtils.executeUpdate;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.NodeUtils;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * TODO: IGNITE-20485 Configure the lease interval as less as possible to decrease the duration of tests.
 * The test class checks invariant of a primary replica choice.
 */
public class ItPrimaryReplicaChoiceTest extends ClusterPerTestIntegrationTest {
    private static final int AWAIT_PRIMARY_REPLICA_TIMEOUT = 10;

    /** Table name. */
    private static final String TABLE_NAME = "test_table";

    @BeforeEach
    @Override
    public void setup(TestInfo testInfo) throws Exception {
        super.setup(testInfo);

        String zoneSql = "create zone test_zone with partitions=1, replicas=3";
        String sql = "create table " + TABLE_NAME + " (key int primary key, val varchar(20)) with primary_zone='TEST_ZONE'";

        cluster.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
        });
    }

    @Test
    public void testPrimaryChangeSubscription() throws Exception {
        TableViewInternal tbl = (TableViewInternal) node(0).tables().table(TABLE_NAME);

        var tblReplicationGrp = new TablePartitionId(tbl.tableId(), 0);

        CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().awaitPrimaryReplica(
                tblReplicationGrp,
                node(0).clock().now(),
                AWAIT_PRIMARY_REPLICA_TIMEOUT,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String primary = primaryReplicaFut.get().getLeaseholder();

        IgniteImpl ignite = node(primary);

        AtomicBoolean primaryChanged = new AtomicBoolean();

        ignite.placementDriver().listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, (evt, e) -> {
            primaryChanged.set(true);

            return falseCompletedFuture();
        });

        NodeUtils.transferPrimary(tbl, null, this::node);

        assertTrue(primaryChanged.get());
    }

    @Test
    public void testPrimaryChangeLongHandling() throws Exception {
        TableViewInternal tbl = (TableViewInternal) node(0).tables().table(TABLE_NAME);

        var tblReplicationGrp = new TablePartitionId(tbl.tableId(), 0);

        CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().awaitPrimaryReplica(
                tblReplicationGrp,
                node(0).clock().now(),
                AWAIT_PRIMARY_REPLICA_TIMEOUT,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String primary = primaryReplicaFut.get().getLeaseholder();

        IgniteImpl ignite = node(primary);

        CompletableFuture<Boolean> primaryChangedHandling = new CompletableFuture<>();

        ignite.placementDriver().listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, (evt, e) -> primaryChangedHandling);

        log.info("Primary replica is: " + primary);

        NodeUtils.transferPrimary(tbl, null, this::node);

        CompletableFuture<String> primaryChangeTask = IgniteTestUtils.runAsync(() -> NodeUtils.transferPrimary(tbl, primary, this::node));

        waitingForLeaderCache(tbl, primary);

        assertFalse(primaryChangeTask.isDone());

        primaryChangedHandling.complete(false);

        assertThat(primaryChangeTask, willCompleteSuccessfully());

        assertEquals(primary, primaryChangeTask.get());
    }

    @Test
    public void testLockReleaseWhenPrimaryChange() throws Exception {
        TableViewInternal tbl = (TableViewInternal) node(0).tables().table(TABLE_NAME);

        var tblReplicationGrp = new TablePartitionId(tbl.tableId(), 0);

        CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().awaitPrimaryReplica(
                tblReplicationGrp,
                node(0).clock().now(),
                AWAIT_PRIMARY_REPLICA_TIMEOUT,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String primary = primaryReplicaFut.get().getLeaseholder();

        IgniteImpl ignite = node(primary);

        InternalTransaction rwTx = (InternalTransaction) ignite.transactions().begin();

        assertTrue(tbl.recordView().insert(rwTx, Tuple.create().set("key", 42).set("val", "val 42")));

        assertTrue(ignite.txManager().lockManager().locks(rwTx.id()).hasNext());

        NodeUtils.transferPrimary(tbl, null, this::node);

        assertFalse(ignite.txManager().lockManager().locks(rwTx.id()).hasNext());
    }

    /**
     * Waits when the leader would be a different with the current primary replica.
     *
     * @param tbl Table.
     * @param primary Current primary replica name.
     * @throws InterruptedException If fail.
     */
    private static void waitingForLeaderCache(TableViewInternal tbl, String primary) throws InterruptedException {
        RaftGroupService raftSrvc = tbl.internalTable().partitionRaftGroupService(0);

        assertTrue(IgniteTestUtils.waitForCondition(() -> {
            raftSrvc.refreshLeader();

            Peer leader = raftSrvc.leader();

            return leader != null && !leader.consistentId().equals(primary);
        }, 10_000));
    }

    /**
     * Gets Ignite instance by the name.
     *
     * @param name Node name.
     * @return Ignite instance.
     */
    private @Nullable IgniteImpl node(String name) {
        for (int i = 0; i < initialNodes(); i++) {
            if (node(i).name().equals(name)) {
                return node(i);
            }
        }

        return null;
    }
}
