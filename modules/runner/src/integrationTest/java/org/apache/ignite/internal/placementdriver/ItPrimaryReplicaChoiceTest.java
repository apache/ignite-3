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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.TableView;
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
        TableView tbl = (TableView) node(0).tables().table(TABLE_NAME);

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

            return CompletableFuture.completedFuture(false);
        });

        transferPrimary(tbl, null);

        assertTrue(primaryChanged.get());
    }

    @Test
    public void testPrimaryChangeLongHandling() throws Exception {
        TableView tbl = (TableView) node(0).tables().table(TABLE_NAME);

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

        transferPrimary(tbl, null);

        CompletableFuture<String> primaryChangeTask = IgniteTestUtils.runAsync(() -> transferPrimary(tbl, primary));

        waitingForLeaderCache(tbl, primary);

        assertFalse(primaryChangeTask.isDone());

        primaryChangedHandling.complete(false);

        assertThat(primaryChangeTask, willCompleteSuccessfully());

        assertEquals(primary, primaryChangeTask.get());
    }

    @Test
    public void testLockReleaseWhenPrimaryChange() throws Exception {
        TableView tbl = (TableView) node(0).tables().table(TABLE_NAME);

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

        transferPrimary(tbl, null);

        assertFalse(ignite.txManager().lockManager().locks(rwTx.id()).hasNext());
    }

    /**
     * Waits when the leader would be a different with the current primary replica.
     *
     * @param tbl Table.
     * @param primary Current primary replica name.
     * @throws InterruptedException If fail.
     */
    private static void waitingForLeaderCache(TableView tbl, String primary) throws InterruptedException {
        RaftGroupService raftSrvc = tbl.internalTable().partitionRaftGroupService(0);

        assertTrue(IgniteTestUtils.waitForCondition(() -> {
            raftSrvc.refreshLeader();

            Peer leader = raftSrvc.leader();

            return leader != null && !leader.consistentId().equals(primary);
        }, 10_000));
    }

    /**
     * Transfers the primary rights to another node.
     *
     * @param tbl Table.
     * @param preferablePrimary Primary replica name which is preferred for being primary or {@code null}.
     * @return Future which points to a new primary replica name.
     * @throws InterruptedException If failed.
     */
    private String transferPrimary(TableView tbl, @Nullable String preferablePrimary) throws InterruptedException {
        var tblReplicationGrp = new TablePartitionId(tbl.tableId(), 0);

        CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().awaitPrimaryReplica(
                tblReplicationGrp,
                node(0).clock().now(),
                AWAIT_PRIMARY_REPLICA_TIMEOUT,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String primary = primaryReplicaFut.join().getLeaseholder();

        if (preferablePrimary != null && preferablePrimary.equals(primary)) {
            return primary;
        }

        // Change leader for the replication group.

        RaftGroupService raftSrvc = tbl.internalTable().partitionRaftGroupService(0);

        raftSrvc.refreshLeader();

        Peer leader = raftSrvc.leader();

        Peer newLeader = null;

        if (preferablePrimary != null) {
            for (Peer peer : raftSrvc.peers()) {
                if (peer.consistentId().equals(preferablePrimary)) {
                    newLeader = peer;
                }
            }
        }

        if (newLeader == null) {
            for (Peer peer : raftSrvc.peers()) {
                if (!leader.equals(peer)) {
                    newLeader = peer;
                }
            }
        }

        assertNotNull(newLeader);

        assertThat(raftSrvc.transferLeadership(newLeader), willCompleteSuccessfully());

        log.info("Leader moved [from={}, to={}]", leader, newLeader);

        // Leader changed.

        AtomicReference<String> newLeaseholder = new AtomicReference<>();

        assertTrue(IgniteTestUtils.waitForCondition(() -> {
            CompletableFuture<ReplicaMeta> newPrimaryReplicaFut = node(0).placementDriver().awaitPrimaryReplica(
                    tblReplicationGrp,
                    node(0).clock().now(),
                    AWAIT_PRIMARY_REPLICA_TIMEOUT,
                    SECONDS
            );

            assertThat(newPrimaryReplicaFut, willCompleteSuccessfully());

            if (!primary.equals(newPrimaryReplicaFut.join().getLeaseholder())) {
                newLeaseholder.set(newPrimaryReplicaFut.join().getLeaseholder());

                return true;
            } else {
                // Insert is needed to notify the placement driver about a leader for the group was changed.
                try {
                    tbl.recordView().upsert(null, Tuple.create().set("key", 1).set("val", "val 1"));
                } catch (Exception e) {
                    log.error("Failed to perform insert", e);
                }

                return false;
            }
        }, 60_000));

        log.info("Primary replica moved [from={}, to={}]", primary, newLeaseholder.get());

        return newLeaseholder.get();
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
