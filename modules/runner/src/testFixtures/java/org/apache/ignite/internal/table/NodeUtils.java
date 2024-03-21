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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.placementdriver.message.StopLeaseProlongationMessage;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.table.Tuple;
import org.checkerframework.common.value.qual.IntRange;
import org.jetbrains.annotations.Nullable;

/**
 * A helper class to manipulate Ignite nodes in tests.
 */
public class NodeUtils {
    private static final IgniteLogger LOG = Loggers.forClass(NodeUtils.class);

    private static final int AWAIT_PRIMARY_REPLICA_TIMEOUT = 10;

    private static final PlacementDriverMessagesFactory PLACEMENT_DRIVER_MESSAGES_FACTORY = new PlacementDriverMessagesFactory();

    /**
     * Transfers the primary rights to another node.
     *
     * @param tbl Table.
     * @param preferablePrimary Primary replica name which is preferred for being primary or {@code null}.
     * @return Future which points to a new primary replica name.
     * @throws InterruptedException If failed.
     */
    // TODO: IGNITE-20365: Replace this method when proper primary change method is implemented.
    public static String transferPrimary(TableViewInternal tbl, @Nullable String preferablePrimary, IntFunction<IgniteImpl> nodes)
            throws InterruptedException {
        var tblReplicationGrp = new TablePartitionId(tbl.tableId(), 0);

        CompletableFuture<ReplicaMeta> primaryReplicaFut = nodes.apply(0).placementDriver().awaitPrimaryReplica(
                tblReplicationGrp,
                nodes.apply(0).clock().now(),
                AWAIT_PRIMARY_REPLICA_TIMEOUT,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String primary = primaryReplicaFut.join().getLeaseholder();

        if (preferablePrimary != null && preferablePrimary.equals(primary)) {
            return primary;
        }

        // Change leader for the replication group.

        RaftGroupService raftSrvc = tbl.internalTable().tableRaftService().partitionRaftGroupService(0);

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

        LOG.info("Leader moved [from={}, to={}]", leader, newLeader);

        // Leader changed.

        AtomicReference<String> newLeaseholder = new AtomicReference<>();

        AtomicLong lastInsertAttempt = new AtomicLong();

        assertTrue(waitForCondition(() -> {
            CompletableFuture<ReplicaMeta> newPrimaryReplicaFut = nodes.apply(0).placementDriver().awaitPrimaryReplica(
                    tblReplicationGrp,
                    nodes.apply(0).clock().now(),
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
                    long lastTs = lastInsertAttempt.get();

                    if (coarseCurrentTimeMillis() - lastTs > 1_000 && lastInsertAttempt.compareAndSet(lastTs, coarseCurrentTimeMillis())) {
                        int nodeCount = nodes.apply(0).clusterNodes().size();

                        IgniteImpl primaryNode = IntStream.range(0, nodeCount)
                                .mapToObj(i -> nodes.apply(i))
                                .filter(n -> n.name().equals(primary))
                                .findAny().orElseThrow();

                        StopLeaseProlongationMessage msg = PLACEMENT_DRIVER_MESSAGES_FACTORY.stopLeaseProlongationMessage()
                                .groupId(tblReplicationGrp)
                                .build();

                        IntStream.range(0, nodeCount)
                                .mapToObj(i -> nodes.apply(i))
                                .forEach(n -> primaryNode.clusterService().messagingService().send(
                                        n.clusterService().topologyService().localMember(),
                                        msg
                                ));
                    }
                } catch (Exception e) {
                    LOG.error("Failed to perform insert", e);
                }

                return false;
            }
        }, 60_000));

        LOG.info("Primary replica moved [from={}, to={}]", primary, newLeaseholder.get());

        return newLeaseholder.get();
    }
}
