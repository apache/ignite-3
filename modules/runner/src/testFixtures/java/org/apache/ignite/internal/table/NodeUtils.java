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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.placementdriver.message.StopLeaseProlongationMessage;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.jetbrains.annotations.Nullable;

/**
 * A helper class to manipulate Ignite nodes in tests.
 */
public class NodeUtils {
    private static final IgniteLogger LOG = Loggers.forClass(NodeUtils.class);

    private static final int AWAIT_PRIMARY_REPLICA_TIMEOUT = 10;

    private static final PlacementDriverMessagesFactory PLACEMENT_DRIVER_MESSAGES_FACTORY = new PlacementDriverMessagesFactory();

    /**
     * Transfers the primary rights to another node, choosing any node from the cluster except the current leaseholder.
     *
     * @param nodes Nodes collection.
     * @param groupId Group id.
     * @return New primary replica name.
     * @throws InterruptedException If failed.
     */
    public static String transferPrimary(
            Collection<IgniteImpl> nodes,
            ReplicationGroupId groupId
    ) throws InterruptedException {
        return transferPrimary(nodes, groupId, (Predicate) null);
    }

    /**
     * Transfers the primary rights to another node.
     *
     * @param nodes Nodes collection.
     * @param groupId Group id.
     * @param preferablePrimary Primary replica preferable node name.
     * @return New primary replica name.
     * @throws InterruptedException If failed.
     */
    public static String transferPrimary(
            Collection<IgniteImpl> nodes,
            ReplicationGroupId groupId,
            String preferablePrimary
    ) throws InterruptedException {
        return transferPrimary(nodes, groupId, s -> s.equals(preferablePrimary));
    }

    /**
     * Transfers the primary rights to another node.
     *
     * @param nodes Nodes collection.
     * @param groupId Group id.
     * @param preferablePrimaryFilter Primary replica preferable nodes filter, accepts the node consistent id.
     * @return New primary replica name.
     * @throws InterruptedException If failed.
     */
    public static String transferPrimary(
            Collection<IgniteImpl> nodes,
            ReplicationGroupId groupId,
            @Nullable Predicate<String> preferablePrimaryFilter
    ) throws InterruptedException {
        LOG.info("Moving the primary replica [groupId={}].", groupId);

        IgniteImpl node = nodes.stream().findAny().orElseThrow();

        ReplicaMeta currentLeaseholder = leaseholder(node, groupId);

        IgniteImpl leaseholderNode = nodes.stream()
                .filter(n -> n.id().equals(currentLeaseholder.getLeaseholderId()))
                .findFirst().orElseThrow();

        Predicate<String> filter = preferablePrimaryFilter == null ? name -> true : preferablePrimaryFilter;

        String[] candidates = nodes.stream()
                .map(IgniteImpl::name)
                .filter(n -> !n.equals(currentLeaseholder.getLeaseholder()) && filter.test(n))
                .toArray(String[]::new);

        assert candidates.length > 0 : "No candidates found for primary replica transfer in group " + groupId;

        AtomicInteger candidateIdx = new AtomicInteger();

        String[] preferablePrimary = new String[1];

        ReplicaMeta[] newPrimaryReplica = new ReplicaMeta[1];
        boolean[] stopLeaseNeeded = { true };

        boolean success = waitForCondition(() -> {
            if (stopLeaseNeeded[0]) {
                preferablePrimary[0] = candidates[candidateIdx.getAndIncrement() % candidates.length];

                LOG.info("Moving the primary replica [groupId={}, currentLeaseholder={}, preferablePrimary={}].",
                        groupId, leaseholderNode.name(), preferablePrimary[0]);

                stopLeaseProlongation(nodes, leaseholderNode, groupId, preferablePrimary[0]);
            }

            ReplicaMeta previousPrimary = newPrimaryReplica[0] == null ? currentLeaseholder : newPrimaryReplica[0];

            newPrimaryReplica[0] = leaseholder(node, groupId);

            // If the lease is changed to not suitable one, then stopLeaseProlongation will be retried, otherwise the cycle will be stopped.
            stopLeaseNeeded[0] =
                    !previousPrimary.getStartTime().equals(newPrimaryReplica[0].getStartTime())                // if lease changed
                            || !previousPrimary.getExpirationTime().equals(newPrimaryReplica[0].getExpirationTime());  // if lease prolonged

            return Arrays.stream(candidates).anyMatch(x -> x.equals(newPrimaryReplica[0].getLeaseholder()));
        }, 30_000);

        if (success) {
            LOG.info("Primary replica moved successfully from [{}] to [{}].", currentLeaseholder.getLeaseholder(), preferablePrimary[0]);
        } else {
            LOG.info("Moving the primary replica failed [groupId={}, actualPrimary={}].", groupId, newPrimaryReplica[0]);
        }

        assertTrue(success, "Failed to transfer primary replica from " + currentLeaseholder.getLeaseholder()
                + " to " + preferablePrimary[0] + " in group " + groupId);

        return preferablePrimary[0];
    }

    /**
     * Stops lease prolongation for the given replication group.
     *
     * @param nodes Cluster nodes.
     * @param leaseholderNode Current Lease holder.
     * @param groupId Replication group id.
     * @param preferablePrimary Preferable primary.
     */
    public static void stopLeaseProlongation(
            Collection<IgniteImpl> nodes,
            IgniteImpl leaseholderNode,
            ReplicationGroupId groupId,
            @Nullable String preferablePrimary
    ) {
        StopLeaseProlongationMessage msg = PLACEMENT_DRIVER_MESSAGES_FACTORY.stopLeaseProlongationMessage()
                .groupId(groupId)
                .redirectProposal(preferablePrimary)
                .build();

        nodes.forEach(
                n -> leaseholderNode.clusterService().messagingService().send(n.clusterService().topologyService().localMember(), msg)
        );
    }

    /**
     * Returns a replica meta information for the given replication group.
     *
     * @param node Ignite node to be used for getting a meta.
     * @param groupId Replication group id.
     * @return Replica meta.
     */
    public static ReplicaMeta leaseholder(IgniteImpl node, ReplicationGroupId groupId) {
        CompletableFuture<ReplicaMeta> leaseholderFuture = node.placementDriver().awaitPrimaryReplica(
                groupId,
                node.clock().now(),
                AWAIT_PRIMARY_REPLICA_TIMEOUT,
                SECONDS
        );

        assertThat(leaseholderFuture, willCompleteSuccessfully());

        return leaseholderFuture.join();
    }
}
