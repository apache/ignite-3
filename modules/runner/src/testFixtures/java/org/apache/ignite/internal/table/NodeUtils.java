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

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
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

        Predicate<String> filter = preferablePrimaryFilter == null ? name -> true : preferablePrimaryFilter::test;

        String finalPreferablePrimary = nodes.stream()
                .map(IgniteImpl::name)
                .filter(n -> !n.equals(currentLeaseholder.getLeaseholder()) && filter.test(n))
                .findFirst()
                .orElseThrow();

        LOG.info("Moving the primary replica [groupId={}, currentLeaseholder={}, preferablePrimary={}].", groupId, leaseholderNode.name(),
                finalPreferablePrimary);

        StopLeaseProlongationMessage msg = PLACEMENT_DRIVER_MESSAGES_FACTORY.stopLeaseProlongationMessage()
                .groupId(groupId)
                .redirectProposal(finalPreferablePrimary)
                .build();

        nodes.forEach(
                n -> leaseholderNode.clusterService().messagingService().send(n.clusterService().topologyService().localMember(), msg)
        );

        ReplicaMeta[] newPrimaryReplica = new ReplicaMeta[1];;

        boolean success = waitForCondition(() -> {
            newPrimaryReplica[0] = leaseholder(node, groupId);

            return newPrimaryReplica[0].getLeaseholder().equals(finalPreferablePrimary);
        }, 10_000);

        if (success) {
            LOG.info("Primary replica moved successfully from [{}] to [{}].", currentLeaseholder.getLeaseholder(), finalPreferablePrimary);
        } else {
            LOG.info("Moving the primary replica failed [groupId={}, actualPrimary={}].", groupId, newPrimaryReplica[0]);
        }

        assertTrue(success);

        return finalPreferablePrimary;
    }

    private static ReplicaMeta leaseholder(IgniteImpl node, ReplicationGroupId groupId) {
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
