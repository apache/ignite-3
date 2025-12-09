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
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.zoneId;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.apache.ignite.internal.storage.pagememory.configuration.PageMemoryStorageEngineLocalConfigurationModule.DEFAULT_PROFILE_NAME;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.message.StopLeaseProlongationMessage;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests of transaction operation retry.
 */
public class ItOperationRetryTest extends ClusterPerTestIntegrationTest {
    /** Table name. */
    private static final String TABLE_NAME = "test_table";

    private static final String ZONE_NAME = "TEST_ZONE";

    private static final int PART_ID = 0;

    @BeforeEach
    public void setup() {
        String createZoneSql = format("create zone {} (partitions 1, replicas 3) storage profiles ['{}']", ZONE_NAME, DEFAULT_PROFILE_NAME);
        String createTableQuery = format("create table {} (key int primary key, val varchar(20)) zone {}", TABLE_NAME,  ZONE_NAME);

        cluster.doInSession(0, session -> {
            executeUpdate(createZoneSql, session);
            executeUpdate(createTableQuery, session);
        });
    }

    @Test
    public void testLockExceptionRetry() {
        IgniteImpl node0 = unwrapIgniteImpl(node(0));

        ZonePartitionId partitionGroupId = extractPartitionGroupId(node0, ZONE_NAME, PART_ID);

        String leaseholder = waitAndGetPrimaryReplica(node0, partitionGroupId).getLeaseholder();

        IgniteImpl leaseholderNode = findNodeByName(leaseholder);
        IgniteImpl otherNode = findNonLeaseholderNode(leaseholderNode);

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

    @Test
    public void retryImplicitTransactionsTest() throws InterruptedException {
        IgniteImpl node0 = unwrapIgniteImpl(node(0));

        ZonePartitionId partitionGroupId = extractPartitionGroupId(node0, ZONE_NAME, PART_ID);

        String leaseholderNodeName = waitAndGetPrimaryReplica(node0, partitionGroupId).getLeaseholder();

        IgniteImpl leaseholderNode = findNodeByName(leaseholderNodeName);

        IgniteImpl transactionCoordinatorNode = findNonLeaseholderNode(leaseholderNode);

        IgniteImpl futureLeaseholderNode = findNodeByFilter(n -> {
            String nodeName = n.name();

            return !(
                    nodeName.equals(leaseholderNode.name())
                            || nodeName.equals(transactionCoordinatorNode.name())
            );
        });

        DefaultMessagingService messagingService = (DefaultMessagingService) leaseholderNode.clusterService().messagingService();
        messagingService.dropMessages((nodeName, msg) -> msg instanceof PrimaryReplicaRequest);

        RecordView<Tuple> view = transactionCoordinatorNode.tables().table(TABLE_NAME).recordView();
        view.upsert(null, Tuple.create().set("key", 1).set("val", "new value"));

        Collection<IgniteImpl> nodes = runningNodes().map(TestWrappers::unwrapIgniteImpl).collect(Collectors.toList());

        NodeUtils.transferPrimary(nodes, partitionGroupId, futureLeaseholderNode.name());

        assertEquals("new value", view.get(null, Tuple.create().set("key", 1)).value("val"));
    }

    private static ZonePartitionId extractPartitionGroupId(IgniteImpl node, String zoneName, int partitionId) {
        return new ZonePartitionId(zoneId(node.catalogManager(), zoneName), partitionId);
    }

    private IgniteImpl findNodeByName(String leaseholderNodeName) {
        return findNodeByFilter(n -> leaseholderNodeName.equals(n.name()));
    }

    private IgniteImpl findNodeByFilter(Predicate<IgniteImpl> filter) {
        return findNode(0, initialNodes(),  filter);
    }

    private IgniteImpl findNode(int startRange, int endRange, Predicate<IgniteImpl> filter) {
        return IntStream.range(startRange, endRange)
                .mapToObj(this::node)
                .map(TestWrappers::unwrapIgniteImpl)
                .filter(filter::test)
                .findFirst()
                .get();
    }

    private IgniteImpl findNonLeaseholderNode(IgniteImpl leaseholderNode) {
        return findNodeByFilter(ignite -> !leaseholderNode.name().equals(ignite.name()));
    }

    private ReplicaMeta waitAndGetPrimaryReplica(IgniteImpl node, ZonePartitionId replicationGroup) {
        CompletableFuture<ReplicaMeta> primaryReplicaFut = node.placementDriver().awaitPrimaryReplica(
                replicationGroup,
                node.clock().now(),
                10,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        return primaryReplicaFut.join();
    }

    private ReplicaMeta movePrimaryReplicaToAnyNodeFrom(IgniteImpl leaseholderNode, ZonePartitionId replicationGroup) {
        log.info("Move lease from {}",  leaseholderNode.name());

        DefaultMessagingService messagingService = (DefaultMessagingService) leaseholderNode.clusterService().messagingService();

        messagingService.dropMessages((nodeName, networkMessage) -> networkMessage instanceof StopLeaseProlongationMessage);

        IgniteImpl nonLeaseholderNode = findNonLeaseholderNode(leaseholderNode);

        ReplicaMeta newPrimaryReplicaMeta = Awaitility.waitAtMost(Duration.ofSeconds(20)).until(
                () -> waitAndGetPrimaryReplica(nonLeaseholderNode , replicationGroup),
                replicaMeta ->
                        replicaMeta != null && !replicaMeta.getLeaseholder().equals(leaseholderNode.name())
        );

        messagingService.stopDroppingMessages();

        assertNotNull(newPrimaryReplicaMeta);

        log.info("New lease is on {}",  newPrimaryReplicaMeta.getLeaseholder());

        return newPrimaryReplicaMeta;
    }
}
