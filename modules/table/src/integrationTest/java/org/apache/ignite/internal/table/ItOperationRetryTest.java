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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.zoneId;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.apache.ignite.internal.storage.pagememory.configuration.PageMemoryStorageEngineLocalConfigurationModule.DEFAULT_PROFILE_NAME;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests of transaction operation retry.
 */
@Timeout(value = 3, unit = MINUTES)
public class ItOperationRetryTest extends ClusterPerTestIntegrationTest {
    /** Table name. */
    private static final String TABLE_NAME = "test_table";

    private static final String ZONE_NAME = "TEST_ZONE";

    private static final int PART_ID = 0;

    private static final int NEW_RECORD_KEY = 1;

    private static final String NEW_RECORD_VALUE = "new value";

    private static final Tuple NEW_RECORD_KEY_TUPLE = Tuple.create().set("key", NEW_RECORD_KEY);

    private static final Tuple NEW_RECORD_TUPLE = Tuple.create().set("key", NEW_RECORD_KEY).set("val", NEW_RECORD_VALUE);

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
        IgniteImpl leaseholderNode = findLeaseholderNode(testPartitionGroupId());
        IgniteImpl otherNode = findNonLeaseholderNode(leaseholderNode.name());

        log.info("Transactions are executed from a non-primary node [node={}, primary={}].", otherNode.name(), leaseholderNode.name());

        RecordView<Tuple> view = otherNode.tables().table(TABLE_NAME).recordView();

        Transaction tx1 = otherNode.transactions().begin();
        Transaction tx2 = otherNode.transactions().begin();

        view.get(tx1, NEW_RECORD_KEY_TUPLE);

        DefaultMessagingService messagingService = (DefaultMessagingService) leaseholderNode.clusterService().messagingService();

        AtomicBoolean lockHold = new AtomicBoolean(true);

        messagingService.dropMessages((nodeName, networkMessage) -> {
            if (nodeName.equals(otherNode.name()) && networkMessage instanceof ReplicaResponse && lockHold.compareAndSet(true, false)) {
                log.info("Exceptional response on lock acquisition [resp={}].", networkMessage.getClass().getSimpleName());

                tx1.commit();
            }

            return false;
        });

        view.upsert(tx2, NEW_RECORD_TUPLE);

        tx2.commit();

        assertEquals(NEW_RECORD_VALUE, view.get(null, NEW_RECORD_KEY_TUPLE).value("val"));
    }

    @Test
    public void retryImplicitTransactionsDueToReplicaMissTest() {
        ZonePartitionId partitionGroupId = testPartitionGroupId();
        String leaseholderNodeName = waitAndGetPrimaryReplica(partitionGroupId);
        IgniteImpl transactionCoordinatorNode = findNonLeaseholderNode(leaseholderNodeName);

        // When upsert will be triggered then we have to pull the latch down and transfer lease to another leaseholder node.
        CountDownLatch upsertIsTriggeredLatch = new CountDownLatch(1);

        // When the latch is arisen, then in case of RW request to the current leaseholder we will wait. When latch is pulled down we
        // additionally transfer lease and still pass a message, so actually the follow doesn't drop any message, but moves lease at
        // the moment after upsert was triggered.
        DefaultMessagingService messagingService = (DefaultMessagingService) transactionCoordinatorNode.clusterService().messagingService();
        messagingService.dropMessages((nodeName, msg) -> {
            boolean isMessageForCurrentLeaseholder = nodeName.equals(leaseholderNodeName);
            boolean isLatchArisen = upsertIsTriggeredLatch.getCount() > 0;
            boolean isUpsertRequestMessage = msg instanceof ReadWriteSingleRowReplicaRequest;

            boolean shouldWaitForLatchDownAndTransferLease = isMessageForCurrentLeaseholder && isUpsertRequestMessage && isLatchArisen;

            if (!shouldWaitForLatchDownAndTransferLease) {
                // Just pass the message then.
                return false;
            }

            log.info("Test: awaiting for upsert triggering and lease transfer.");

            try {
                upsertIsTriggeredLatch.await();
            } catch (InterruptedException e) {
                fail("Test: awaiting for upsert triggering and lease transfer were interrupted.", e);
            }

            log.info("Test: lease transfer begins.");

            String newLeaseholderNodeName = transferPrimaryToNonCoordinatorNode(
                    leaseholderNodeName,
                    transactionCoordinatorNode.name(),
                    partitionGroupId
            );

            log.info("Test: lease transfer is done [from={}, to={}].", leaseholderNodeName, newLeaseholderNodeName);

            return false;
        });

        RecordView<Tuple> view = transactionCoordinatorNode.tables().table(TABLE_NAME).recordView();

        // Trigger upsert and pull down the latch to trigger lease transfer. We need to do the first step in a separate thread because
        // drop message handler may be executed in the same thread with the test worker one from upsertAsync's internals.
        CompletableFuture<Void> futureUpsert = CompletableFuture
                .supplyAsync(() -> view.upsertAsync(null, NEW_RECORD_TUPLE))
                .thenCompose(identity());
        upsertIsTriggeredLatch.countDown();

        log.info("Test: upsert is triggered, latch is pulled down.");

        // Finally we expect the triggered upsert will succeed eventually because of retry with new leaseholder enlisted.
        assertThat(futureUpsert, willSucceedIn(20, SECONDS));
        // And we can also read the expected value too.
        assertEquals(NEW_RECORD_VALUE, view.get(null, NEW_RECORD_KEY_TUPLE).value("val"));
    }

    private String  transferPrimaryTo(String newLeaseholderNodeName, ZonePartitionId replicationGroup) {
        return NodeUtils.transferPrimary(
                runningNodes().map(TestWrappers::unwrapIgniteImpl).collect(Collectors.toList()),
                replicationGroup,
                newLeaseholderNodeName
        );
    }

    private String  transferPrimaryToNonCoordinatorNode(
            String leaseholderNodeName,
            String transactionCoordinatorNodeName,
            ZonePartitionId replicationGroupId
    ) {
        return transferPrimaryTo(
                findNodeByFilter(n -> !(n.name().equals(leaseholderNodeName) || n.name().equals(transactionCoordinatorNodeName))).name(),
                replicationGroupId
        );
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

    private IgniteImpl findNonLeaseholderNode(String leaseholderNodeName) {
        return findNodeByFilter(ignite -> !leaseholderNodeName.equals(ignite.name()));
    }

    private IgniteImpl findLeaseholderNode(ZonePartitionId replicationGroupId) {
        return findNodeByName(waitAndGetPrimaryReplica(replicationGroupId));
    }

    private ZonePartitionId testPartitionGroupId() {
        return extractPartitionGroupId(node0(), ZONE_NAME, PART_ID);
    }

    private static String waitAndGetPrimaryReplica(IgniteImpl node, ZonePartitionId replicationGroupId) {
        CompletableFuture<ReplicaMeta> primaryReplicaFut = node.placementDriver().awaitPrimaryReplica(
                replicationGroupId,
                node.clock().now(),
                10,
                SECONDS
        );
        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        ReplicaMeta replicaMeta = primaryReplicaFut.join();
        assertNotNull(replicaMeta);

        String leaseholderNodeName = replicaMeta.getLeaseholder();
        assertNotNull(leaseholderNodeName);

        return leaseholderNodeName;
    }

    private String waitAndGetPrimaryReplica(ZonePartitionId replicationGroup) {
        return waitAndGetPrimaryReplica(node0(), replicationGroup);
    }

    private IgniteImpl node0() {
        return unwrapIgniteImpl(node(0));
    }
}
