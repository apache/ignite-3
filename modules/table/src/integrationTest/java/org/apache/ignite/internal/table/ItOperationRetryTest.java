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
import static org.apache.ignite.internal.table.distributed.storage.InternalTableImpl.AWAIT_PRIMARY_REPLICA_TIMEOUT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.hasCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.metastorage.server.WatchListenerInhibitor;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
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

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        super.customizeInitParameters(builder);

        long txTimeoutMs = SECONDS.toMillis(AWAIT_PRIMARY_REPLICA_TIMEOUT * 2);

        // In retryImplicitTransactionsDueToReplicationTimeoutTest we want to ensure that retry will be triggered before default tx timeout.
        builder.clusterConfiguration("ignite { transaction: { readWriteTimeoutMillis: " + txTimeoutMs + " } }");
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
    public void retryImplicitTransactionsDueToReplicationTimeoutTest() throws InterruptedException {
        ZonePartitionId partitionGroupId = testPartitionGroupId();

        // We want node 0 as MG leader, node 1 as leaseholder and node 2 as implicit tx coordinator.
        int leaseholderNodeIdx = 1;
        String leaseholderNodeName = waitAndGetPrimaryReplica(partitionGroupId);

        // Move lease to node 1 in case of it was granted for a wrong node.
        if (nodeIndex(leaseholderNodeName) != leaseholderNodeIdx) {
            leaseholderNodeName = node(leaseholderNodeIdx).name();
            transferPrimaryTo(leaseholderNodeName, partitionGroupId);
        }

        IgniteImpl transactionCoordinatorNode = unwrapIgniteImpl(node(2));

        // Now we stop MS updates handling on coordinator nodes then we won't see any new lease updates there.
        WatchListenerInhibitor txCoordInhibitor = WatchListenerInhibitor.metastorageEventsInhibitor(transactionCoordinatorNode);
        txCoordInhibitor.startInhibit();

        // Stopping leaseholder node that should trigger lease changes for the group.
        stopNode(leaseholderNodeName);

        // Even with inhibitor schema sync and value trackers should work, so we won't hang there.
        RecordView<Tuple> view = transactionCoordinatorNode.tables().table(TABLE_NAME).recordView();
        CompletableFuture<Void> futureUpsert = view.upsertAsync(null, NEW_RECORD_TUPLE);

        // Let's wait for replication timeout that will lead to the retry.
        long msTimeout = SECONDS.toMillis(AWAIT_PRIMARY_REPLICA_TIMEOUT + 1);
        log.info("Test: waiting for {}ms.", msTimeout);
        Thread.sleep(msTimeout);

        // Let's return back the node and ability to find lease updates on the coordinator.
        log.info("Test: waiting is over, starting node {}.", leaseholderNodeIdx);
        txCoordInhibitor.stopInhibit();
        startNode(leaseholderNodeIdx);

        // Waiting unitl lease will be granted for the group and the coordinator should be able to get the primary replica for the retry.
        waitAndGetPrimaryReplica(transactionCoordinatorNode,  partitionGroupId);

        // Finally we expect the upsert will succeed eventually because of retry with new leaseholder enlisted.
        assertThat(futureUpsert, willSucceedIn(10, SECONDS));
        // And we can also read the expected value too.
        assertEquals(NEW_RECORD_VALUE, view.get(null, NEW_RECORD_KEY_TUPLE).value("val"));
    }

    @Test
    public void retryImplicitTransactionsDueToReplicaMissTest() {
        ZonePartitionId partitionGroupId = testPartitionGroupId();
        String leaseholderNodeName = waitAndGetPrimaryReplica(partitionGroupId);
        IgniteImpl transactionCoordinatorNode = findNonLeaseholderNode(leaseholderNodeName);

        AtomicBoolean leaseTransferIsTriggered = new AtomicBoolean();

        DefaultMessagingService messagingService = (DefaultMessagingService) transactionCoordinatorNode.clusterService().messagingService();
        messagingService.dropMessages((nodeName, msg) -> {
            boolean isMessageForCurrentLeaseholder = nodeName.equals(leaseholderNodeName);
            boolean isUpsertRequestMessage = msg instanceof ReadWriteSingleRowReplicaRequest;

            boolean shouldWaitForLatchDownAndTransferLease = isMessageForCurrentLeaseholder
                    && isUpsertRequestMessage
                    && leaseTransferIsTriggered.compareAndSet(false, true);

            if (!shouldWaitForLatchDownAndTransferLease) {
                return false; // Just pass the message then.
            }
            String newLeaseholderNodeName = transferPrimaryToNonCoordinatorNode(
                    leaseholderNodeName,
                    transactionCoordinatorNode.name(),
                    partitionGroupId
            );

            log.info("Test: lease transfer is done [from={}, to={}].", leaseholderNodeName, newLeaseholderNodeName);

            return false;
        });

        RecordView<Tuple> view = transactionCoordinatorNode.tables().table(TABLE_NAME).recordView();

        CompletableFuture<Void> futureUpsert = CompletableFuture
                .supplyAsync(() -> view.upsertAsync(null, NEW_RECORD_TUPLE))
                .thenCompose(identity());

        assertThat(futureUpsert, willSucceedIn(20, SECONDS));
        assertEquals(NEW_RECORD_VALUE, view.get(null, NEW_RECORD_KEY_TUPLE).value("val"));
    }

    @Test
    public void retryAfterLockFailureInSameTransaction() {
        Transaction tx1 = node(0).transactions().begin();
        Transaction tx2 = node(0).transactions().begin();

        executeSql(0, tx1, "INSERT INTO " + TABLE_NAME + " VALUES(?, ?)", NEW_RECORD_KEY, NEW_RECORD_VALUE);

        Exception firstAttempt = assertThrows(Exception.class, () ->
                executeSql(0, tx2, "INSERT INTO " + TABLE_NAME + " VALUES(?, ?)", NEW_RECORD_KEY, "other value"));

        assertInstanceOf(SqlException.class, firstAttempt);
        assertThat(firstAttempt.getMessage(), containsString("Failed to acquire a lock during request handling"));
        assertTrue(hasCause(firstAttempt, LockException.class, null), "Expected lock exception as a cause");

        Exception secondAttempt = assertThrows(Exception.class, () ->
                executeSql(0, tx2, "INSERT INTO " + TABLE_NAME + " VALUES(?, ?)", NEW_RECORD_KEY, "other value"));

        if (secondAttempt.getMessage() != null && secondAttempt.getMessage().contains("Transaction is already finished or finishing")) {
            assertInstanceOf(SqlException.class, secondAttempt);
            assertTrue(hasCause(secondAttempt, LockException.class, null), "Expected lock exception as a cause");
        } else {
            fail();
        }

        tx1.rollback();
        tx2.rollback();
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

    private static void assertTransactionLockException(Exception e) {
        assertInstanceOf(SqlException.class, e);
        assertThat(e.getMessage(), containsString("Failed to acquire a lock during request handling"));
        assertTrue(hasCause(e, LockException.class, null), "Expected lock exception as a cause");
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
