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

import static java.util.Collections.emptyList;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.partitiondistribution.Assignment.forPeer;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_LEASES_KEY;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.components.SystemPropertiesNodeProperties;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.ConditionalWatchInhibitor;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.UnresolvableConsistentIdException;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.leases.LeaseBatch;
import org.apache.ignite.internal.placementdriver.leases.LeaseTracker;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessage;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.placementdriver.negotiation.LeaseAgreement;
import org.apache.ignite.internal.placementdriver.negotiation.LeaseNegotiator;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.ignite.network.NetworkAddress;
import org.apache.logging.log4j.core.LogEvent;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test checking exceptional situations on lease negotiation.
 */
@ExtendWith({ConfigurationExtension.class})
public class LeaseNegotiationTest extends BaseIgniteAbstractTest {
    private static final PlacementDriverMessagesFactory MSG_FACTORY = new PlacementDriverMessagesFactory();

    private final boolean enabledColocation = IgniteSystemProperties.colocationEnabled();

    private final PartitionGroupId groupId = replicationGroupId(0, 0);

    private static final String NODE_0_NAME = "node0";
    private static final LogicalNode CLUSTER_NODE_0 = new LogicalNode(randomUUID(), NODE_0_NAME, mock(NetworkAddress.class));

    private static final String NODE_1_NAME = "node1";

    private static final LogicalNode CLUSTER_NODE_1 = new LogicalNode(randomUUID(), NODE_1_NAME, mock(NetworkAddress.class));

    private LeaseUpdater leaseUpdater;

    private AssignmentsTracker assignmentsTracker;

    private StandaloneMetaStorageManager metaStorageManager;

    private ClusterService pdClusterService;

    private MessagingService pdMessagingService;

    private LogicalTopologyService pdLogicalTopologyService;

    private LogicalTopologyEventListener pdLogicalTopologyEventListener;

    private BiFunction<String, LeaseGrantedMessage, CompletableFuture<LeaseGrantedMessageResponse>> leaseGrantedMessageHandler;

    private final long assignmentsTimestamp = new HybridTimestamp(0, 1).longValue();

    @InjectConfiguration("mock.leaseAgreementAcceptanceTimeLimitMillis = 2000")
    private ReplicationConfiguration replicationConfiguration;

    private PartitionGroupId replicationGroupId(int objectId, int partId) {
        return enabledColocation ? new ZonePartitionId(objectId, partId) : new TablePartitionId(objectId, partId);
    }

    @BeforeEach
    public void setUp() {
        metaStorageManager = StandaloneMetaStorageManager.create();
        assertThat(metaStorageManager.startAsync(new ComponentContext()), willCompleteSuccessfully());
        metaStorageManager.deployWatches();

        pdLogicalTopologyService = mock(LogicalTopologyService.class);
        when(pdLogicalTopologyService.logicalTopologyOnLeader())
                .thenAnswer(inv -> completedFuture(new LogicalTopologySnapshot(0, Set.of(CLUSTER_NODE_0, CLUSTER_NODE_1))));
        doAnswer(inv -> {
            pdLogicalTopologyEventListener = inv.getArgument(0);

            return null;
        }).when(pdLogicalTopologyService).addEventListener(any());

        leaseUpdater = createLeaseUpdater();

        leaseUpdater.init();

        leaseUpdater.activate();
    }

    @AfterEach
    public void tearDown() {
        leaseUpdater.deactivate();
        assignmentsTracker.stopTrack();

        metaStorageManager.beforeNodeStop();
        assertThat(metaStorageManager.stopAsync(), willCompleteSuccessfully());
    }

    private LeaseUpdater createLeaseUpdater() {
        TopologyService pdTopologyService = mock(TopologyService.class);
        when(pdTopologyService.getById(any(UUID.class))).thenAnswer(inv -> CLUSTER_NODE_0);

        pdMessagingService = mock(MessagingService.class);
        when(pdMessagingService.invoke(anyString(), any(), anyLong())).thenAnswer(inv -> {
            String nodeId = inv.getArgument(0);
            long timeout = inv.getArgument(2);

            LeaseGrantedMessage leaseGrantedMessage = inv.getArgument(1);

            if (leaseGrantedMessageHandler != null) {
                return CompletableFuture.supplyAsync(() -> null)
                        .thenCompose(unused -> leaseGrantedMessageHandler.apply(nodeId, leaseGrantedMessage))
                        .orTimeout(timeout, TimeUnit.MILLISECONDS);
            } else {
                return completedFuture(createLeaseGrantedMessageResponse(true));
            }
        });

        pdClusterService = mock(ClusterService.class);
        when(pdClusterService.messagingService()).thenAnswer(inv -> pdMessagingService);
        when(pdClusterService.topologyService()).thenAnswer(inv -> pdTopologyService);

        var clockService = new TestClockService(new HybridClockImpl());

        LeaseTracker leaseTracker = new LeaseTracker(
                metaStorageManager,
                pdClusterService.topologyService(),
                clockService,
                zoneId -> completedFuture(Set.of()),
                id -> null,
                new SystemPropertiesNodeProperties()
        );

        leaseTracker.startTrack(0L);

        assignmentsTracker = new AssignmentsTracker(
                metaStorageManager,
                mock(FailureProcessor.class),
                new SystemPropertiesNodeProperties(),
                zoneId -> completedFuture(Set.of()),
                zoneId -> null
        );

        assignmentsTracker.startTrack();

        return new LeaseUpdater(
                NODE_0_NAME,
                pdClusterService,
                metaStorageManager,
                mock(FailureProcessor.class),
                pdLogicalTopologyService,
                leaseTracker,
                clockService,
                assignmentsTracker,
                replicationConfiguration,
                Runnable::run
        );
    }

    private static LeaseGrantedMessageResponse createLeaseGrantedMessageResponse(boolean accept) {
        return MSG_FACTORY.leaseGrantedMessageResponse().accepted(accept).build();
    }

    @Test
    public void testAssignmentChangeOnNegotiation() throws InterruptedException {
        var lgmReceived = new CompletableFuture<>();
        var lgmProcessed = new CompletableFuture<>();

        leaseGrantedMessageHandler = (n, lgm) -> {
            if (n.equals(NODE_0_NAME)) {
                lgmReceived.complete(null);

                return lgmProcessed.thenApply(unused -> createLeaseGrantedMessageResponse(true));
            }

            return completedFuture(createLeaseGrantedMessageResponse(true));
        };

        metaStorageManager.put(stableAssignmentsKey(groupId), Assignments.toBytes(Set.of(forPeer(NODE_0_NAME)), assignmentsTimestamp));

        assertThat(lgmReceived, willCompleteSuccessfully());

        metaStorageManager.put(stableAssignmentsKey(groupId), Assignments.toBytes(Set.of(forPeer(NODE_1_NAME)), assignmentsTimestamp));

        waitForAcceptedLease();

        assertLeaseCorrect(CLUSTER_NODE_1.id());

        lgmProcessed.complete(null);
    }

    private ByteArray stableAssignmentsKey(PartitionGroupId groupId) {
        return enabledColocation ? ZoneRebalanceUtil.stablePartAssignmentsKey((ZonePartitionId) groupId)
                : stablePartAssignmentsKey((TablePartitionId) groupId);
    }

    @Test
    public void testAssignmentChangeOnNegotiationAndReplicaRejectsLease() throws InterruptedException {
        var lgmReceived = new CompletableFuture<>();

        leaseGrantedMessageHandler = (n, lgm) -> {
            if (n.equals(NODE_0_NAME) && !lgmReceived.isDone()) {
                lgmReceived.complete(null);

                return completedFuture(createLeaseGrantedMessageResponse(false));
            }

            return completedFuture(createLeaseGrantedMessageResponse(true));
        };

        metaStorageManager.put(stableAssignmentsKey(groupId), Assignments.toBytes(Set.of(forPeer(NODE_0_NAME)), assignmentsTimestamp));

        assertThat(lgmReceived, willCompleteSuccessfully());

        waitForAcceptedLease();

        assertLeaseCorrect(CLUSTER_NODE_0.id());
    }

    @Test
    public void testAssignmentChangeOnNegotiationNodeLeftTopology() throws InterruptedException {
        var lgmReceived = new CompletableFuture<>();
        var lgmProcessed = new CompletableFuture<>();

        leaseGrantedMessageHandler = (n, lgm) -> {
            if (n.equals(NODE_0_NAME)) {
                lgmReceived.complete(null);

                return lgmProcessed.thenApply(unused -> createLeaseGrantedMessageResponse(true));
            }

            return completedFuture(createLeaseGrantedMessageResponse(true));
        };

        metaStorageManager.put(stableAssignmentsKey(groupId),
                Assignments.toBytes(Set.of(forPeer(NODE_0_NAME), forPeer(NODE_1_NAME)), assignmentsTimestamp));

        assertThat(lgmReceived, willCompleteSuccessfully());

        pdLogicalTopologyEventListener.onNodeLeft(CLUSTER_NODE_0, new LogicalTopologySnapshot(1L, Set.of(CLUSTER_NODE_1)));

        waitForAcceptedLease();

        assertLeaseCorrect(CLUSTER_NODE_1.id());

        lgmProcessed.complete(null);
    }

    @Test
    public void testNetworkExceptionOnNegotiation() throws InterruptedException {
        var lgmReceived = new CompletableFuture<>();

        leaseGrantedMessageHandler = (n, lgm) -> {
            if (!lgmReceived.isDone()) {
                lgmReceived.complete(null);

                throw new RuntimeException("test");
            }

            return completedFuture(createLeaseGrantedMessageResponse(true));
        };

        metaStorageManager.put(stableAssignmentsKey(groupId), Assignments.toBytes(Set.of(forPeer(NODE_0_NAME)), assignmentsTimestamp));

        assertThat(lgmReceived, willCompleteSuccessfully());

        waitForAcceptedLease();

        assertLeaseCorrect(CLUSTER_NODE_0.id());
    }

    @Test
    public void testAgreementCannotBeOverriddenWhileValid() throws InterruptedException {
        ConditionalWatchInhibitor watchInhibitor = new ConditionalWatchInhibitor(metaStorageManager);
        watchInhibitor.startInhibit(rev -> getLeaseFromMs() != null);

        CompletableFuture<Void> lgmResponseFuture = new CompletableFuture<>();
        AtomicInteger invokeFailCounter = new AtomicInteger();
        AtomicInteger lgmCounter = new AtomicInteger();

        metaStorageManager.setAfterInvokeInterceptor(res -> {
            if (!res) {
                invokeFailCounter.incrementAndGet();
            }
        });

        AtomicLong negotiatedLeaseStartTime = new AtomicLong();

        leaseGrantedMessageHandler = (n, lgm) -> {
            lgmCounter.incrementAndGet();

            log.info("Lease granted message received [node={}, leaseStartTime={}, cntr={}].", n, lgm.leaseStartTime().longValue(),
                    lgmCounter.get());

            if (negotiatedLeaseStartTime.get() == 0) {
                negotiatedLeaseStartTime.set(lgm.leaseStartTime().longValue());
            }

            return lgmResponseFuture.thenApply(unused -> createLeaseGrantedMessageResponse(true));
        };

        metaStorageManager.put(stableAssignmentsKey(groupId), Assignments.toBytes(Set.of(forPeer(NODE_0_NAME)), assignmentsTimestamp));

        assertTrue(waitForCondition(() -> lgmCounter.get() == 1, 3_000));

        // Wait for a couple of iterations of LeaseUpdater.
        Thread.sleep(1_000);

        lgmResponseFuture.complete(null);
        watchInhibitor.stopInhibit();

        waitForAcceptedLease();

        Lease lease = getLeaseFromMs();

        assertEquals(negotiatedLeaseStartTime.get(), lease.getStartTime().longValue());
    }

    @Test
    public void testAllLeasesAreProlongedIfOneIs() throws InterruptedException {
        leaseGrantedMessageHandler = (n, lgm) -> completedFuture(createLeaseGrantedMessageResponse(true));

        PartitionGroupId groupId0 = replicationGroupId(1, 0);
        PartitionGroupId groupId1 = replicationGroupId(1, 1);

        metaStorageManager.put(stableAssignmentsKey(groupId0), Assignments.toBytes(Set.of(forPeer(NODE_0_NAME)), assignmentsTimestamp));

        waitForAcceptedLease();

        metaStorageManager.put(stableAssignmentsKey(groupId1), Assignments.toBytes(Set.of(forPeer(NODE_0_NAME)), assignmentsTimestamp));

        try {
            assertTrue(waitForCondition(() -> {
                Collection<Lease> leases = getAllLeasesFromMs();

                Lease lease0 = leases.stream().filter(l -> l.replicationGroupId().equals(groupId0)).findAny().orElse(null);
                Lease lease1 = leases.stream().filter(l -> l.replicationGroupId().equals(groupId1)).findAny().orElse(null);

                // Start time of the leases should be different, but their expiration time should be finally the same.
                return lease0 != null
                        && lease1 != null
                        && lease0.isAccepted()
                        && lease1.isAccepted()
                        && lease1.getStartTime().compareTo(lease0.getStartTime()) > 0
                        && lease1.getExpirationTime().equals(lease0.getExpirationTime());
            }, 9000));
        } catch (AssertionError e) {
            Collection<Lease> leases = getAllLeasesFromMs();
            log.warn("Leases from meta storage:");
            leases.forEach(lease -> log.warn("    " + lease));
            throw e;
        }
    }

    @Test
    public void testLeasesCleanup() throws InterruptedException {
        leaseGrantedMessageHandler = (n, lgm) -> completedFuture(createLeaseGrantedMessageResponse(true));

        metaStorageManager.put(stableAssignmentsKey(groupId), Assignments.toBytes(Set.of(forPeer(NODE_0_NAME)), assignmentsTimestamp));

        waitForAcceptedLease();

        metaStorageManager.remove(stableAssignmentsKey(groupId));

        assertTrue(waitForCondition(() -> getAllLeasesFromMs().isEmpty(), 20_000));
    }

    @Test
    public void testLeaseAgreementCleanup() throws Exception {
        CompletableFuture<?> timedOutGroupLgmReceived = new CompletableFuture<>();
        CompletableFuture<?> removedGroupLgmReceived = new CompletableFuture<>();

        PartitionGroupId timedOutGroup = replicationGroupId(1, 1);
        PartitionGroupId removedGroup = replicationGroupId(1, 2);
        byte[] assignmentBytes = Assignments.toBytes(Set.of(forPeer(NODE_0_NAME)), assignmentsTimestamp);

        leaseGrantedMessageHandler = (n, lgm) -> {
            if (lgm.groupId().equals(groupId)) {
                return completedFuture(createLeaseGrantedMessageResponse(true));
            } else if (lgm.groupId().equals(timedOutGroup)) {
                timedOutGroupLgmReceived.complete(null);

                // Return a future that will never be completed, to trigger the agreement timeout.
                return new CompletableFuture<>();
            } else {
                removedGroupLgmReceived.complete(null);
                return new CompletableFuture<>();
            }
        };

        metaStorageManager.put(stableAssignmentsKey(groupId), assignmentBytes);
        metaStorageManager.put(stableAssignmentsKey(timedOutGroup), assignmentBytes);
        metaStorageManager.put(stableAssignmentsKey(removedGroup), assignmentBytes);

        // Wait for accepted lease for groupId.
        assertTrue(waitForCondition(
                () -> getAllLeasesFromMs().stream().anyMatch(l -> l.replicationGroupId().equals(groupId) && l.isAccepted()),
                5000
        ));

        assertThat(timedOutGroupLgmReceived, willSucceedFast());
        assertThat(removedGroupLgmReceived, willSucceedFast());

        metaStorageManager.remove(stableAssignmentsKey(removedGroup));

        LeaseNegotiator leaseNegotiator = getFieldValue(leaseUpdater, "leaseNegotiator");
        Map<ReplicationGroupId, LeaseAgreement> agreementsMap = getFieldValue(leaseNegotiator, "leaseToNegotiate");

        assertNotNull(agreementsMap);

        Lease timedOutAgreementLease = agreementsMap.get(timedOutGroup).getLease();

        assertTrue(
                waitForCondition(() -> {
                    LeaseAgreement t = agreementsMap.get(timedOutGroup);
                    LeaseAgreement r = agreementsMap.get(removedGroup);

                    // As the old agreements are cleaned up, they can be replaced with new ones.
                    return (t == null || t.getLease().getStartTime().longValue() > timedOutAgreementLease.getStartTime().longValue())
                            && r == null;
                }, 10_000),
                format("Agreements: timedOutGroup: {}, removedGroup: {}", agreementsMap.get(timedOutGroup), agreementsMap.get(removedGroup))
        );
    }

    @Test
    public void testLeasesCleanupOfOneGroupFromMultiple() throws InterruptedException {
        leaseGrantedMessageHandler = (n, lgm) -> completedFuture(createLeaseGrantedMessageResponse(true));

        PartitionGroupId groupId0 = replicationGroupId(0, 0);
        PartitionGroupId groupId1 = replicationGroupId(0, 1);

        metaStorageManager.put(stableAssignmentsKey(groupId0), Assignments.toBytes(Set.of(forPeer(NODE_0_NAME)), assignmentsTimestamp));
        metaStorageManager.put(stableAssignmentsKey(groupId1), Assignments.toBytes(Set.of(forPeer(NODE_1_NAME)), assignmentsTimestamp));

        waitForAcceptedLease();
        assertTrue(waitForCondition(() -> getAllLeasesFromMs().stream().allMatch(Lease::isAccepted), 3_000));

        metaStorageManager.remove(stableAssignmentsKey(groupId0));

        assertTrue(waitForCondition(() -> {
            Collection<Lease> leases = getAllLeasesFromMs();
            return leases.size() == 1 && leases.stream()
                    .allMatch(lease -> lease.replicationGroupId().equals(groupId1) && lease.isAccepted());
        }, 20_000));
    }

    @Test
    public void repeatedConnectionIssuesAreNotReported() {
        Predicate<LogEvent> pred = logEvent -> logEvent.getMessage().getFormattedMessage()
                .contains("Lease was not negotiated due to exception");

        LogInspector logInspector = new LogInspector(LeaseNegotiator.class.getName(), pred);

        logInspector.start();

        try {
            var clusterService = mock(ClusterService.class);
            var messagingService = mock(MessagingService.class);

            when(messagingService.invoke(anyString(), any(), anyLong()))
                    .thenAnswer(inv -> failedFuture(new UnresolvableConsistentIdException("test")));

            when(clusterService.messagingService()).thenAnswer(inv -> messagingService);

            var leaseNegotiator = new LeaseNegotiator(clusterService, Runnable::run);

            var startTs = new HybridTimestamp(0, 1);
            var expirationTs = new HybridTimestamp(1000, 1);

            var lease1 = new Lease("testNode", randomUUID(), startTs, expirationTs, new ZonePartitionId(0, 1));
            var lease2 = new Lease("testNode", randomUUID(), startTs, expirationTs, new ZonePartitionId(0, 2));

            leaseNegotiator.negotiate(new LeaseAgreement(lease1, false));
            leaseNegotiator.negotiate(new LeaseAgreement(lease2, false));

            assertEquals(1, logInspector.timesMatched().sum());
        } finally {
            logInspector.stop();
        }
    }

    @Nullable
    private Lease getLeaseFromMs() {
        return getAllLeasesFromMs().stream().findFirst().orElse(null);
    }

    private Collection<Lease> getAllLeasesFromMs() {
        CompletableFuture<Entry> f = metaStorageManager.get(PLACEMENTDRIVER_LEASES_KEY);

        assertThat(f, willSucceedFast());

        Entry e = f.join();

        if (e.empty()) {
            return emptyList();
        }

        LeaseBatch leases = LeaseBatch.fromBytes(e.value());

        return leases.leases();
    }

    private void waitForAcceptedLease() throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            Lease lease = getLeaseFromMs();

            return lease != null && lease.isAccepted();
        }, 10_000));
    }

    private void assertLeaseCorrect(UUID leaseholderId) {
        Lease lease = getLeaseFromMs();

        assertNotNull(lease);
        assertTrue(lease.isAccepted());
        assertEquals(leaseholderId, lease.getLeaseholderId());
    }

    private <T> T getFieldValue(Object o, String fieldName) throws NoSuchFieldException, IllegalAccessException {
        Field f = o.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        return (T) f.get(o);
    }
}
