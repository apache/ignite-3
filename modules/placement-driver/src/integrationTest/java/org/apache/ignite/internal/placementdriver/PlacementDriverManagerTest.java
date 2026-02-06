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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.lang.ByteArray.fromString;
import static org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager.configureCmgManagerToStartMetastorage;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_LEASES_KEY;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.RaftGroupOptionsConfigHelper;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessage;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessageGroup;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.TestLozaFactory;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * There are tests for Placement driver manager.
 */
@ExtendWith(ConfigurationExtension.class)
public class PlacementDriverManagerTest extends BasePlacementDriverTest {
    public static final int PORT = 1234;

    private static final PlacementDriverMessagesFactory PLACEMENT_DRIVER_MESSAGES_FACTORY = new PlacementDriverMessagesFactory();

    private String nodeName;

    /** Another node name. The node name is matched to {@code anotherClusterService}. */
    private String anotherNodeName;

    private HybridClock nodeClock = new HybridClockImpl();

    private ClusterService clusterService;

    private LogicalTopologyServiceTestImpl logicalTopologyService;

    /** This service is used to redirect a lease proposal. */
    private ClusterService anotherClusterService;

    private Loza raftManager;

    private LogStorageFactory partitionsLogStorageFactory;

    private LogStorageFactory msLogStorageFactory;

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private SystemLocalConfiguration systemLocalConfiguration;

    @InjectConfiguration
    private SystemDistributedConfiguration systemDistributedConfiguration;

    @InjectConfiguration
    private ReplicationConfiguration replicationConfiguration;

    private MetaStorageManagerImpl metaStorageManager;

    private PlacementDriverManager placementDriverManager;

    private TestInfo testInfo;

    /** This closure handles {@link LeaseGrantedMessage} to check the placement driver manager behavior. */
    private BiFunction<LeaseGrantedMessage, String, LeaseGrantedMessageResponse> leaseGrantHandler;

    private final AtomicInteger nextZoneId = new AtomicInteger();

    private final long assignmentsTimestamp = new HybridTimestamp(0, 1).longValue();

    @BeforeEach
    public void beforeTest(TestInfo testInfo) {
        this.nodeName = testNodeName(testInfo, PORT);
        this.anotherNodeName = testNodeName(testInfo, PORT + 1);
        this.testInfo = testInfo;

        assertTrue(nodeName.hashCode() < anotherNodeName.hashCode(),
                "Node for the first lease grant message should be determined strictly.");

        startPlacementDriverManager();
    }

    private void startPlacementDriverManager() {
        var nodeFinder = new StaticNodeFinder(Collections.singletonList(new NetworkAddress("localhost", PORT)));

        clusterService = ClusterServiceTestUtils.clusterService(testInfo, PORT, nodeFinder);
        anotherClusterService = ClusterServiceTestUtils.clusterService(testInfo, PORT + 1, nodeFinder);

        anotherClusterService.messagingService().addMessageHandler(
                PlacementDriverMessageGroup.class,
                leaseGrantMessageHandler(anotherNodeName)
        );

        clusterService.messagingService().addMessageHandler(PlacementDriverMessageGroup.class, leaseGrantMessageHandler(nodeName));

        ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);

        Set<String> metastorageNodes = Set.of(clusterService.nodeName());
        when(cmgManager.metaStorageNodes()).thenReturn(completedFuture(metastorageNodes));
        when(cmgManager.metaStorageInfo()).thenReturn(completedFuture(
                new CmgMessagesFactory().metaStorageInfo().metaStorageNodes(metastorageNodes).build()
        ));
        configureCmgManagerToStartMetastorage(cmgManager);

        RaftGroupEventsClientListener eventsClientListener = new RaftGroupEventsClientListener();

        logicalTopologyService = new LogicalTopologyServiceTestImpl(clusterService);

        TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                clusterService,
                logicalTopologyService,
                Loza.FACTORY,
                eventsClientListener
        );

        ComponentWorkingDir workingDir = new ComponentWorkingDir(workDir.resolve("loza"));

        partitionsLogStorageFactory = SharedLogStorageFactoryUtils.create(
                clusterService.nodeName(),
                workingDir.raftLogPath()
        );

        raftManager = TestLozaFactory.create(
                clusterService,
                raftConfiguration,
                systemLocalConfiguration,
                nodeClock,
                eventsClientListener
        );

        var readOperationForCompactionTracker = new ReadOperationForCompactionTracker();

        var storage = new SimpleInMemoryKeyValueStorage(nodeName, readOperationForCompactionTracker);

        ClockService clockService = new TestClockService(nodeClock);

        ComponentWorkingDir metastorageWorkDir = new ComponentWorkingDir(workDir.resolve("metastorage"));

        msLogStorageFactory =
                SharedLogStorageFactoryUtils.create(clusterService.nodeName(), metastorageWorkDir.raftLogPath());

        RaftGroupOptionsConfigurer msRaftConfigurer =
                RaftGroupOptionsConfigHelper.configureProperties(msLogStorageFactory, metastorageWorkDir.metaPath());

        metaStorageManager = new MetaStorageManagerImpl(
                clusterService,
                cmgManager,
                logicalTopologyService,
                raftManager,
                storage,
                nodeClock,
                topologyAwareRaftGroupServiceFactory,
                new NoOpMetricManager(),
                systemDistributedConfiguration,
                msRaftConfigurer,
                readOperationForCompactionTracker
        );

        placementDriverManager = new PlacementDriverManager(
                nodeName,
                metaStorageManager,
                MetastorageGroupId.INSTANCE,
                clusterService,
                cmgManager::metaStorageNodes,
                logicalTopologyService,
                raftManager,
                topologyAwareRaftGroupServiceFactory,
                clockService,
                mock(FailureProcessor.class),
                replicationConfiguration,
                Runnable::run,
                mock(MetricManager.class),
                zoneId -> completedFuture(Set.of())
        );

        ComponentContext componentContext = new ComponentContext();

        assertThat(
                startAsync(componentContext,
                        clusterService,
                        anotherClusterService,
                        partitionsLogStorageFactory,
                        msLogStorageFactory,
                        raftManager,
                        metaStorageManager
                )
                        .thenCompose(unused -> metaStorageManager.recoveryFinishedFuture())
                        .thenCompose(unused -> placementDriverManager.startAsync(componentContext))
                        .thenCompose(unused -> metaStorageManager.notifyRevisionUpdateListenerOnStart())
                        .thenCompose(unused -> metaStorageManager.deployWatches()),
                willCompleteSuccessfully()
        );
    }

    /**
     * Handles a lease grant message.
     *
     * @param handlerNode Node which will handles the message.
     * @return Response message.
     */
    private NetworkMessageHandler leaseGrantMessageHandler(String handlerNode) {
        return (msg, sender, correlationId) -> {
            assert msg instanceof LeaseGrantedMessage : "Message type is unexpected [type=" + msg.getClass().getSimpleName() + ']';

            log.info("Lease is being granted [actor={}, recipient={}, force={}]", sender, handlerNode,
                    ((LeaseGrantedMessage) msg).force());

            LeaseGrantedMessageResponse resp = null;

            if (leaseGrantHandler != null) {
                resp = leaseGrantHandler.apply((LeaseGrantedMessage) msg, handlerNode);
            }

            if (resp == null) {
                resp = PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessageResponse()
                        .accepted(true)
                        .build();
            }

            clusterService.messagingService().respond(sender, resp, correlationId);
        };
    }

    @AfterEach
    public void afterEach() throws Exception {
        stopPlacementDriverManager();
    }

    private void stopPlacementDriverManager() throws Exception {
        List<IgniteComponent> igniteComponents = List.of(
                placementDriverManager,
                metaStorageManager,
                raftManager,
                partitionsLogStorageFactory,
                msLogStorageFactory,
                clusterService,
                anotherClusterService
        );

        closeAll(Stream.concat(
                igniteComponents.stream().filter(Objects::nonNull).map(component -> component::beforeNodeStop),
                Stream.of(() -> assertThat(stopAsync(new ComponentContext(), igniteComponents), willCompleteSuccessfully())))
        );
    }

    @Test
    public void testLeaseCreate() throws Exception {
        PartitionGroupId grpPart0 = createAssignments();

        checkLeaseCreated(grpPart0, false);
    }

    @Test
    public void testLeaseRenew() throws Exception {
        assertThat(
                replicationConfiguration.change(change -> change.changeLeaseAgreementAcceptanceTimeLimitMillis(200)),
                willCompleteSuccessfully()
        );

        PartitionGroupId grpPart0 = createAssignments();

        checkLeaseCreated(grpPart0, false);

        CompletableFuture<Entry> leaseFut = metaStorageManager.get(PLACEMENTDRIVER_LEASES_KEY);

        Lease lease = leaseFromBytes(sync(leaseFut).value(), grpPart0);

        assertNotNull(lease);

        assertTrue(waitForCondition(() -> {
            CompletableFuture<Entry> fut = metaStorageManager.get(PLACEMENTDRIVER_LEASES_KEY);

            Lease leaseRenew = leaseFromBytes(sync(fut).value(), grpPart0);

            return lease.getExpirationTime().compareTo(leaseRenew.getExpirationTime()) < 0;

        }, 10_000));
    }

    private static <T> T sync(CompletableFuture<T> future) {
        assertThat(future, willCompleteSuccessfully());
        return future.join();
    }

    @Test
    public void testLeaseholderUpdate() throws Exception {
        assertThat(
                replicationConfiguration.change(change -> change.changeLeaseAgreementAcceptanceTimeLimitMillis(200)),
                willCompleteSuccessfully()
        );

        PartitionGroupId grpPart0 = createAssignments();

        checkLeaseCreated(grpPart0, false);

        Set<Assignment> assignments = Set.of();

        String stableAssignmentsPrefix = ZoneRebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;

        metaStorageManager.put(fromString(stableAssignmentsPrefix + grpPart0), Assignments.toBytes(assignments, assignmentsTimestamp));

        assertTrue(waitForCondition(() -> {
            CompletableFuture<Entry> fut = metaStorageManager.get(PLACEMENTDRIVER_LEASES_KEY);

            Lease lease = leaseFromBytes(sync(fut).value(), grpPart0);

            return lease.getExpirationTime().compareTo(nodeClock.now()) < 0;

        }, 10_000));

        assignments = calculateAssignmentForPartition(Collections.singleton(nodeName), 1, 2, 1, 1);

        metaStorageManager.put(fromString(stableAssignmentsPrefix + grpPart0), Assignments.toBytes(assignments, assignmentsTimestamp));

        assertTrue(waitForCondition(() -> {
            CompletableFuture<Entry> fut = metaStorageManager.get(PLACEMENTDRIVER_LEASES_KEY);

            Lease lease = leaseFromBytes(sync(fut).value(), grpPart0);

            return lease.getExpirationTime().compareTo(nodeClock.now()) > 0;
        }, 10_000));
    }

    @Test
    public void testPrimaryReplicaEvents() throws Exception {
        PartitionGroupId grpPart0 =
                createAssignments(metaStorageManager, nextZoneId.incrementAndGet(), List.of(nodeName), assignmentsTimestamp);

        Lease lease1 = checkLeaseCreated(grpPart0, true);

        ConcurrentHashMap<UUID, PrimaryReplicaEventParameters> electedEvts = new ConcurrentHashMap<>(2);
        ConcurrentHashMap<UUID, PrimaryReplicaEventParameters> expiredEvts = new ConcurrentHashMap<>(2);

        placementDriverManager.placementDriver().listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, evt -> {
            log.info("Primary replica is elected [grp={}, recipient={}]", evt.groupId(), evt.leaseholderId());
            electedEvts.put(evt.leaseholderId(), evt);

            return falseCompletedFuture();
        });

        placementDriverManager.placementDriver().listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, evt -> {
            log.info("Primary replica is expired [grp={}, recipient={}]", evt.groupId(), evt.leaseholder());

            expiredEvts.put(evt.leaseholderId(), evt);

            return falseCompletedFuture();
        });

        Set<Assignment> assignments = calculateAssignmentForPartition(Collections.singleton(anotherNodeName), 1, 2, 1, 1);

        metaStorageManager.put(
                fromString(ZoneRebalanceUtil.STABLE_ASSIGNMENTS_PREFIX + grpPart0),
                Assignments.toBytes(assignments, assignmentsTimestamp)
        );

        assertTrue(waitForCondition(() -> {
            CompletableFuture<ReplicaMeta> fut = placementDriverManager.placementDriver()
                    .getPrimaryReplica(grpPart0, lease1.getExpirationTime());

            ReplicaMeta meta = sync(fut);

            return meta != null && meta.getLeaseholder().equals(anotherNodeName)
                    && electedEvts.values().stream().anyMatch(v -> v.leaseholder().equals(anotherNodeName))
                    && expiredEvts.values().stream().anyMatch(v -> v.leaseholder().equals(nodeName));
        }, 10_000));

        Lease lease2 = checkLeaseCreated(grpPart0, true);

        assertNotEquals(lease1.getLeaseholder(), lease2.getLeaseholder());

        assertTrue(electedEvts.containsKey(lease2.getLeaseholderId()));
        assertTrue(expiredEvts.containsKey(lease1.getLeaseholderId()));

        // Clear events from the previous lease change to verify only new events after node restart.
        // At this point, the maps can have either size 1 from put above, or size 2 from cluster start and put.
        electedEvts.clear();
        expiredEvts.clear();
        stopAnotherNode(anotherClusterService);
        anotherClusterService = startAnotherNode(anotherNodeName, PORT + 1);

        assertTrue(waitForCondition(() -> {
            CompletableFuture<ReplicaMeta> fut = placementDriverManager.placementDriver()
                    .getPrimaryReplica(grpPart0, lease2.getExpirationTime());

            ReplicaMeta meta = sync(fut);

            return meta != null
                    && meta.getLeaseholderId().equals(anotherClusterService.topologyService().localMember().id())
                    // Check event map sizes to prevent race condition between receiving events and the check above.
                    && electedEvts.size() == 1 && expiredEvts.size() == 1;
        }, 10_000));

        Lease lease3 = checkLeaseCreated(grpPart0, true);

        assertEquals(1, electedEvts.size());
        assertEquals(1, expiredEvts.size());

        assertTrue(electedEvts.containsKey(lease3.getLeaseholderId()));
        assertTrue(expiredEvts.containsKey(lease2.getLeaseholderId()));
    }

    /**
     * Stops another node.
     *
     * @param nodeClusterService Node service to stop.
     * @throws Exception If failed.
     */
    private void stopAnotherNode(ClusterService nodeClusterService) throws Exception {
        nodeClusterService.beforeNodeStop();
        assertThat(nodeClusterService.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        assertTrue(waitForCondition(
                () -> !clusterService.topologyService().allMembers().contains(nodeClusterService.topologyService().localMember()),
                10_000
        ));

        logicalTopologyService.updateTopology();
    }

    /**
     * Starts another node.
     *
     * @param nodeName Node name.
     * @param port Node port.
     * @return Cluster service for the newly started node.
     * @throws Exception If failed.
     */
    private ClusterService startAnotherNode(String nodeName, int port) throws Exception {
        ClusterService nodeClusterService = ClusterServiceTestUtils.clusterService(
                testInfo,
                port,
                new StaticNodeFinder(Collections.singletonList(new NetworkAddress("localhost", PORT)))
        );

        nodeClusterService.messagingService().addMessageHandler(
                PlacementDriverMessageGroup.class,
                leaseGrantMessageHandler(nodeName)
        );

        assertThat(nodeClusterService.startAsync(new ComponentContext()), willCompleteSuccessfully());

        assertTrue(waitForCondition(
                () -> clusterService.topologyService().allMembers().contains(nodeClusterService.topologyService().localMember()),
                10_000
        ));

        logicalTopologyService.updateTopology();

        return nodeClusterService;
    }

    @Test
    public void testLeaseRemovedAfterExpirationAndAssignmetnsRemoval() throws Exception {
        List<PartitionGroupId> groupIds = List.of(
                createAssignments(metaStorageManager, nextZoneId.incrementAndGet(), List.of(nodeName), assignmentsTimestamp),
                createAssignments(metaStorageManager, nextZoneId.incrementAndGet(), List.of(nodeName), assignmentsTimestamp)
        );

        Map<PartitionGroupId, AtomicBoolean> leaseExpirationMap =
                groupIds.stream().collect(Collectors.toMap(id -> id, id -> new AtomicBoolean()));

        groupIds.forEach(groupId -> {
            placementDriverManager.placementDriver().listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, evt -> {
                log.info("Primary replica is expired [grp={}]", groupId);

                leaseExpirationMap.get(groupId).set(true);

                return falseCompletedFuture();
            });
        });

        checkLeaseCreated(groupIds.get(0), true);
        checkLeaseCreated(groupIds.get(1), true);

        assertFalse(leaseExpirationMap.get(groupIds.get(0)).get());
        assertFalse(leaseExpirationMap.get(groupIds.get(1)).get());

        assertThat(metaStorageManager.remove(
                fromString(ZoneRebalanceUtil.STABLE_ASSIGNMENTS_PREFIX + groupIds.get(0))
        ), willCompleteSuccessfully());

        assertTrue(waitForCondition(() -> {
            CompletableFuture<Entry> fut = metaStorageManager.get(PLACEMENTDRIVER_LEASES_KEY);
            Entry entry = sync(fut);

            // Only lease from grpPart0 should be removed.
            return leaseFromBytes(entry.value(), groupIds.get(0)) == null
                    && leaseFromBytes(entry.value(), groupIds.get(1)) != null;

        }, 10_000));
    }

    @Test
    public void testLeaseAccepted() throws Exception {
        PartitionGroupId grpPart0 = createAssignments();

        checkLeaseCreated(grpPart0, true);
    }

    @Test
    public void testLeaseForceAccepted() throws Exception {
        leaseGrantHandler = (req, handler) ->
                PLACEMENT_DRIVER_MESSAGES_FACTORY
                        .leaseGrantedMessageResponse()
                        .accepted(req.force())
                        .build();

        PartitionGroupId grpPart0 = createAssignments();

        checkLeaseCreated(grpPart0, true);
    }

    @Test
    public void testExceptionOnAcceptance() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        leaseGrantHandler = (req, handler) -> {
            latch.countDown();

            throw new RuntimeException("test");
        };

        PartitionGroupId grpPart0 = createAssignments();

        checkLeaseCreated(grpPart0, false);

        latch.await();

        Lease lease = checkLeaseCreated(grpPart0, false);

        assertFalse(lease.isAccepted());
    }

    @Test
    public void testRedirectionAcceptance() throws Exception {
        AtomicReference<String> redirect = new AtomicReference<>();
        AtomicBoolean forceDetected = new AtomicBoolean(false);

        leaseGrantHandler = (req, handler) -> {
            if (req.force()) {
                forceDetected.set(true);
            }

            if (redirect.get() == null) {
                redirect.set(handler.equals(nodeName) ? anotherNodeName : nodeName);
            }

            return PLACEMENT_DRIVER_MESSAGES_FACTORY
                    .leaseGrantedMessageResponse()
                    .accepted(redirect.get().equals(handler))
                    .redirectProposal(redirect.get().equals(handler) ? null : redirect.get())
                    .build();
        };

        PartitionGroupId grpPart0 = createAssignments();

        checkLeaseCreated(grpPart0, true);

        if (forceDetected.get()) {
            fail("Unexpected force leaseGrantedMessage detected");
        }
    }

    @Test
    public void testLeaseRestore() throws Exception {
        PartitionGroupId grpPart0 = createAssignments();

        checkLeaseCreated(grpPart0, false);

        stopPlacementDriverManager();
        startPlacementDriverManager();

        checkLeaseCreated(grpPart0, false);
    }

    @Test
    public void testLeaseMatchGrantMessage() throws Exception {
        var leaseGrantReqRef = new AtomicReference<LeaseGrantedMessage>();

        leaseGrantHandler = (req, handler) -> {
            leaseGrantReqRef.set(req);

            return null;
        };

        PartitionGroupId grpPart0 = createAssignments();

        Lease lease = checkLeaseCreated(grpPart0, false);

        assertTrue(waitForCondition(() -> leaseGrantReqRef.get() != null, 10_000));

        assertEquals(leaseGrantReqRef.get().leaseStartTime(), lease.getStartTime());
        assertTrue(leaseGrantReqRef.get().leaseExpirationTime().compareTo(lease.getExpirationTime()) >= 0);
    }

    /**
     * Checks if a group lease is created during the timeout.
     *
     * @param grpPartId Replication group id.
     * @param waitAccept Await a lease with the accepted flag.
     * @return A lease that is read from Meta storage.
     * @throws InterruptedException If the waiting is interrupted.
     */
    private Lease checkLeaseCreated(PartitionGroupId grpPartId, boolean waitAccept) throws InterruptedException {
        AtomicReference<Lease> leaseRef = new AtomicReference<>();

        assertTrue(waitForCondition(() -> {
            CompletableFuture<Entry> leaseFut = metaStorageManager.get(PLACEMENTDRIVER_LEASES_KEY);

            Entry leaseEntry = sync(leaseFut);

            if (leaseEntry != null && !leaseEntry.empty()) {
                Lease lease = leaseFromBytes(leaseEntry.value(), grpPartId);

                if (lease == null) {
                    return false;
                }

                if (!waitAccept) {
                    leaseRef.set(lease);
                } else if (lease.isAccepted()) {
                    leaseRef.set(lease);
                }
            }

            return leaseRef.get() != null;
        }, 10_000));

        return leaseRef.get();
    }

    /**
     * Creates an assignment for the fake table.
     *
     * @return Replication group id.
     */
    private PartitionGroupId createAssignments() {
        return createAssignments(
                metaStorageManager,
                nextZoneId.incrementAndGet(),
                List.of(nodeName, anotherNodeName),
                assignmentsTimestamp
        );
    }

    /**
     * Test implementation of {@link LogicalTopologyService}.
     */
    protected static class LogicalTopologyServiceTestImpl implements LogicalTopologyService {
        private final ClusterService clusterService;

        private List<LogicalTopologyEventListener> listeners;

        private int ver = 1;

        public LogicalTopologyServiceTestImpl(ClusterService clusterService) {
            this.clusterService = clusterService;
            this.listeners = new ArrayList<>();
        }

        @Override
        public void addEventListener(LogicalTopologyEventListener listener) {
            this.listeners.add(listener);
        }

        @Override
        public void removeEventListener(LogicalTopologyEventListener listener) {
            this.listeners.remove(listener);
        }

        /**
         * Updates logical topology to the physical one.
         */
        public void updateTopology() {
            if (listeners != null) {
                var topologySnapshot = new LogicalTopologySnapshot(
                        ++ver,
                        clusterService.topologyService().allMembers().stream().map(LogicalNode::new).collect(toSet())
                );

                listeners.forEach(lnsr -> lnsr.onTopologyLeap(topologySnapshot));
            }
        }

        @Override
        public CompletableFuture<LogicalTopologySnapshot> logicalTopologyOnLeader() {
            return completedFuture(new LogicalTopologySnapshot(
                    ver,
                    clusterService.topologyService().allMembers().stream().map(LogicalNode::new).collect(toSet()))
            );
        }

        @Override
        public LogicalTopologySnapshot localLogicalTopology() {
            return new LogicalTopologySnapshot(
                    ver,
                    clusterService.topologyService().allMembers().stream().map(LogicalNode::new).collect(toSet()));
        }

        @Override
        public CompletableFuture<Set<InternalClusterNode>> validatedNodesOnLeader() {
            return completedFuture(Set.copyOf(clusterService.topologyService().allMembers()));
        }
    }
}
