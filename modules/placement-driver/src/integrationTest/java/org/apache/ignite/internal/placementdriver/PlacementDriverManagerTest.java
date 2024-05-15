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
import static org.apache.ignite.internal.affinity.AffinityUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.lang.ByteArray.fromString;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessage;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessageGroup;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.network.ClusterNode;
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

    protected static final TablePartitionId GROUP_ID = new TablePartitionId(1, 0);

    protected static final ZonePartitionId ZONE_GROUP_ID = new ZonePartitionId(1, 0);

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

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private MetaStorageConfiguration metaStorageConfiguration;

    private MetaStorageManagerImpl metaStorageManager;

    private PlacementDriverManager placementDriverManager;

    private TestInfo testInfo;

    /** This closure handles {@link LeaseGrantedMessage} to check the placement driver manager behavior. */
    private BiFunction<LeaseGrantedMessage, String, LeaseGrantedMessageResponse> leaseGrantHandler;

    private final AtomicInteger nextTableId = new AtomicInteger();

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

        when(cmgManager.metaStorageNodes()).thenReturn(completedFuture(Set.of(clusterService.nodeName())));

        RaftGroupEventsClientListener eventsClientListener = new RaftGroupEventsClientListener();

        logicalTopologyService = new LogicalTopologyServiceTestImpl(clusterService);

        TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                clusterService,
                logicalTopologyService,
                Loza.FACTORY,
                eventsClientListener
        );

        raftManager = new Loza(
                clusterService,
                new NoOpMetricManager(),
                raftConfiguration,
                workDir.resolve("loza"),
                nodeClock,
                eventsClientListener
        );

        var storage = new SimpleInMemoryKeyValueStorage(nodeName);

        metaStorageManager = new MetaStorageManagerImpl(
                clusterService,
                cmgManager,
                logicalTopologyService,
                raftManager,
                storage,
                nodeClock,
                topologyAwareRaftGroupServiceFactory,
                new NoOpMetricManager(),
                metaStorageConfiguration
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
                new TestClockService(nodeClock),
                grp -> ZONE_GROUP_ID
        );

        assertThat(
                startAsync(clusterService, anotherClusterService, raftManager, metaStorageManager)
                        .thenCompose(unused -> metaStorageManager.recoveryFinishedFuture())
                        .thenCompose(unused -> placementDriverManager.startAsync())
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
                        .appliedGroups(Set.of(GROUP_ID))
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
                clusterService,
                anotherClusterService
        );

        closeAll(Stream.concat(
                igniteComponents.stream().filter(Objects::nonNull).map(component -> component::beforeNodeStop),
                Stream.of(() -> assertThat(stopAsync(igniteComponents), willCompleteSuccessfully())))
        );
    }

    @Test
    public void testLeaseCreate() throws Exception {
        ZonePartitionId grpPart0 = createZoneAssignment();

        checkLeaseCreated(grpPart0, false);
    }

    @Test
    @WithSystemProperty(key = "IGNITE_LONG_LEASE", value = "200")
    public void testLeaseRenew() throws Exception {
        ZonePartitionId grpPart0 = createZoneAssignment();

        checkLeaseCreated(grpPart0, false);

        var leaseFut = metaStorageManager.get(PLACEMENTDRIVER_LEASES_KEY);

        Lease lease = leaseFromBytes(leaseFut.join().value(), grpPart0);

        assertNotNull(lease);

        assertTrue(waitForCondition(() -> {
            var fut = metaStorageManager.get(PLACEMENTDRIVER_LEASES_KEY);

            Lease leaseRenew = leaseFromBytes(fut.join().value(), grpPart0);

            return lease.getExpirationTime().compareTo(leaseRenew.getExpirationTime()) < 0;

        }, 10_000));
    }

    @Test
    @WithSystemProperty(key = "IGNITE_LONG_LEASE", value = "200")
    public void testLeaseholderUpdate() throws Exception {
        ZonePartitionId grpPart0 = createZoneAssignment();

        checkLeaseCreated(grpPart0, false);

        Set<Assignment> assignments = Set.of();

        metaStorageManager.put(fromString(STABLE_ASSIGNMENTS_PREFIX + grpPart0), Assignments.toBytes(assignments));

        assertTrue(waitForCondition(() -> {
            var fut = metaStorageManager.get(PLACEMENTDRIVER_LEASES_KEY);

            Lease lease = leaseFromBytes(fut.join().value(), grpPart0);

            return lease.getExpirationTime().compareTo(nodeClock.now()) < 0;

        }, 10_000));

        assignments = calculateAssignmentForPartition(Collections.singleton(nodeName), 1, 1);

        metaStorageManager.put(fromString(STABLE_ASSIGNMENTS_PREFIX + grpPart0), Assignments.toBytes(assignments));

        assertTrue(waitForCondition(() -> {
            var fut = metaStorageManager.get(PLACEMENTDRIVER_LEASES_KEY);

            Lease lease = leaseFromBytes(fut.join().value(), grpPart0);

            return lease.getExpirationTime().compareTo(nodeClock.now()) > 0;
        }, 10_000));
    }

    @Test
    public void testPrimaryReplicaEvents() throws Exception {
        ZonePartitionId grpPart0 = createZoneAssignment(metaStorageManager, nextTableId.incrementAndGet(), List.of(nodeName));

        Lease lease1 = checkLeaseCreated(grpPart0, true);

        ConcurrentHashMap<String, HybridTimestamp> electedEvts = new ConcurrentHashMap<>(2);
        ConcurrentHashMap<String, HybridTimestamp> expiredEvts = new ConcurrentHashMap<>(2);

        placementDriverManager.placementDriver().listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, evt -> {
            log.info("Primary replica is elected [grp={}]", evt.groupId());

            electedEvts.put(evt.leaseholderId(), evt.startTime());

            return falseCompletedFuture();
        });

        placementDriverManager.placementDriver().listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, evt -> {
            log.info("Primary replica is expired [grp={}]", evt.groupId());

            expiredEvts.put(evt.leaseholderId(), evt.startTime());

            return falseCompletedFuture();
        });

        Set<Assignment> assignments = calculateAssignmentForPartition(Collections.singleton(anotherNodeName), 1, 1);

        metaStorageManager.put(fromString(STABLE_ASSIGNMENTS_PREFIX + grpPart0), Assignments.toBytes(assignments));

        assertTrue(waitForCondition(() -> {
            CompletableFuture<ReplicaMeta> fut = placementDriverManager.placementDriver()
                    .getPrimaryReplica(GROUP_ID, lease1.getExpirationTime());

            ReplicaMeta meta = fut.join();

            return meta != null && meta.getLeaseholder().equals(anotherNodeName);
        }, 10_000));

        Lease lease2 = checkLeaseCreated(grpPart0, true);

        assertNotEquals(lease1.getLeaseholder(), lease2.getLeaseholder());

        assertEquals(1, electedEvts.size());
        assertEquals(1, expiredEvts.size());

        assertTrue(electedEvts.containsKey(lease2.getLeaseholderId()));
        assertTrue(expiredEvts.containsKey(lease1.getLeaseholderId()));

        stopAnotherNode(anotherClusterService);
        anotherClusterService = startAnotherNode(anotherNodeName, PORT + 1);

        assertTrue(waitForCondition(() -> {
            CompletableFuture<ReplicaMeta> fut = placementDriverManager.placementDriver()
                    .getPrimaryReplica(GROUP_ID, lease2.getExpirationTime());

            ReplicaMeta meta = fut.join();

            return meta != null && meta.getLeaseholderId().equals(anotherClusterService.topologyService().localMember().id());
        }, 10_000));

        Lease lease3 = checkLeaseCreated(grpPart0, true);

        assertEquals(2, electedEvts.size());
        assertEquals(2, expiredEvts.size());

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
        assertThat(nodeClusterService.stopAsync(), willCompleteSuccessfully());

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

        assertThat(nodeClusterService.startAsync(), willCompleteSuccessfully());

        assertTrue(waitForCondition(
                () -> clusterService.topologyService().allMembers().contains(nodeClusterService.topologyService().localMember()),
                10_000
        ));

        logicalTopologyService.updateTopology();

        return nodeClusterService;
    }

    @Test
    public void testLeaseRemovedAfterExpirationAndAssignmetnsRemoval() throws Exception {
        List<ZonePartitionId> groupIds = List.of(
                createZoneAssignment(metaStorageManager, nextTableId.incrementAndGet(), List.of(nodeName)),
                createZoneAssignment(metaStorageManager, nextTableId.incrementAndGet(), List.of(nodeName))
        );

        Map<ZonePartitionId, AtomicBoolean> leaseExpirationMap =
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

        metaStorageManager.remove(fromString(STABLE_ASSIGNMENTS_PREFIX + groupIds.get(0)));

        assertTrue(waitForCondition(() -> {
            var fut = metaStorageManager.get(PLACEMENTDRIVER_LEASES_KEY);

            // Only lease from grpPart0 should be removed.
            return leaseFromBytes(fut.join().value(), groupIds.get(0)) == null
                    && leaseFromBytes(fut.join().value(), groupIds.get(1)) != null;

        }, 10_000));
    }

    @Test
    public void testLeaseAccepted() throws Exception {
        ZonePartitionId grpPart0 = createZoneAssignment();

        checkLeaseCreated(grpPart0, true);
    }

    @Test
    public void testLeaseForceAccepted() throws Exception {
        leaseGrantHandler = (req, handler) ->
                PLACEMENT_DRIVER_MESSAGES_FACTORY
                        .leaseGrantedMessageResponse()
                        .appliedGroups(Set.of(GROUP_ID))
                        .accepted(req.force())
                        .build();

        ZonePartitionId grpPart0 = createZoneAssignment();

        checkLeaseCreated(grpPart0, true);
    }

    @Test
    public void testExceptionOnAcceptance() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        leaseGrantHandler = (req, handler) -> {
            latch.countDown();

            throw new RuntimeException("test");
        };

        ZonePartitionId grpPart0 = createZoneAssignment();

        checkLeaseCreated(grpPart0, false);

        latch.await();

        Lease lease = checkLeaseCreated(grpPart0, false);

        assertFalse(lease.isAccepted());
    }

    @Test
    public void testRedirectionAcceptance() throws Exception {
        AtomicReference<String> redirect = new AtomicReference<>();

        leaseGrantHandler = (req, handler) -> {
            if (redirect.get() == null) {
                redirect.set(handler.equals(nodeName) ? anotherNodeName : nodeName);

                return PLACEMENT_DRIVER_MESSAGES_FACTORY
                        .leaseGrantedMessageResponse()
                        .appliedGroups(Set.of(GROUP_ID))
                        .accepted(false)
                        .redirectProposal(redirect.get())
                        .build();
            } else {
                return PLACEMENT_DRIVER_MESSAGES_FACTORY
                        .leaseGrantedMessageResponse()
                        .appliedGroups(Set.of(GROUP_ID))
                        .accepted(redirect.get().equals(handler))
                        .build();
            }
        };

        ZonePartitionId grpPart0 = createZoneAssignment();

        checkLeaseCreated(grpPart0, true);
    }

    @Test
    public void testLeaseRestore() throws Exception {
        ZonePartitionId grpPart0 = createZoneAssignment();

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

        ZonePartitionId grpPart0 = createZoneAssignment();

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
    private Lease checkLeaseCreated(ZonePartitionId grpPartId, boolean waitAccept) throws InterruptedException {
        AtomicReference<Lease> leaseRef = new AtomicReference<>();

        assertTrue(waitForCondition(() -> {
            var leaseFut = metaStorageManager.get(PLACEMENTDRIVER_LEASES_KEY);

            var leaseEntry = leaseFut.join();

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
    private ZonePartitionId createZoneAssignment() {
        return createZoneAssignment(metaStorageManager, nextTableId.incrementAndGet(), List.of(nodeName, anotherNodeName));
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
        public CompletableFuture<Set<ClusterNode>> validatedNodesOnLeader() {
            return completedFuture(Set.copyOf(clusterService.topologyService().allMembers()));
        }
    }
}
