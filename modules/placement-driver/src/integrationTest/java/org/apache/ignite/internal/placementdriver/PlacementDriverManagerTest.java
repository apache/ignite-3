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
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_LEASES_KEY;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.utils.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.lang.ByteArray.fromString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessage;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessageGroup;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.utils.ClusterServiceTestUtils;
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

    private HybridClock clock = new HybridClockImpl();

    private VaultManager vaultManager;

    private ClusterService clusterService;

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
    public void beforeTest(TestInfo testInfo) throws NodeStoppingException {
        this.nodeName = testNodeName(testInfo, PORT);
        this.anotherNodeName = testNodeName(testInfo, PORT + 1);
        this.testInfo = testInfo;

        assertTrue(nodeName.hashCode() < anotherNodeName.hashCode(),
                "Node for the first lease grant message should be determined strictly.");

        startPlacementDriverManager();
    }

    private void startPlacementDriverManager() {
        vaultManager = new VaultManager(new PersistentVaultService(testNodeName(testInfo, PORT), workDir.resolve("vault")));

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

        LogicalTopologyService logicalTopologyService = new LogicalTopologyServiceTestImpl(clusterService);

        TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                clusterService,
                logicalTopologyService,
                Loza.FACTORY,
                eventsClientListener
        );

        HybridClock nodeClock = new HybridClockImpl();
        raftManager = new Loza(
                clusterService,
                raftConfiguration,
                workDir.resolve("loza"),
                nodeClock,
                eventsClientListener
        );

        var storage = new SimpleInMemoryKeyValueStorage(nodeName);

        metaStorageManager = new MetaStorageManagerImpl(
                vaultManager,
                clusterService,
                cmgManager,
                logicalTopologyService,
                raftManager,
                storage,
                nodeClock,
                topologyAwareRaftGroupServiceFactory,
                metaStorageConfiguration
        );

        placementDriverManager = new PlacementDriverManager(
                nodeName,
                metaStorageManager,
                vaultManager,
                MetastorageGroupId.INSTANCE,
                clusterService,
                cmgManager::metaStorageNodes,
                logicalTopologyService,
                raftManager,
                topologyAwareRaftGroupServiceFactory,
                clock
        );

        vaultManager.start();
        clusterService.start();
        anotherClusterService.start();
        raftManager.start();
        metaStorageManager.start();
        placementDriverManager.start();

        assertThat("Watches were not deployed", metaStorageManager.deployWatches(), willCompleteSuccessfully());
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
        placementDriverManager.beforeNodeStop();
        metaStorageManager.beforeNodeStop();
        raftManager.beforeNodeStop();
        clusterService.beforeNodeStop();
        vaultManager.beforeNodeStop();

        placementDriverManager.stop();
        metaStorageManager.stop();
        raftManager.stop();
        anotherClusterService.stop();
        clusterService.stop();
        vaultManager.stop();
    }

    @Test
    public void testLeaseCreate() throws Exception {
        TablePartitionId grpPart0 = createTableAssignment();

        checkLeaseCreated(grpPart0, false);
    }

    @Test
    @WithSystemProperty(key = "IGNITE_LONG_LEASE", value = "200")
    public void testLeaseRenew() throws Exception {
        TablePartitionId grpPart0 = createTableAssignment();

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
        TablePartitionId grpPart0 = createTableAssignment();

        checkLeaseCreated(grpPart0, false);

        Set<Assignment> assignments = Set.of();

        metaStorageManager.put(fromString(STABLE_ASSIGNMENTS_PREFIX + grpPart0), ByteUtils.toBytes(assignments));

        assertTrue(waitForCondition(() -> {
            var fut = metaStorageManager.get(PLACEMENTDRIVER_LEASES_KEY);

            Lease lease = leaseFromBytes(fut.join().value(), grpPart0);

            return lease.getExpirationTime().compareTo(clock.now()) < 0;

        }, 10_000));

        assignments = calculateAssignmentForPartition(Collections.singleton(nodeName), 1, 1);

        metaStorageManager.put(fromString(STABLE_ASSIGNMENTS_PREFIX + grpPart0), ByteUtils.toBytes(assignments));

        assertTrue(waitForCondition(() -> {
            var fut = metaStorageManager.get(PLACEMENTDRIVER_LEASES_KEY);

            Lease lease = leaseFromBytes(fut.join().value(), grpPart0);

            return lease.getExpirationTime().compareTo(clock.now()) > 0;
        }, 10_000));
    }

    @Test
    public void testLeaseAccepted() throws Exception {
        TablePartitionId grpPart0 = createTableAssignment();

        checkLeaseCreated(grpPart0, true);
    }

    @Test
    public void testLeaseForceAccepted() throws Exception {
        leaseGrantHandler = (req, handler) ->
                PLACEMENT_DRIVER_MESSAGES_FACTORY
                        .leaseGrantedMessageResponse()
                        .accepted(req.force())
                        .build();

        TablePartitionId grpPart0 = createTableAssignment();

        checkLeaseCreated(grpPart0, true);
    }

    @Test
    public void testExceptionOnAcceptance() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        leaseGrantHandler = (req, handler) -> {
            latch.countDown();

            throw new RuntimeException("test");
        };

        TablePartitionId grpPart0 = createTableAssignment();

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
                        .accepted(false)
                        .redirectProposal(redirect.get())
                        .build();
            } else {
                return PLACEMENT_DRIVER_MESSAGES_FACTORY
                        .leaseGrantedMessageResponse()
                        .accepted(redirect.get().equals(handler))
                        .build();
            }
        };

        TablePartitionId grpPart0 = createTableAssignment();

        checkLeaseCreated(grpPart0, true);
    }

    @Test
    public void testLeaseRestore() throws Exception {
        TablePartitionId grpPart0 = createTableAssignment();

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

        TablePartitionId grpPart0 = createTableAssignment();

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
    private Lease checkLeaseCreated(TablePartitionId grpPartId, boolean waitAccept) throws InterruptedException {
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
    private TablePartitionId createTableAssignment() {
        return createTableAssignment(metaStorageManager, nextTableId.incrementAndGet(), List.of(nodeName, anotherNodeName));
    }

    /**
     * Test implementation of {@link LogicalTopologyService}.
     */
    protected static class LogicalTopologyServiceTestImpl implements LogicalTopologyService {
        private final ClusterService clusterService;

        private List<LogicalTopologyEventListener> listeners;

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
                var top = clusterService.topologyService().allMembers().stream().map(LogicalNode::new).collect(toSet());

                listeners.forEach(lnsr -> lnsr.onTopologyLeap(new LogicalTopologySnapshot(2, top)));
            }
        }

        @Override
        public CompletableFuture<LogicalTopologySnapshot> logicalTopologyOnLeader() {
            return completedFuture(new LogicalTopologySnapshot(
                    1,
                    clusterService.topologyService().allMembers().stream().map(LogicalNode::new).collect(toSet()))
            );
        }

        @Override
        public CompletableFuture<Set<ClusterNode>> validatedNodesOnLeader() {
            return completedFuture(Set.copyOf(clusterService.topologyService().allMembers()));
        }
    }
}
