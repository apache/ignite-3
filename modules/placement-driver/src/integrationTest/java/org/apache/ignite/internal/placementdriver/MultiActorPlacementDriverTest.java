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
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_LEASES_KEY;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.placementdriver.PlacementDriverManagerTest.LogicalTopologyServiceTestImpl;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessage;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessageGroup;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverReplicaMessage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.IgniteTriFunction;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * There are tests of muti-nodes for placement driver.
 */
@ExtendWith(ConfigurationExtension.class)
public class MultiActorPlacementDriverTest extends BasePlacementDriverTest {
    public static final int BASE_PORT = 1234;

    private static final PlacementDriverMessagesFactory PLACEMENT_DRIVER_MESSAGES_FACTORY = new PlacementDriverMessagesFactory();

    private final HybridClock clock = new HybridClockImpl();

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private MetaStorageConfiguration metaStorageConfiguration;

    private List<String> placementDriverNodeNames;

    private List<String> nodeNames;

    private List<Closeable> servicesToClose;

    /** The manager is used to read a data from Meta storage in the tests. */
    private MetaStorageManagerImpl metaStorageManager;

    /** Cluster service by node name. */
    private Map<String, ClusterService> clusterServices;

    /** This closure handles {@link LeaseGrantedMessage} to check the placement driver manager behavior. */
    private IgniteTriFunction<LeaseGrantedMessage, String, String, LeaseGrantedMessageResponse> leaseGrantHandler;

    private final AtomicInteger nextTableId = new AtomicInteger(1);

    @BeforeEach
    public void beforeTest(TestInfo testInfo, @WorkDirectory Path workDir) {
        this.placementDriverNodeNames = IntStream.range(BASE_PORT, BASE_PORT + 3).mapToObj(port -> testNodeName(testInfo, port))
                .collect(Collectors.toList());
        this.nodeNames = IntStream.range(BASE_PORT, BASE_PORT + 5).mapToObj(port -> testNodeName(testInfo, port))
                .collect(Collectors.toList());

        this.clusterServices = startNodes();

        List<LogicalTopologyServiceTestImpl> logicalTopManagers = new ArrayList<>();

        servicesToClose = startPlacementDriver(clusterServices, logicalTopManagers, workDir);

        for (String nodeName : nodeNames) {
            if (!placementDriverNodeNames.contains(nodeName)) {
                var service = clusterServices.get(nodeName);

                service.start();

                servicesToClose.add(() -> {
                    try {
                        service.beforeNodeStop();

                        service.stop();
                    } catch (Exception e) {
                        log.info("Fail to stop services [node={}]", e, nodeName);
                    }
                });
            }
        }

        logicalTopManagers.forEach(LogicalTopologyServiceTestImpl::updateTopology);
    }

    @AfterEach
    public void afterTest() throws Exception {
        IgniteUtils.closeAll(servicesToClose);
    }

    /**
     * Handles a lease grant message.
     *
     * @param handlerService Node service which will handles the message.
     * @return Response message.
     */
    private NetworkMessageHandler leaseGrantMessageHandler(ClusterService handlerService) {
        return (msg, sender, correlationId) -> {
            if (!(msg instanceof PlacementDriverReplicaMessage)) {
                return;
            }

            var handlerNode = handlerService.topologyService().localMember();

            log.info("Lease is being granted [actor={}, recipient={}, force={}]", sender, handlerNode.name(),
                    ((LeaseGrantedMessage) msg).force());

            LeaseGrantedMessageResponse resp = null;

            if (leaseGrantHandler != null) {
                resp = leaseGrantHandler.apply((LeaseGrantedMessage) msg, sender, handlerNode.name());
            }

            if (resp == null) {
                resp = PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessageResponse()
                        .accepted(true)
                        .build();
            }

            handlerService.messagingService().respond(sender, resp, correlationId);
        };
    }

    /**
     * Starts cluster nodes.
     *
     * @return Cluster services.
     */
    public Map<String, ClusterService> startNodes() {
        var res = new HashMap<String, ClusterService>(nodeNames.size());

        var nodeFinder = new StaticNodeFinder(IntStream.range(BASE_PORT, BASE_PORT + 5)
                .mapToObj(p -> new NetworkAddress("localhost", p))
                .collect(Collectors.toList()));

        int port = BASE_PORT;

        for (String nodeName : nodeNames) {
            var srvc = ClusterServiceTestUtils.clusterService(nodeName, port++, nodeFinder);

            srvc.messagingService().addMessageHandler(PlacementDriverMessageGroup.class, leaseGrantMessageHandler(srvc));

            res.put(nodeName, srvc);
        }

        return res;
    }

    /**
     * Starts placement driver.
     *
     * @param services Cluster services.
     * @param logicalTopManagers The list to update in the method. The list might be used for driving of the logical topology.
     * @return List of closures to stop the services.
     */
    public List<Closeable> startPlacementDriver(
            Map<String, ClusterService> services,
            List<LogicalTopologyServiceTestImpl> logicalTopManagers,
            Path workDir
    ) {
        var res = new ArrayList<Closeable>(placementDriverNodeNames.size());

        var msFutures = new CompletableFuture[placementDriverNodeNames.size()];

        for (int i = 0; i < placementDriverNodeNames.size(); i++) {
            String nodeName = placementDriverNodeNames.get(i);
            var vaultManager = new VaultManager(new InMemoryVaultService());
            var clusterService = services.get(nodeName);

            ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);

            when(cmgManager.metaStorageNodes()).thenReturn(completedFuture(new HashSet<>(placementDriverNodeNames)));

            RaftGroupEventsClientListener eventsClientListener = new RaftGroupEventsClientListener();

            var logicalTopologyService = new LogicalTopologyServiceTestImpl(clusterService);

            logicalTopManagers.add(logicalTopologyService);

            TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                    clusterService,
                    logicalTopologyService,
                    Loza.FACTORY,
                    eventsClientListener
            );

            HybridClock nodeClock = new HybridClockImpl();

            var raftManager = new Loza(
                    clusterService,
                    raftConfiguration,
                    workDir.resolve(nodeName + "_loza"),
                    nodeClock,
                    eventsClientListener
            );

            var storage = new SimpleInMemoryKeyValueStorage(nodeName);

            var metaStorageManager = new MetaStorageManagerImpl(
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

            if (this.metaStorageManager == null) {
                this.metaStorageManager = metaStorageManager;
            }

            var placementDriverManager = new PlacementDriverManager(
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
            raftManager.start();
            metaStorageManager.start();
            placementDriverManager.start();

            msFutures[i] = metaStorageManager.deployWatches();

            res.add(() -> {
                        try {
                            placementDriverManager.beforeNodeStop();
                            metaStorageManager.beforeNodeStop();
                            raftManager.beforeNodeStop();
                            clusterService.beforeNodeStop();
                            vaultManager.beforeNodeStop();

                            placementDriverManager.stop();
                            metaStorageManager.stop();
                            raftManager.stop();
                            clusterService.stop();
                            vaultManager.stop();
                        } catch (Exception e) {
                            log.info("Fail to stop services [node={}]", e, nodeName);
                        }
                    }
            );
        }

        assertThat("Nodes were not started", CompletableFuture.allOf(msFutures), willCompleteSuccessfully());

        return res;
    }

    @Test
    public void testLeaseCreate() throws Exception {
        TablePartitionId grpPart0 = createTableAssignment();

        checkLeaseCreated(grpPart0, true);
    }

    @Test
    public void testLeaseProlong() throws Exception {
        var acceptedNodeRef = new AtomicReference<String>();

        leaseGrantHandler = (msg, from, to) -> {
            acceptedNodeRef.compareAndSet(null, to);

            return PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessageResponse()
                    .accepted(true)
                    .build();
        };

        TablePartitionId grpPart0 = createTableAssignment();

        Lease lease = checkLeaseCreated(grpPart0, true);
        Lease leaseRenew = waitForProlong(grpPart0, lease);

        assertEquals(acceptedNodeRef.get(), leaseRenew.getLeaseholder());
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19325")
    public void prolongAfterActiveActorChanger() throws Exception {
        var acceptedNodeRef = new AtomicReference<String>();

        leaseGrantHandler = (msg, from, to) -> {
            acceptedNodeRef.compareAndSet(null, to);

            return PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessageResponse()
                    .accepted(true)
                    .build();
        };

        TablePartitionId grpPart0 = createTableAssignment();

        Lease lease = checkLeaseCreated(grpPart0, true);

        var msRaftClient = metaStorageManager.getService().raftGroupService();

        msRaftClient.refreshLeader().join();

        var previousLeader = msRaftClient.leader();

        var newLeader = msRaftClient.peers().stream().filter(peer -> !peer.equals(previousLeader)).findAny().get();

        log.info("Leader transfer [from={}, to={}]", previousLeader, newLeader);

        msRaftClient.transferLeadership(newLeader).get();

        Lease leaseRenew = waitForProlong(grpPart0, lease);
    }


    @Test
    public void testLeaseProlongAfterRedirect() throws Exception {
        var declinedNodeRef = new AtomicReference<String>();
        var acceptedNodeRef = new AtomicReference<String>();

        leaseGrantHandler = (msg, from, to) -> {
            if (declinedNodeRef.compareAndSet(null, to)) {
                var redirectNode = nodeNames.stream().filter(nodeName -> !nodeName.equals(to)).findAny().get();

                acceptedNodeRef.compareAndSet(null, redirectNode);

                log.info("Leaseholder candidate proposes other node to hold the lease [candidate={}, proposal={}]", to, redirectNode);

                return PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessageResponse()
                        .accepted(false)
                        .redirectProposal(redirectNode)
                        .build();
            } else {
                log.info("Lease is accepted [leaseholder={}]", to);

                return PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessageResponse()
                        .accepted(true)
                        .build();
            }
        };

        TablePartitionId grpPart0 = createTableAssignment();

        Lease lease = checkLeaseCreated(grpPart0, true);

        assertEquals(lease.getLeaseholder(), acceptedNodeRef.get());

        waitForProlong(grpPart0, lease);
    }

    @Test
    public void testDeclineLeaseByLeaseholder() throws Exception {
        var acceptedNodeRef = new AtomicReference<String>();
        var activeActorRef = new AtomicReference<String>();

        leaseGrantHandler = (msg, from, to) -> {
            acceptedNodeRef.set(to);
            activeActorRef.set(from);

            return PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessageResponse()
                    .accepted(true)
                    .build();
        };

        TablePartitionId grpPart = createTableAssignment();

        Lease lease = checkLeaseCreated(grpPart, true);

        lease = waitForProlong(grpPart, lease);

        assertEquals(acceptedNodeRef.get(), lease.getLeaseholder());
        assertTrue(lease.isAccepted());

        var service = clusterServices.get(acceptedNodeRef.get());

        leaseGrantHandler = (msg, from, to) -> {
            if (acceptedNodeRef.get().equals(to)) {
                var redirectNode = nodeNames.stream().filter(nodeName -> !nodeName.equals(to)).findAny().get();

                return PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessageResponse()
                        .redirectProposal(redirectNode)
                        .accepted(false)
                        .build();
            } else {
                return PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessageResponse()
                        .accepted(true)
                        .build();
            }
        };

        service.messagingService().send(
                clusterServices.get(activeActorRef.get()).topologyService().localMember(),
                PLACEMENT_DRIVER_MESSAGES_FACTORY.stopLeaseProlongationMessage().groupId(grpPart).build()
        );

        Lease leaseRenew = waitNewLeaseholder(grpPart, lease);

        log.info("Lease move from {} to {}", lease.getLeaseholder(), leaseRenew.getLeaseholder());
    }

    /**
     * Waits for a new leaseholder.
     *
     * @param grpPart Replication group id.
     * @param lease Previous lease.
     * @return Renewed lease.
     * @throws InterruptedException If the waiting is interrupted.
     */
    private Lease waitNewLeaseholder(TablePartitionId grpPart, Lease lease) throws InterruptedException {
        var leaseRenewRef = new AtomicReference<Lease>();

        assertTrue(waitForCondition(() -> {
            var fut = metaStorageManager.get(PLACEMENTDRIVER_LEASES_KEY);

            Lease leaseRenew = leaseFromBytes(fut.join().value(), grpPart);

            if (lease == null) {
                return false;
            }

            if (!lease.getLeaseholder().equals(leaseRenew.getLeaseholder())) {
                leaseRenewRef.set(leaseRenew);

                return true;
            }

            return false;
        }, 10_000));

        assertTrue(lease.getExpirationTime().compareTo(leaseRenewRef.get().getStartTime()) < 0);

        return leaseRenewRef.get();
    }

    /**
     * Waits for a lease prolong.
     *
     * @param grpPart Replication group id.
     * @param lease Lease which waits for prolong.
     * @return Renewed lease.
     * @throws InterruptedException If the waiting is interrupted.
     */
    private Lease waitForProlong(TablePartitionId grpPart, Lease lease) throws InterruptedException {
        var leaseRenewRef = new AtomicReference<Lease>();

        assertTrue(waitForCondition(() -> {
            var fut = metaStorageManager.get(PLACEMENTDRIVER_LEASES_KEY);

            Lease leaseRenew = leaseFromBytes(fut.join().value(), grpPart);

            if (lease == null) {
                return false;
            }

            if (lease.getExpirationTime().compareTo(leaseRenew.getExpirationTime()) < 0) {
                leaseRenewRef.set(leaseRenew);

                return true;
            }

            return false;
        }, 10_000));

        assertEquals(lease.getLeaseholder(), leaseRenewRef.get().getLeaseholder());

        return leaseRenewRef.get();
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
        return createTableAssignment(metaStorageManager, nextTableId.get(), nodeNames);
    }
}
