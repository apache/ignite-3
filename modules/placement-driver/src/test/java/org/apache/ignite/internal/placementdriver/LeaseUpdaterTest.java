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

import static java.util.Collections.emptyMap;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.OperationImpl;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.leases.LeaseBatch;
import org.apache.ignite.internal.placementdriver.leases.LeaseTracker;
import org.apache.ignite.internal.placementdriver.leases.Leases;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * TODO: IGNITE-20485 Configure the lease interval as less as possible to decrease the duration of tests.
 * The class contains unit tests for {@link LeaseUpdater}.
 */
@ExtendWith({MockitoExtension.class, ConfigurationExtension.class})
public class LeaseUpdaterTest extends BaseIgniteAbstractTest {
    private static final PlacementDriverMessagesFactory PLACEMENT_DRIVER_MESSAGES_FACTORY = new PlacementDriverMessagesFactory();
    private static final String LEASE_UPDATE_TOO_LONG = "Lease update invocation took longer than lease interval";
    private static final long TEST_LEASE_INTERVAL_MILLIS = 100L;
    /** Empty leases. */
    private final Leases leases = new Leases(emptyMap(), BYTE_EMPTY_ARRAY);
    /** Cluster nodes. */
    private final LogicalNode stableNode = new LogicalNode(randomUUID(), "test-node-stable", NetworkAddress.from("127.0.0.1:10000"));
    private final LogicalNode pendingNode = new LogicalNode(randomUUID(), "test-node-pending", NetworkAddress.from("127.0.0.1:10001"));

    @InjectConfiguration("mock.leaseExpirationIntervalMillis = " + TEST_LEASE_INTERVAL_MILLIS)
    private ReplicationConfiguration replicationConfiguration;

    @Mock
    private LogicalTopologyService topologyService;

    @Mock
    MetaStorageManager metaStorageManager;

    /** Lease updater for tests. */
    private LeaseUpdater leaseUpdater;

    private AssignmentsTracker assignmentsTracker;

    /** Closure to get a lease that is passed in Meta storage. */
    private volatile Consumer<Lease> renewLeaseConsumer = null;

    private static ZonePartitionId replicationGroupId(int objectId, int partId) {
        return new ZonePartitionId(objectId, partId);
    }

    private static ByteArray stableAssignmentsKey(ZonePartitionId groupId) {
        return ZoneRebalanceUtil.stablePartAssignmentsKey(groupId);
    }

    private static ByteArray pendingAssignmentsQueueKey(ZonePartitionId groupId) {
        return ZoneRebalanceUtil.pendingPartAssignmentsQueueKey(groupId);
    }

    @BeforeEach
    void setUp(
            @Mock ClusterService clusterService,
            @Mock LeaseTracker leaseTracker,
            @Mock MessagingService messagingService
    ) {
        mockStableAssignments(Set.of(Assignment.forPeer(stableNode.name())));
        mockPendingAssignments(Set.of(Assignment.forPeer(pendingNode.name())));

        when(messagingService.invoke(anyString(), any(), anyLong()))
                .then(i -> completedFuture(PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessageResponse().accepted(true).build()));

        when(clusterService.messagingService()).thenReturn(messagingService);

        lenient().when(leaseTracker.leasesLatest()).thenReturn(leases);
        lenient().when(leaseTracker.getLease(any(ReplicationGroupId.class))).then(i -> Lease.emptyLease(i.getArgument(0)));

        when(metaStorageManager.recoveryFinishedFuture()).thenReturn(completedFuture(new Revisions(1, -1)));

        when(topologyService.logicalTopologyOnLeader()).thenReturn(completedFuture(new LogicalTopologySnapshot(1, List.of(stableNode))));

        lenient().when(metaStorageManager.invoke(any(Condition.class), any(Operation.class), any(Operation.class)))
                .thenAnswer(invocation -> {
                    Consumer<Lease> leaseConsumer = renewLeaseConsumer;

                    if (leaseConsumer != null) {
                        OperationImpl op = invocation.getArgument(1);

                        Lease lease = LeaseBatch.fromBytes(toByteArray(op.value())).leases().iterator()
                                .next();

                        leaseConsumer.accept(lease);
                    }

                    return trueCompletedFuture();
                });

        assignmentsTracker = new AssignmentsTracker(
                metaStorageManager,
                mock(FailureProcessor.class),
                zoneId -> completedFuture(Set.of())
        );
        assignmentsTracker.startTrack();

        leaseUpdater = new LeaseUpdater(
                stableNode.name(),
                clusterService,
                metaStorageManager,
                mock(FailureProcessor.class),
                topologyService,
                leaseTracker,
                new TestClockService(new HybridClockImpl()),
                assignmentsTracker,
                replicationConfiguration,
                Runnable::run
        );

    }

    @AfterEach
    void tearDown() {
        leaseUpdater.deInit();
        assignmentsTracker.stopTrack();

        leaseUpdater.deactivate();
        leaseUpdater = null;
    }

    @Test
    public void testLeaseUpdateTooLong() throws Exception {
        initAndActivateLeaseUpdater();

        LogInspector logInspector = LogInspector.create(LeaseUpdater.class, true);

        try {
            AtomicInteger counter = new AtomicInteger(0);

            logInspector.addHandler(
                    evt -> evt.getMessage().getFormattedMessage().startsWith(LEASE_UPDATE_TOO_LONG),
                    counter::incrementAndGet
            );

            Lease lease = awaitForLease(true);

            assertEquals(0, counter.get());

            renewLeaseConsumer = plonogedLease -> {
                try {
                    log.info("Explicitly wait for longer than the lease interval to ensure that the lease updater logs the warning.");

                    Thread.sleep(TEST_LEASE_INTERVAL_MILLIS + 50);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            };

            assertTrue(IgniteTestUtils.waitForCondition(() -> counter.get() >= 1, 10_000));

            awaitForLease(true, lease, 10_000);
        } finally {
            logInspector.stop();
        }
    }

    @Test
    public void testActiveDeactivate() throws Exception {
        initAndActivateLeaseUpdater();

        assertTrue(leaseUpdater.active());

        awaitForLease();

        AtomicReference<Thread> threadRef = new AtomicReference<>();

        assertTrue(IgniteTestUtils.waitForCondition(() -> {
            Thread t = getUpdaterThread();

            if (t != null) {
                threadRef.set(t);

                return true;
            }

            return false;
        }, 10_000));

        assertNotNull(threadRef.get());

        leaseUpdater.deactivate();

        assertFalse(leaseUpdater.active());

        assertTrue(IgniteTestUtils.waitForCondition(() -> getUpdaterThread() == null, 10_000));
    }

    /**
     * The test repeats to attempt to reproduce a race.
     *
     * @throws InterruptedException If failed.
     */
    @RepeatedTest(20)
    public void testActiveDeactivateMultiThread() throws InterruptedException {
        Thread[] threads = new Thread[10];
        CyclicBarrier barrier = new CyclicBarrier(threads.length);
        Random random = new Random();

        leaseUpdater.init();

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                boolean active = random.nextBoolean();

                try {
                    barrier.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    fail(e.getMessage());
                }

                if (active) {
                    leaseUpdater.activate();
                } else {
                    leaseUpdater.deactivate();
                }
            });

            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        leaseUpdater.activate();

        assertTrue(IgniteTestUtils.waitForCondition(() -> getUpdaterThread() != null, 10_000));

        awaitForLease();

        leaseUpdater.deactivate();

        assertTrue(IgniteTestUtils.waitForCondition(() -> getUpdaterThread() == null, 10_000));
    }

    @Test
    public void testLeaseRenew() throws Exception {
        initAndActivateLeaseUpdater();

        Lease lease = awaitForLease(true);

        assertTrue(lease.getStartTime().compareTo(lease.getExpirationTime()) < 0);
        assertEquals(stableNode.name(), lease.getLeaseholder());

        Lease renewedLease = awaitForLease(true);

        assertTrue(lease.getStartTime().compareTo(renewedLease.getStartTime()) < 0);
        assertTrue(lease.getExpirationTime().compareTo(renewedLease.getExpirationTime()) < 0);
        assertEquals(lease.getLeaseholder(), renewedLease.getLeaseholder());
    }

    @Test
    public void testLeaseAmongPendings() throws Exception {
        when(topologyService.logicalTopologyOnLeader()).thenReturn(completedFuture(new LogicalTopologySnapshot(1, List.of(pendingNode))));

        initAndActivateLeaseUpdater();

        Lease lease = awaitForLease();

        assertEquals(pendingNode.name(), lease.getLeaseholder());
    }

    @Test
    public void testLeaseAmongPendingsSkipsLearners() throws Exception {
        var peer = Assignment.forPeer("test-node-pending-peer");
        var learner = Assignment.forLearner("test-node-pending-learner");

        // no stable, pending peer and learner
        mockTopology(Set.of(), Set.of(peer, learner));

        Lease lease = awaitForLease();

        assertEquals(peer.consistentId(), lease.getLeaseholder());

        // no stable, pending learner only
        mockTopology(Set.of(), Set.of(learner));

        assertThrows(AssertionError.class, () -> awaitForLease(1_000));
    }

    private void mockTopology(Set<Assignment> stable, Set<Assignment> pending) {
        mockStableAssignments(stable);
        mockPendingAssignments(pending);

        List<LogicalNode> nodes = Stream.concat(stable.stream(), pending.stream())
                .map(a -> new LogicalNode(randomUUID(), a.consistentId(), NetworkAddress.from("127.0.0.1:10001")))
                .collect(Collectors.toList());

        when(topologyService.logicalTopologyOnLeader())
                .thenReturn(completedFuture(new LogicalTopologySnapshot(1, nodes)));

        initAndActivateLeaseUpdater();
    }

    private void mockPendingAssignments(Set<Assignment> assignments) {
        Entry pendingEntry = new EntryImpl(
                pendingAssignmentsQueueKey(replicationGroupId(1, 0)).bytes(),
                AssignmentsQueue.toBytes(Assignments.of(HybridTimestamp.MIN_VALUE.longValue(), assignments.toArray(Assignment[]::new))),
                1,
                new HybridClockImpl().now()
        );

        byte[] prefixBytes = ZoneRebalanceUtil.PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES;
        when(metaStorageManager.prefixLocally(eq(new ByteArray(prefixBytes)), anyLong()))
                .thenReturn(Cursor.fromIterable(List.of(pendingEntry)));
    }

    private void mockStableAssignments(Set<Assignment> assignments) {
        Entry stableEntry = new EntryImpl(
                stableAssignmentsKey(replicationGroupId(1, 0)).bytes(),
                Assignments.of(HybridTimestamp.MIN_VALUE.longValue(), assignments.toArray(Assignment[]::new)).toBytes(),
                1,
                new HybridClockImpl().now()
        );

        byte[] prefixBytes = ZoneRebalanceUtil.STABLE_ASSIGNMENTS_PREFIX_BYTES;
        when(metaStorageManager.prefixLocally(eq(new ByteArray(prefixBytes)), anyLong()))
                .thenReturn(Cursor.fromIterable(List.of(stableEntry)));
    }

    private void initAndActivateLeaseUpdater() {
        assignmentsTracker.startTrack();

        leaseUpdater.deactivate();

        leaseUpdater.init();

        leaseUpdater.activate();
    }

    /**
     * Waits for lease write to Meta storage.
     *
     * @return A lease.
     * @throws InterruptedException if the wait is interrupted.
     */
    private Lease awaitForLease() throws InterruptedException {
        return awaitForLease(false);
    }

    /**
     * Waits for lease write to Meta storage.
     *
     * @param timeoutMillis Timeout in milliseconds to wait for lease.
     * @return A lease.
     * @throws InterruptedException if the wait is interrupted.
     */
    private Lease awaitForLease(long timeoutMillis) throws InterruptedException {
        return awaitForLease(false, null, timeoutMillis);
    }

    /**
     * Waits for lease write to Meta storage.
     *
     * @param needAccepted Whether to wait only for accepted lease.
     * @return A lease.
     * @throws InterruptedException if the wait is interrupted.
     */
    private Lease awaitForLease(boolean needAccepted) throws InterruptedException {
        return awaitForLease(needAccepted, null, 10_000);
    }

    /**
     * Waits for lease write to Meta storage.
     *
     * @param needAccepted Whether to wait only for accepted lease.
     * @param previousLease Previous lease. If not null, then wait for any lease having expiration time other than the previous has (i.e.
     *      either another lease or prolonged lease).
     * @param timeoutMillis Timeout in milliseconds to wait for lease.
     * @return A lease.
     * @throws InterruptedException if the wait is interrupted.
     */
    private Lease awaitForLease(boolean needAccepted, @Nullable Lease previousLease, long timeoutMillis) throws InterruptedException {
        AtomicReference<Lease> renewedLease = new AtomicReference<>();

        renewLeaseConsumer = lease -> {
            if (needAccepted && !lease.isAccepted()) {
                return;
            }

            if (previousLease != null && previousLease.getExpirationTime().equals(lease.getExpirationTime())) {
                return;
            }

            renewedLease.set(lease);

            renewLeaseConsumer = null;
        };

        assertTrue(IgniteTestUtils.waitForCondition(() -> renewedLease.get() != null, timeoutMillis));

        return renewedLease.get();
    }

    /**
     * Gets a lease updater tread.
     *
     * @return The lease updater thread.
     */
    private static @Nullable Thread getUpdaterThread() {
        Set<Thread> threads = Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().contains("lease-updater")).collect(toSet());

        return threads.isEmpty() ? null : threads.iterator().next();
    }
}
