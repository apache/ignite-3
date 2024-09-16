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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteOrder;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.OperationImpl;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.leases.LeaseBatch;
import org.apache.ignite.internal.placementdriver.leases.LeaseTracker;
import org.apache.ignite.internal.placementdriver.leases.Leases;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.network.NetworkAddress;
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
@ExtendWith({MockitoExtension.class})
public class LeaseUpdaterTest extends BaseIgniteAbstractTest {
    /** Empty leases. */
    private final Leases leases = new Leases(emptyMap(), BYTE_EMPTY_ARRAY);
    /** Cluster node. */
    private final LogicalNode node = new LogicalNode("123", "test-node", NetworkAddress.from("127.0.0.1:10000"));
    @Mock
    private ClusterService clusterService;
    @Mock
    private MetaStorageManager metaStorageManager;
    @Mock
    private LogicalTopologyService topologyService;
    @Mock
    private LeaseTracker leaseTracker;
    @Mock
    private Cursor<Entry> mcEntriesCursor;
    /** Lease updater for tests. */
    private LeaseUpdater leaseUpdater;
    /** Closure to get a lease that is passed in Meta storage. */
    private volatile Consumer<Lease> renewLeaseConsumer = null;

    @BeforeEach
    void setUp() {
        Entry entry = new EntryImpl(
                stablePartAssignmentsKey(new TablePartitionId(1, 0)).bytes(),
                Assignments.of(HybridTimestamp.MIN_VALUE.longValue(), Assignment.forPeer(node.name())).toBytes(),
                1,
                0
        );

        when(mcEntriesCursor.iterator()).thenReturn(List.of(entry).iterator());
        when(clusterService.messagingService()).thenReturn(mock(MessagingService.class));
        lenient().when(leaseTracker.leasesCurrent()).thenReturn(leases);
        lenient().when(leaseTracker.getLease(any(ReplicationGroupId.class))).then(i -> Lease.emptyLease(i.getArgument(0)));
        when(metaStorageManager.recoveryFinishedFuture()).thenReturn(completedFuture(1L));
        when(metaStorageManager.getLocally(any(ByteArray.class), any(ByteArray.class), anyLong())).thenReturn(mcEntriesCursor);
        when(topologyService.logicalTopologyOnLeader()).thenReturn(completedFuture(new LogicalTopologySnapshot(1, List.of(node))));

        lenient().when(metaStorageManager.invoke(any(Condition.class), any(Operation.class), any(Operation.class)))
                .thenAnswer(invocation -> {
                    Consumer<Lease> leaseConsumer = renewLeaseConsumer;

                    if (leaseConsumer != null) {
                        OperationImpl op = invocation.getArgument(1);

                        Lease lease = LeaseBatch.fromBytes(op.value().order(ByteOrder.LITTLE_ENDIAN)).leases().iterator()
                                .next();

                        leaseConsumer.accept(lease);
                    }

                    return trueCompletedFuture();
                });

        leaseUpdater = new LeaseUpdater(
                node.name(),
                clusterService,
                metaStorageManager,
                topologyService,
                leaseTracker,
                new TestClockService(new HybridClockImpl()),
                new AssignmentsTracker(metaStorageManager)
        );

        leaseUpdater.init();
    }

    @AfterEach
    void tearDown() {
        leaseUpdater.deInit();

        leaseUpdater = null;
    }

    @Test
    public void testActiveDeactivate() throws Exception {
        leaseUpdater.activate();

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
        leaseUpdater.activate();

        Lease lease = awaitForLease();

        assertTrue(lease.getStartTime().compareTo(lease.getExpirationTime()) < 0);
        assertEquals(node.name(), lease.getLeaseholder());

        Lease renewedLease = awaitForLease();

        assertTrue(lease.getStartTime().compareTo(renewedLease.getStartTime()) < 0);
        assertTrue(lease.getExpirationTime().compareTo(renewedLease.getExpirationTime()) < 0);
        assertEquals(lease.getLeaseholder(), renewedLease.getLeaseholder());

        leaseUpdater.deactivate();
    }

    /**
     * Waits for lease write to Meta storage.
     *
     * @return A lease.
     * @throws InterruptedException if the wait is interrupted.
     */
    private Lease awaitForLease() throws InterruptedException {
        AtomicReference<Lease> renewedLease = new AtomicReference<>();

        renewLeaseConsumer = lease -> {
            renewedLease.set(lease);

            renewLeaseConsumer = null;
        };

        assertTrue(IgniteTestUtils.waitForCondition(() -> renewedLease.get() != null, 10_000));

        return renewedLease.get();
    }

    /**
     * Gets a lease updater tread.
     *
     * @return The lease updater thread.
     */
    private Thread getUpdaterThread() {
        Set<Thread> threads = Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().contains("lease-updater")).collect(toSet());

        return threads.isEmpty() ? null : threads.iterator().next();
    }
}
