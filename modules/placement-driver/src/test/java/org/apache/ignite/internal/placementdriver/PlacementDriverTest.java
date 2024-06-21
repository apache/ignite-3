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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.affinity.Assignment.forPeer;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_LEASES_KEY;
import static org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.affinity.TokenizedAssignments;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.leases.LeaseBatch;
import org.apache.ignite.internal.placementdriver.leases.LeaseTracker;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/** Tests to verify {@link LeaseTracker} implemented by {@link PlacementDriver}. */
public class PlacementDriverTest extends BaseIgniteAbstractTest {
    private static final HybridTimestamp AWAIT_TIME_1_000 = new HybridTimestamp(1_000, 0);

    private static final HybridTimestamp AWAIT_TIME_10_000 = new HybridTimestamp(10_000, 0);

    private static final ByteArray FAKE_KEY = new ByteArray("foobar");

    private static final TablePartitionId GROUP_1 = new TablePartitionId(1000, 0);

    private static final String LEASEHOLDER_1 = "leaseholder1";

    private static final String LEASEHOLDER_ID_1 = "leaseholder1_id";

    private static final ClusterNode FAKE_NODE = new ClusterNodeImpl(LEASEHOLDER_ID_1, LEASEHOLDER_1, mock(NetworkAddress.class));

    private static final Lease LEASE_FROM_1_TO_5_000 = new Lease(
            LEASEHOLDER_1,
            LEASEHOLDER_ID_1,
            new HybridTimestamp(1, 0),
            new HybridTimestamp(5_000, 0),
            false,
            true,
            null,
            GROUP_1
    );

    private static final Lease LEASE_FROM_1_TO_15_000 = new Lease(
            LEASEHOLDER_1,
            LEASEHOLDER_ID_1,
            new HybridTimestamp(1, 0),
            new HybridTimestamp(15_000, 0),
            false,
            true,
            null,
            GROUP_1
    );

    private static final Lease LEASE_FROM_15_000_TO_30_000 = new Lease(
            LEASEHOLDER_1,
            LEASEHOLDER_ID_1,
            new HybridTimestamp(15_000, 0),
            new HybridTimestamp(30_000, 0),
            false,
            true,
            null,
            GROUP_1
    );

    private static final String NODE_A_CONSITIENT_ID = "A";

    private static final String NODE_B_CONSITIENT_ID = "B";

    private static final String NODE_C_CONSITIENT_ID = "C";

    private static final Set<Assignment> ASSIGNMENTS_A = Set.of(forPeer(NODE_A_CONSITIENT_ID));

    private static final Set<Assignment> ASSIGNMENTS_AB = Set.of(forPeer(NODE_A_CONSITIENT_ID), forPeer(NODE_B_CONSITIENT_ID));

    private static final Set<Assignment> ASSIGNMENTS_ABC =
            Set.of(forPeer(NODE_A_CONSITIENT_ID), forPeer(NODE_B_CONSITIENT_ID), forPeer(NODE_C_CONSITIENT_ID));

    private static final int AWAIT_PERIOD_FOR_LOCAL_NODE_TO_BE_NOTIFIED_ABOUT_LEASE_UPDATES = 1_000;

    private static final int AWAIT_PRIMARY_REPLICA_TIMEOUT = 10;

    private MetaStorageManager metastore;

    private PendingComparableValuesTracker<Long, Void> revisionTracker;

    private final ClockService clockService = new TestClockService(new HybridClockImpl());

    private LeaseTracker leasePlacementDriver;

    private AssignmentsTracker assignmentsPlacementDriver;

    @Nullable
    private ClusterNode leaseholder;

    @BeforeEach
    void setUp() {
        metastore = StandaloneMetaStorageManager.create();

        revisionTracker = new PendingComparableValuesTracker<>(-1L);

        leasePlacementDriver = createPlacementDriver();

        assignmentsPlacementDriver = createAssignmentsPlacementDriver();

        metastore.registerRevisionUpdateListener(rev -> {
            revisionTracker.update(rev, null);

            return nullCompletedFuture();
        });

        assertThat(metastore.startAsync(new ComponentContext()), willCompleteSuccessfully());

        CompletableFuture<Long> recoveryFinishedFuture = metastore.recoveryFinishedFuture();

        assertThat(recoveryFinishedFuture, willCompleteSuccessfully());

        leasePlacementDriver.startTrack(recoveryFinishedFuture.join());

        assignmentsPlacementDriver.startTrack();

        assertThat("Watches were not deployed", metastore.deployWatches(), willCompleteSuccessfully());

        leaseholder = FAKE_NODE;
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAll(
                assignmentsPlacementDriver == null ? null : assignmentsPlacementDriver::stopTrack,
                leasePlacementDriver == null ? null : leasePlacementDriver::stopTrack,
                metastore == null ? null : () -> assertThat(metastore.stopAsync(new ComponentContext()), willCompleteSuccessfully())
        );
    }

    /**
     * Test steps.
     * <ol>
     *     <li>Await primary replica for time 10.</li>
     *     <li>Publish primary replica for an interval [1, 5].</li>
     *     <li>Assert that primary await future isn't completed yet because corresponding await time 10 is greater than lease expiration
     *     time 5. </li>
     *     <li>Prolongate primary replica lease to an interval [1, 15].</li>
     *     <li>Assert that primary await future will succeed fast.</li>
     * </ol>
     */
    @Test
    public void testAwaitPrimaryReplicaInInterval() throws Exception {
        // Await primary replica for time 10.
        CompletableFuture<ReplicaMeta> primaryReplicaFuture = leasePlacementDriver.awaitPrimaryReplica(GROUP_1, AWAIT_TIME_10_000,
                AWAIT_PRIMARY_REPLICA_TIMEOUT, SECONDS);
        assertFalse(primaryReplicaFuture.isDone());

        // Publish primary replica for an interval [1, 5].
        publishLease(LEASE_FROM_1_TO_5_000);

        // Await local node to be notified about new primary replica.
        assertTrue(waitForCondition(() -> leasePlacementDriver.getLease(GROUP_1).equals(LEASE_FROM_1_TO_5_000), 1_000));

        // Assert that primary await future isn't completed yet because corresponding await time 10 is greater than lease expiration time 5.
        assertFalse(primaryReplicaFuture.isDone());

        // Prolongate primary replica lease to an interval [1, 15]
        publishLease(LEASE_FROM_1_TO_15_000);

        // Assert that primary await future will succeed fast.
        assertThat(primaryReplicaFuture, willSucceedFast());

        assertEquals(LEASEHOLDER_1, primaryReplicaFuture.get().getLeaseholder());
        assertEquals(LEASE_FROM_1_TO_15_000.getStartTime(), primaryReplicaFuture.get().getStartTime());
        assertEquals(LEASE_FROM_1_TO_15_000.getExpirationTime(), primaryReplicaFuture.get().getExpirationTime());
    }

    /**
     * Test steps.
     * <ol>
     *     <li>Await primary replica for time 10.</li>
     *     <li>Publish primary replica for an interval [1, 5].</li>
     *     <li>Assert that primary await future isn't completed yet because corresponding await time 10 is greater than lease expiration
     *     time 5.</li>
     *     <li>Publish primary replica for non-overlapping interval [15, 30] with left border gt than await time 10.</li>
     *     <li>Assert that primary await future will succeed fast.</li>
     * </ol>
     */
    @Test
    public void testAwaitPrimaryReplicaBeforeInterval() throws Exception {
        // Await primary replica for time 10.
        CompletableFuture<ReplicaMeta> primaryReplicaFuture = leasePlacementDriver.awaitPrimaryReplica(GROUP_1, AWAIT_TIME_10_000,
                AWAIT_PRIMARY_REPLICA_TIMEOUT, SECONDS);
        assertFalse(primaryReplicaFuture.isDone());

        // Publish primary replica for an interval [1, 5].
        publishLease(LEASE_FROM_1_TO_5_000);

        // Await local node to be notified about new primary replica.
        assertTrue(waitForCondition(() -> leasePlacementDriver.getLease(GROUP_1).equals(LEASE_FROM_1_TO_5_000), 1_000));

        // Assert that primary await future isn't completed yet because corresponding await time 10 is greater than lease expiration time 5.
        assertFalse(primaryReplicaFuture.isDone());

        // Publish primary replica for non-overlapping interval [15, 30] with left border gt than await time 10.
        publishLease(LEASE_FROM_15_000_TO_30_000);

        // Assert that primary await future will succeed fast.
        assertThat(primaryReplicaFuture, willSucceedFast());

        assertEquals(LEASEHOLDER_1, primaryReplicaFuture.get().getLeaseholder());
        assertEquals(LEASE_FROM_15_000_TO_30_000.getStartTime(), primaryReplicaFuture.get().getStartTime());
        assertEquals(LEASE_FROM_15_000_TO_30_000.getExpirationTime(), primaryReplicaFuture.get().getExpirationTime());
    }

    /**
     * Test steps.
     * <ol>
     *     <li>Publish primary replica for an interval [1, 15].</li>
     *     <li>Await primary replica for time 10.</li>
     *     <li>Assert that primary waiter is completed.</li>
     * </ol>
     */
    @Test
    public void testAwaitPrimaryReplicaBeforeIntervalAfterPublishing() throws Exception {
        // Publish primary replica for an interval [1, 15].
        publishLease(LEASE_FROM_1_TO_15_000);

        // Await local node to be notified about new primary replica.
        assertTrue(waitForCondition(() -> leasePlacementDriver.getLease(GROUP_1).equals(LEASE_FROM_1_TO_15_000),
                AWAIT_PERIOD_FOR_LOCAL_NODE_TO_BE_NOTIFIED_ABOUT_LEASE_UPDATES));

        // Await primary replica for time 10.
        CompletableFuture<ReplicaMeta> primaryReplicaFuture = leasePlacementDriver.awaitPrimaryReplica(GROUP_1, AWAIT_TIME_10_000,
                AWAIT_PRIMARY_REPLICA_TIMEOUT, SECONDS);

        // Assert that primary waiter is completed.
        assertTrue(primaryReplicaFuture.isDone());

        assertEquals(LEASEHOLDER_1, primaryReplicaFuture.get().getLeaseholder());
        assertEquals(LEASE_FROM_1_TO_15_000.getStartTime(), primaryReplicaFuture.get().getStartTime());
        assertEquals(LEASE_FROM_1_TO_15_000.getExpirationTime(), primaryReplicaFuture.get().getExpirationTime());
    }

    /**
     * Common method for several tests. Test steps:
     * <ol>
     *     <li>Publish primary replica for an interval from 1 to {@code leaseDurationMilliseconds} inclusively.</li>
     *     <li>Leaseholder of the primary replica is offline.</li>
     *     <li>Await primary replica for timestamp 1, this shouldn't complete instantly.</li>
     *     <li>Publish a new lease for interval beginning at {@code newLeaseholderPhysicalStartTimeMilliseconds} and another leaseholder
     *         node that is online or offline depending on {@code newLeaseholderIsOnline}.</li>
     *     <li>If {@code newLeaseholderIsOnline} is true, then assert that primary waiter is completed and the new lease belongs
     *         to the online node. Otherwise, assert that the primary waiter is completed exceptionally with
     *         PrimaryReplicaAwaitTimeoutException within the timeout.</li>
     * </ol>
     *
     * @param leaseDurationMilliseconds Lease duration.
     * @param newLeaseholderPhysicalStartTimeMilliseconds Physical start time for a new lease.
     * @param awaitPrimaryReplicaTimeoutMilliseconds Timeout to pass to
     *     {@link PlacementDriver#awaitPrimaryReplica(ReplicationGroupId, HybridTimestamp, long, TimeUnit)}.
     * @param newLeaseholderIsOnline Whether the new leaseholder is online.
     */
    private void testAwaitCurrentPrimaryIsOffline(
            long leaseDurationMilliseconds,
            long newLeaseholderPhysicalStartTimeMilliseconds,
            int awaitPrimaryReplicaTimeoutMilliseconds,
            boolean newLeaseholderIsOnline
    ) {
        // Publish primary replica for an interval [1, 5].
        Lease firstLease = new Lease(
                LEASEHOLDER_1,
                LEASEHOLDER_ID_1,
                new HybridTimestamp(1, 0),
                new HybridTimestamp(leaseDurationMilliseconds, 0),
                false,
                true,
                null,
                GROUP_1
        );

        publishLease(firstLease);

        // Cluster node resolver will return null as if the current leaseholder would be offline.
        leaseholder = null;

        CompletableFuture<ReplicaMeta> primaryReplicaFuture =
                leasePlacementDriver.awaitPrimaryReplica(GROUP_1, AWAIT_TIME_1_000, awaitPrimaryReplicaTimeoutMilliseconds, MILLISECONDS);

        assertFalse(primaryReplicaFuture.isDone());

        String newLeaseholder = "newLeaseholder";
        Lease newLease = new Lease(
                newLeaseholder,
                newLeaseholder,
                new HybridTimestamp(newLeaseholderPhysicalStartTimeMilliseconds, 0),
                new HybridTimestamp(newLeaseholderPhysicalStartTimeMilliseconds + leaseDurationMilliseconds, 0),
                false,
                true,
                null,
                GROUP_1
        );

        if (newLeaseholderIsOnline) {
            leaseholder = new ClusterNodeImpl(newLeaseholder, newLeaseholder, mock(NetworkAddress.class));
        }

        // Publish the lease for the new leaseholder.
        publishLease(newLease);

        if (newLeaseholderIsOnline) {
            assertThat(primaryReplicaFuture, willCompleteSuccessfully());

            ReplicaMeta replicaMeta = primaryReplicaFuture.join();

            assertEquals(newLeaseholder, replicaMeta.getLeaseholderId());
            assertEquals(newLeaseholder, replicaMeta.getLeaseholder());
            assertEquals(newLease.getStartTime(), replicaMeta.getStartTime());
            assertEquals(newLease.getExpirationTime(), replicaMeta.getExpirationTime());
        } else {
            // New leaseholder is offline, and the future fails to wait for an acceptable leaseholder that is online and completes
            // with PrimaryReplicaAwaitTimeoutException.
            assertThat(
                    primaryReplicaFuture,
                    willThrow(PrimaryReplicaAwaitTimeoutException.class, awaitPrimaryReplicaTimeoutMilliseconds + 1000, MILLISECONDS)
            );
        }
    }

    /**
     * Test steps.
     * <ol>
     *     <li>Publish primary replica for an interval [1, 5].</li>
     *     <li>Leaseholder of the primary replica is offline.</li>
     *     <li>Await primary replica for timestamp 1, this shouldn't complete instantly.</li>
     *     <li>Publish a new lease for interval (5, 10] and another leaseholder node that is online.</li>
     *     <li>Assert that primary waiter is completed and the new lease belongs to the online node.</li>
     * </ol>
     */
    @Test
    public void testAwaitCurrentPrimaryIsOffline() {
        testAwaitCurrentPrimaryIsOffline(5000, 5001, AWAIT_PRIMARY_REPLICA_TIMEOUT * 1000, true);
    }

    /**
     * Test steps.
     * <ol>
     *     <li>Publish primary replica for an interval [1, 5].</li>
     *     <li>Leaseholder of the primary replica is offline.</li>
     *     <li>Await primary replica for timestamp 1, this shouldn't complete instantly.</li>
     *     <li>Publish a new lease for interval (8, 10] with delay after the first lease expiration
     *     and another leaseholder node that is online.</li>
     *     <li>Assert that primary waiter is completed and the new lease belongs to the online node.</li>
     * </ol>
     */
    @Test
    public void testAwaitCurrentPrimaryIsOfflineWithPauseBeforeNewPrimaryElection() {
        testAwaitCurrentPrimaryIsOffline(5000, 8000, AWAIT_PRIMARY_REPLICA_TIMEOUT * 1000, true);
    }

    /**
     * Test steps.
     * <ol>
     *     <li>Publish primary replica for an interval [1, 2].</li>
     *     <li>Leaseholder of the primary replica is offline.</li>
     *     <li>Await primary replica for timestamp 1, this shouldn't complete instantly.</li>
     *     <li>Publish a new lease for interval (8, 10] and another leaseholder node also appears to be offline.</li>
     *     <li>Assert that primary waiter is completed exceptionally with PrimaryReplicaAwaitTimeoutException.</li>
     * </ol>
     */
    @Test
    public void testAwaitCurrentPrimaryIsOfflineWithTimeout() {
        testAwaitCurrentPrimaryIsOffline(2000, 3000, 4000, false);
    }

    /**
     * Test steps.
     * <ol>
     *     <li>Await primary replica for time 10 twice.</li>
     *     <li>Publish primary replica for an interval [15, 30] with left border gt than await time 10.</li>
     *     <li>Assert that both primary await futures will succeed fast.</li>
     * </ol>
     */
    @Test
    public void testTwoWaitersSameTime() throws Exception {
        // Await primary replica for time 10 twice.
        CompletableFuture<ReplicaMeta> primaryReplicaFuture1 = leasePlacementDriver.awaitPrimaryReplica(GROUP_1, AWAIT_TIME_10_000,
                AWAIT_PRIMARY_REPLICA_TIMEOUT, SECONDS);
        CompletableFuture<ReplicaMeta> primaryReplicaFuture2 = leasePlacementDriver.awaitPrimaryReplica(GROUP_1, AWAIT_TIME_10_000,
                AWAIT_PRIMARY_REPLICA_TIMEOUT, SECONDS);

        assertFalse(primaryReplicaFuture1.isDone());
        assertFalse(primaryReplicaFuture2.isDone());

        // Publish primary replica for an interval [15, 30] with left border gt than await time 10.
        publishLease(LEASE_FROM_15_000_TO_30_000);

        // Assert that both primary await futures will succeed fast.
        assertThat(primaryReplicaFuture1, willSucceedFast());
        assertThat(primaryReplicaFuture2, willSucceedFast());

        assertEquals(LEASEHOLDER_1, primaryReplicaFuture1.get().getLeaseholder());
        assertEquals(LEASE_FROM_15_000_TO_30_000.getStartTime(), primaryReplicaFuture1.get().getStartTime());
        assertEquals(LEASE_FROM_15_000_TO_30_000.getExpirationTime(), primaryReplicaFuture1.get().getExpirationTime());

        assertEquals(LEASEHOLDER_1, primaryReplicaFuture2.get().getLeaseholder());
        assertEquals(LEASE_FROM_15_000_TO_30_000.getStartTime(), primaryReplicaFuture2.get().getStartTime());
        assertEquals(LEASE_FROM_15_000_TO_30_000.getExpirationTime(), primaryReplicaFuture2.get().getExpirationTime());
    }

    /**
     * Test steps.
     * <ol>
     *     <li>Await primary replica for time 10 twice.</li>
     *     <li>Timeout first waiter, and assert that timeout occurred.</li>
     *     <li>Publish primary replica for an interval [15, 30] with left border gt than await time 10.</li>
     *     <li>Assert that second primary await future will succeed fast.</li>
     * </ol>
     */
    @Test
    public void testTwoWaitersSameTimeFirstTimedOutSecondSucceed() throws Exception {
        // Await primary replica for time 10 twice.
        CompletableFuture<ReplicaMeta> primaryReplicaFuture1 = leasePlacementDriver.awaitPrimaryReplica(GROUP_1, AWAIT_TIME_10_000,
                AWAIT_PRIMARY_REPLICA_TIMEOUT, SECONDS);
        CompletableFuture<ReplicaMeta> primaryReplicaFuture2 = leasePlacementDriver.awaitPrimaryReplica(GROUP_1, AWAIT_TIME_10_000,
                AWAIT_PRIMARY_REPLICA_TIMEOUT, SECONDS);

        assertFalse(primaryReplicaFuture1.isDone());
        assertFalse(primaryReplicaFuture2.isDone());

        // Timeout first waiter, and assert that timeout occurred.
        primaryReplicaFuture1.orTimeout(1, MILLISECONDS);

        //noinspection ThrowableNotThrown
        assertThrowsWithCause(primaryReplicaFuture1::get, TimeoutException.class);
        assertFalse(primaryReplicaFuture2.isDone());

        // Publish primary replica for an interval [15, 30] with left border gt than await time 10.
        publishLease(LEASE_FROM_15_000_TO_30_000);

        // Assert that second primary await future will succeed fast.
        assertThat(primaryReplicaFuture2, willSucceedFast());

        assertEquals(LEASEHOLDER_1, primaryReplicaFuture2.get().getLeaseholder());
        assertEquals(LEASE_FROM_15_000_TO_30_000.getStartTime(), primaryReplicaFuture2.get().getStartTime());
        assertEquals(LEASE_FROM_15_000_TO_30_000.getExpirationTime(), primaryReplicaFuture2.get().getExpirationTime());
    }

    /**
     * Test steps.
     * <ol>
     *     <li>Await primary replica for time 10.</li>
     *     <li>Publish primary replica for an interval [1, 15].</li>
     *     <li>Assert that primary await future will succeed fast.</li>
     *     <li>Assert that retrieved primary replica for same awaiting timestamp as within await ones will be completed immediately.</li>
     *     <li>Assert that retrieved primary replica for awaiting timestamp lt lease expiration time will be completed immediately.</li>
     *     <li>Assert that retrieved primary replica for awaiting timestamp gt lease expiration time will be completed soon with null./li>
     * </ol>
     */
    @Test
    public void testGetPrimaryReplica() throws Exception {
        // Await primary replica for time 10.
        CompletableFuture<ReplicaMeta> primaryReplicaFuture = leasePlacementDriver.awaitPrimaryReplica(GROUP_1, AWAIT_TIME_10_000,
                AWAIT_PRIMARY_REPLICA_TIMEOUT, SECONDS);
        assertFalse(primaryReplicaFuture.isDone());

        // Publish primary replica for an interval [1, 15].
        publishLease(LEASE_FROM_1_TO_15_000);

        // Assert that primary await future will succeed fast.
        assertThat(primaryReplicaFuture, willSucceedFast());

        // Assert that retrieved primary replica for same awaiting timestamp as within await ones will be completed immediately.
        CompletableFuture<ReplicaMeta> retrievedPrimaryReplicaSameTime = leasePlacementDriver.getPrimaryReplica(GROUP_1, AWAIT_TIME_10_000);
        assertTrue(retrievedPrimaryReplicaSameTime.isDone());

        // Assert that retrieved primary replica for awaiting timestamp lt lease expiration time will be completed immediately.
        CompletableFuture<ReplicaMeta> retrievedPrimaryReplicaTimeLtLeaseExpiration =
                leasePlacementDriver.getPrimaryReplica(GROUP_1, new HybridTimestamp(14_000, 0));
        assertTrue(retrievedPrimaryReplicaTimeLtLeaseExpiration.isDone());

        // Assert that retrieved primary replica for awaiting timestamp gt lease expiration time will be completed soon with null.
        CompletableFuture<ReplicaMeta> retrievedPrimaryReplicaTimeGtLeaseExpiration =
                leasePlacementDriver.getPrimaryReplica(GROUP_1, new HybridTimestamp(16_000, 0));

        assertThat(retrievedPrimaryReplicaTimeGtLeaseExpiration, willSucceedFast());
        assertNull(retrievedPrimaryReplicaTimeGtLeaseExpiration.get());

        assertEquals(LEASEHOLDER_1, retrievedPrimaryReplicaSameTime.get().getLeaseholder());
        assertEquals(LEASE_FROM_1_TO_15_000.getStartTime(), retrievedPrimaryReplicaSameTime.get().getStartTime());
        assertEquals(LEASE_FROM_1_TO_15_000.getExpirationTime(), retrievedPrimaryReplicaSameTime.get().getExpirationTime());

        assertEquals(LEASEHOLDER_1, retrievedPrimaryReplicaTimeLtLeaseExpiration.get().getLeaseholder());
        assertEquals(LEASE_FROM_1_TO_15_000.getStartTime(), retrievedPrimaryReplicaTimeLtLeaseExpiration.get().getStartTime());
        assertEquals(LEASE_FROM_1_TO_15_000.getExpirationTime(), retrievedPrimaryReplicaTimeLtLeaseExpiration.get().getExpirationTime());
    }

    /**
     * Test steps.
     * <ol>
     *     <li>Await primary replica for time 10.</li>
     *     <li>Publish primary replica for an interval [1, 15].</li>
     *     <li>Assert that primary await future will succeed fast.</li>
     *     <li>Assert that retrieved primary replica for timestamp less than primaryReplica.expirationTimestamp - CLOCK_SKEW
     *     will return null./li>
     * </ol>
     */
    @Test
    public void testGetPrimaryReplicaWithLessThanClockSkewDiff() {
        // Await primary replica for time 10.
        CompletableFuture<ReplicaMeta> primaryReplicaFuture = leasePlacementDriver.awaitPrimaryReplica(GROUP_1, AWAIT_TIME_10_000,
                AWAIT_PRIMARY_REPLICA_TIMEOUT, SECONDS);
        assertFalse(primaryReplicaFuture.isDone());

        // Publish primary replica for an interval [1, 15].
        publishLease(LEASE_FROM_1_TO_15_000);

        // Assert that primary await future will succeed fast.
        assertThat(primaryReplicaFuture, willSucceedFast());

        // Assert that retrieved primary replica for timestamp less than primaryReplica.expirationTimestamp - CLOCK_SKEW will return null.
        assertThat(
                leasePlacementDriver.getPrimaryReplica(
                        GROUP_1,
                        LEASE_FROM_1_TO_15_000.getExpirationTime()
                                .subtractPhysicalTime(clockService.maxClockSkewMillis())
                                .addPhysicalTime(1L)
                ),
                willBe(nullValue())
        );
    }

    @Test
    void testListenReplicaBecomePrimaryEventCaseNoLeaseBefore() {
        CompletableFuture<PrimaryReplicaEventParameters> eventParametersFuture = listenAnyReplicaBecomePrimaryEvent();

        long publishLeaseRelease = publishLease(LEASE_FROM_1_TO_5_000);

        assertThat(eventParametersFuture, willCompleteSuccessfully());

        PrimaryReplicaEventParameters parameters = eventParametersFuture.join();

        assertThat(parameters.causalityToken(), greaterThanOrEqualTo(publishLeaseRelease));

        checkReplicaBecomePrimaryEventParameters(LEASE_FROM_1_TO_5_000, parameters);
    }

    @Test
    void testListenReplicaBecomePrimaryEventCaseFullTimeIntervalShifted() {
        publishLease(LEASE_FROM_1_TO_5_000);

        CompletableFuture<PrimaryReplicaEventParameters> eventParametersFuture = listenAnyReplicaBecomePrimaryEvent();

        // It is important that startTime shifts because if only expirationTime changes, then this is an prolongation
        // (no need to fire the event).
        long publishLeaseRelease = publishLease(LEASE_FROM_15_000_TO_30_000);

        assertThat(eventParametersFuture, willCompleteSuccessfully());

        PrimaryReplicaEventParameters parameters = eventParametersFuture.join();

        assertThat(parameters.causalityToken(), greaterThanOrEqualTo(publishLeaseRelease));

        checkReplicaBecomePrimaryEventParameters(LEASE_FROM_15_000_TO_30_000, parameters);
    }

    @Test
    void testListenReplicaBecomePrimaryEventCaseOnlyExpirationTimeShifted() {
        publishLease(LEASE_FROM_1_TO_5_000);

        CompletableFuture<PrimaryReplicaEventParameters> eventParametersFuture = listenAnyReplicaBecomePrimaryEvent();

        // Because if only expirationTime changes, then this is an prolongation (no need to fire the event).
        publishLease(LEASE_FROM_1_TO_15_000);

        assertThat(eventParametersFuture, willTimeoutFast());
    }

    @Test
    void testListenNeighborGroupReplicaBecomePrimaryEvent() {
        Lease lease = LEASE_FROM_1_TO_15_000;

        publishLease(lease);

        TablePartitionId groupId = (TablePartitionId) lease.replicationGroupId();

        CompletableFuture<PrimaryReplicaEventParameters> eventParametersFuture = listenSpecificGroupReplicaBecomePrimaryEvent(groupId);

        Lease neighborGroupLease = new Lease(
                LEASEHOLDER_1,
                LEASEHOLDER_ID_1,
                new HybridTimestamp(1, 0),
                new HybridTimestamp(15_000, 0),
                false,
                true,
                null,
                new TablePartitionId(groupId.tableId() + 1, groupId.partitionId() + 1)
        );

        publishLeases(lease, neighborGroupLease);

        assertThat(eventParametersFuture, willTimeoutFast());
    }

    /**
     * Ensure that AssignmentsPlacementDriver#getAssignments will await cluster time and return stable assignments.
     *
     * <ol>
     *     <li>Request assignments for the timestamp < cluster time (MS safe time).</li>
     *     <li>Ensure that assignments future is not completed.</li>
     *     <li>Publish stable, pending and planned assignments in order to verify that pending and planed won't be retrieved by
     *     getAssignments.</li>
     *     <li>Ensure that assignments future was completed with published stable assignments./li>
     * </ol>
     */
    // Races are possible, so let's give it a better change to fail.
    @RepeatedTest(100)
    public void testGetAssignmentsAwaitsClusterTimeAndReturnAssignments() throws Exception {
        // Request assignments for the timestamp < cluster time (MS safe time).
        CompletableFuture<TokenizedAssignments> assignmentsFuture = assignmentsPlacementDriver.getAssignments(GROUP_1, clockService.now());

        // Ensure that assignments future is not completed.
        assertFalse(assignmentsFuture.isDone());

        // Publish stable, pending and planned assignments in order to verify that pending and planed won't be retrieved by getAssignments.
        publishStableAssignments(ASSIGNMENTS_ABC);
        publishPendingAssignments(ASSIGNMENTS_AB);
        publishPlannedAssignments(ASSIGNMENTS_AB);

        // Ensure that assignments future was completed with published stable assignments.
        assertThat(assignmentsFuture, willCompleteSuccessfully());
        assertEquals(ASSIGNMENTS_ABC, assignmentsFuture.get().nodes());
    }

    /**
     * Ensure that AssignmentsPlacementDriver#getAssignments will immediately return stable assignments if clusterTimeToAwait has already
     * passed.
     *
     * <ol>
     *     <li>Publish stable, pending and planned assignments in order to verify that pending and planed won't be retrieved by
     *     getAssignments.</li>
     *     <li>Request assignments for already passed cluster time (MS safe time).</li>
     *     <li>Ensure that assignments future is completed with published stable assignments.</li>
     * </ol>
     */
    // Races are possible, so let's give it a better change to fail.
    @RepeatedTest(100)
    public void testGetAssignmentsImmediatelyReturnAssignmentsIfClusterTimeAlreadyPassed() throws Exception {
        HybridTimestamp requestTimestamp = clockService.now();

        // Publish stable, pending and planned assignments in order to verify that pending and planed won't be retrieved by getAssignments.
        publishStableAssignments(ASSIGNMENTS_ABC);
        publishPendingAssignments(ASSIGNMENTS_AB);
        publishPlannedAssignments(ASSIGNMENTS_AB);

        assertThat(metastore.clusterTime().waitFor(requestTimestamp), willCompleteSuccessfully());

        // Request assignments for already passed cluster time (MS safe time).
        CompletableFuture<TokenizedAssignments> assignmentsFuture = assignmentsPlacementDriver.getAssignments(GROUP_1, requestTimestamp);

        // Ensure that assignments future is completed with published stable assignments.
        assertTrue(assignmentsFuture.isDone());
        assertEquals(ASSIGNMENTS_ABC, assignmentsFuture.get().nodes());
    }

    /**
     * Ensure that AssignmentsPlacementDriver#getAssignments will return the future with null value if there are no stable assignments
     * at the specified clusterAwaitTimestamp.
     *
     * <ol>
     *     <li>Request assignments for the timestamp < cluster time (MS safe time).</li>
     *     <li>Ensure that assignments future is not completed.</li>
     *     <li>Publish **pending** assignments in order to increase cluster time to the value greater then requested within
     *     getAssignments. Pay attention that not stable but pending assignments were published. It's used in order to move cluster time.
     *     </li>
     *     <li>Ensure that assignments future was completed with null value.</li>
     * </ol>
     */
    // Races are possible, so let's give it a better change to fail.
    @RepeatedTest(100)
    public void testGetAssignmentsMayReturnFutureWithNullValue() throws Exception {
        // Request assignments for the timestamp < cluster time (MS safe time).
        CompletableFuture<TokenizedAssignments> assignmentsFuture = assignmentsPlacementDriver.getAssignments(GROUP_1, clockService.now());

        // Ensure that assignments future is not completed.
        assertFalse(assignmentsFuture.isDone());

        // Publish **pending** assignments in order to increase cluster time to the value greater then requested within getAssignments.
        // Pay attention that not stable but pending assignments were published. It's used in order to move cluster time.
        publishPendingAssignments(ASSIGNMENTS_ABC);

        // Ensure that assignments future was completed with null value.
        assertThat(assignmentsFuture, willCompleteSuccessfully());
        assertNull(assignmentsFuture.get());
    }

    /**
     * Ensure that newest assignments are retrieved by AssignmentsPlacementDriver#getAssignments.
     *
     * <ol>
     *     <li>Publish stable assignments.</li>
     *     <li>Request assignments for not requestTimestamp >= ms.safeTime.</li>
     *     <li>Ensure that assignments future is not completed.</li>
     *     <li>Publish new stable assignments that besides publishing the assignments will move ms.safeTime.</li>
     *     <li>Ensure that assignments retrieval future will complete successfully with newest assignments and not initially published.</li>
     *     <li>Retrieve assignments one more time for the same request timestamp.</li>
     *     <li>Ensure that assignments retrieval future will complete successfully with same assignments and token.</li>
     *     <li>Publish yet another new stable assignments.</li>
     *     <li>Ensure that assignments retrieval future will complete successfully with newest assignments and not previously retrieved.
     *     </li>
     *     <li>Ensure that assignments token was updated.</li>
     * </ol>
     */
    // Races are possible, so let's give it a better change to fail.
    @RepeatedTest(100)
    public void testGetAssignmentsReturnsNewestAssignmentsAssociatedWithSafeTimeGreaterThanRequested() throws Exception {
        // Publish stable assignments.
        publishStableAssignments(ASSIGNMENTS_A);

        // requestTimestamp >= clusterTime
        HybridTimestamp requestTimestamp = HybridTimestamp.hybridTimestamp(metastore.clusterTime().nowLong() + 1);

        // Request assignments for not requestTimestamp >= ms.safeTime.
        CompletableFuture<TokenizedAssignments> assignmentsFuture = assignmentsPlacementDriver.getAssignments(GROUP_1, requestTimestamp);

        // Ensure that assignments future is not completed.
        assertFalse(assignmentsFuture.isDone());

        // Publish new stable assignments that besides publishing the assignments will move ms.safeTime.
        publishStableAssignments(ASSIGNMENTS_AB);

        // Ensure that assignments retrieval future will complete successfully with newest assignments and not initially published.
        assertThat(assignmentsFuture, willCompleteSuccessfully());
        assertEquals(ASSIGNMENTS_AB, assignmentsFuture.get().nodes());
        long assignmentsTokenAb = assignmentsFuture.get().token();

        // Retrieve assignments one more time for the same request timestamp.
        CompletableFuture<TokenizedAssignments> assignmentsFuture2 = assignmentsPlacementDriver.getAssignments(GROUP_1, requestTimestamp);

        // Ensure that assignments retrieval future will complete successfully with same assignments and token.
        assertEquals(ASSIGNMENTS_AB, assignmentsFuture.get().nodes());
        assertEquals(assignmentsTokenAb, assignmentsFuture2.get().token());

        // Publish yet another new stable assignments.
        publishStableAssignments(ASSIGNMENTS_ABC);

        // Request assignments for the same requestTimestamp
        CompletableFuture<TokenizedAssignments> assignmentsFuture3 = assignmentsPlacementDriver.getAssignments(GROUP_1, requestTimestamp);

        // Ensure that assignments retrieval future will complete successfully with newest assignments and not previously retrieved.
        assertThat(assignmentsFuture3, willCompleteSuccessfully());
        assertEquals(ASSIGNMENTS_ABC, assignmentsFuture3.get().nodes());
        // Ensure that assignments token was updated.
        assertNotEquals(assignmentsTokenAb, assignmentsFuture3.get().token());
    }

    private long publishLease(Lease lease) {
        return publishLeases(lease);
    }

    private long publishLeases(Lease... leases) {
        long rev = metastore.appliedRevision();

        metastore.invoke(
                Conditions.notExists(FAKE_KEY),
                put(PLACEMENTDRIVER_LEASES_KEY, new LeaseBatch(List.of(leases)).bytes()),
                noop()
        );

        long expRev = rev + 1;

        assertThat(revisionTracker.waitFor(expRev), willCompleteSuccessfully());

        return expRev;
    }

    private void publishAssignments(ByteArray assignmentsKey, Set<Assignment> assignments) {
        long timestampBeforeUpdate = metastore.clusterTime().nowLong();

        metastore.invoke(
                Conditions.notExists(FAKE_KEY),
                put(assignmentsKey, Assignments.toBytes(assignments)),
                noop()
        );

        assertThat(
                metastore.clusterTime().waitFor(HybridTimestamp.hybridTimestamp(timestampBeforeUpdate + 1)),
                willCompleteSuccessfully()
        );
    }

    private void publishStableAssignments(Set<Assignment> assignments) {
        publishAssignments(RebalanceUtil.stablePartAssignmentsKey(GROUP_1), assignments);
    }

    private void publishPendingAssignments(Set<Assignment> assignments) {
        publishAssignments(RebalanceUtil.pendingPartAssignmentsKey(GROUP_1), assignments);
    }

    private void publishPlannedAssignments(Set<Assignment> assignments) {
        publishAssignments(RebalanceUtil.plannedPartAssignmentsKey(GROUP_1), assignments);
    }

    private CompletableFuture<PrimaryReplicaEventParameters> listenAnyReplicaBecomePrimaryEvent() {
        return listenReplicaBecomePrimaryEvent(null);
    }

    private CompletableFuture<PrimaryReplicaEventParameters> listenSpecificGroupReplicaBecomePrimaryEvent(ReplicationGroupId groupId) {
        return listenReplicaBecomePrimaryEvent(Objects.requireNonNull(groupId));
    }

    private CompletableFuture<PrimaryReplicaEventParameters> listenReplicaBecomePrimaryEvent(@Nullable ReplicationGroupId groupId) {
        var eventParametersFuture = new CompletableFuture<PrimaryReplicaEventParameters>();

        leasePlacementDriver.listen(PRIMARY_REPLICA_ELECTED, parameters -> {
            if (groupId == null || groupId.equals(parameters.groupId())) {
                eventParametersFuture.complete(parameters);
            }

            return falseCompletedFuture();
        });

        return eventParametersFuture;
    }

    private static void checkReplicaBecomePrimaryEventParameters(
            Lease expLease,
            PrimaryReplicaEventParameters parameters
    ) {
        assertThat(parameters.groupId(), equalTo(expLease.replicationGroupId()));
        assertThat(parameters.leaseholderId(), equalTo(expLease.getLeaseholderId()));
    }

    private LeaseTracker createPlacementDriver() {
        return new LeaseTracker(metastore, new ClusterNodeResolver() {
            @Override
            public @Nullable ClusterNode getByConsistentId(String consistentId) {
                return leaseholder;
            }

            @Override
            public @Nullable ClusterNode getById(String id) {
                return leaseholder;
            }
        }, clockService);
    }

    private AssignmentsTracker createAssignmentsPlacementDriver() {
        return new AssignmentsTracker(metastore);
    }
}
