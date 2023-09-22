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
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_LEASES_KEY;
import static org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.leases.LeaseBatch;
import org.apache.ignite.internal.placementdriver.leases.LeaseTracker;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.ByteArray;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests to verify {@link PlacementDriver} implemented by {@link LeaseTracker}. */
public class PlacementDriverTest extends BaseIgniteAbstractTest {
    private static final HybridTimestamp AWAIT_TIME_10_000 = new HybridTimestamp(10_000, 0);

    private static final ByteArray FAKE_KEY = new ByteArray("foobar");

    private static final TablePartitionId GROUP_1 = new TablePartitionId(1000, 0);

    private static final String LEASEHOLDER_1 = "leaseholder1";
    private static final Lease LEASE_FROM_1_TO_5_000 = new Lease(
            LEASEHOLDER_1,
            new HybridTimestamp(1, 0),
            new HybridTimestamp(5_000, 0),
            false,
            true,
            GROUP_1
    );

    private static final Lease LEASE_FROM_1_TO_15_000 = new Lease(
            LEASEHOLDER_1,
            new HybridTimestamp(1, 0),
            new HybridTimestamp(15_000, 0),
            false,
            true,
            GROUP_1
    );

    private static final Lease LEASE_FROM_15_000_TO_30_000 = new Lease(
            LEASEHOLDER_1,
            new HybridTimestamp(15_000, 0),
            new HybridTimestamp(30_000, 0),
            false,
            true,
            GROUP_1
    );

    private static final int AWAIT_PERIOD_FOR_LOCAL_NODE_TO_BE_NOTIFIED_ABOUT_LEASE_UPDATES = 1_000;

    private VaultManager vault;

    private MetaStorageManager metastore;

    private PendingComparableValuesTracker<Long, Void> revisionTracker;

    private LeaseTracker placementDriver;

    @BeforeEach
    void setUp() {
        vault = new VaultManager(new InMemoryVaultService());

        metastore = StandaloneMetaStorageManager.create(vault);

        revisionTracker = new PendingComparableValuesTracker<>(-1L);

        placementDriver = createPlacementDriver();

        metastore.registerRevisionUpdateListener(rev -> {
            revisionTracker.update(rev, null);

            return completedFuture(null);
        });

        vault.start();
        metastore.start();

        CompletableFuture<Long> recoveryFinishedFuture = metastore.recoveryFinishedFuture();

        assertThat(recoveryFinishedFuture, willCompleteSuccessfully());

        placementDriver.startTrackAsync(recoveryFinishedFuture.join());

        assertThat("Watches were not deployed", metastore.deployWatches(), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(
                placementDriver == null ? null : placementDriver::stopTrack,
                metastore == null ? null : metastore::stop,
                vault == null ? null : vault::stop
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
        CompletableFuture<ReplicaMeta> primaryReplicaFuture = placementDriver.awaitPrimaryReplica(GROUP_1, AWAIT_TIME_10_000);
        assertFalse(primaryReplicaFuture.isDone());

        // Publish primary replica for an interval [1, 5].
        publishLease(LEASE_FROM_1_TO_5_000);

        // Await local node to be notified about new primary replica.
        assertTrue(waitForCondition(() -> placementDriver.getLease(GROUP_1).equals(LEASE_FROM_1_TO_5_000), 1_000));

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
        CompletableFuture<ReplicaMeta> primaryReplicaFuture = placementDriver.awaitPrimaryReplica(GROUP_1, AWAIT_TIME_10_000);
        assertFalse(primaryReplicaFuture.isDone());

        // Publish primary replica for an interval [1, 5].
        publishLease(LEASE_FROM_1_TO_5_000);

        // Await local node to be notified about new primary replica.
        assertTrue(waitForCondition(() -> placementDriver.getLease(GROUP_1).equals(LEASE_FROM_1_TO_5_000), 1_000));

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
        assertTrue(waitForCondition(() -> placementDriver.getLease(GROUP_1).equals(LEASE_FROM_1_TO_15_000),
                AWAIT_PERIOD_FOR_LOCAL_NODE_TO_BE_NOTIFIED_ABOUT_LEASE_UPDATES));

        // Await primary replica for time 10.
        CompletableFuture<ReplicaMeta> primaryReplicaFuture = placementDriver.awaitPrimaryReplica(GROUP_1, AWAIT_TIME_10_000);

        // Assert that primary waiter is completed.
        assertTrue(primaryReplicaFuture.isDone());

        assertEquals(LEASEHOLDER_1, primaryReplicaFuture.get().getLeaseholder());
        assertEquals(LEASE_FROM_1_TO_15_000.getStartTime(), primaryReplicaFuture.get().getStartTime());
        assertEquals(LEASE_FROM_1_TO_15_000.getExpirationTime(), primaryReplicaFuture.get().getExpirationTime());
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
        CompletableFuture<ReplicaMeta> primaryReplicaFuture1 = placementDriver.awaitPrimaryReplica(GROUP_1, AWAIT_TIME_10_000);
        CompletableFuture<ReplicaMeta> primaryReplicaFuture2 = placementDriver.awaitPrimaryReplica(GROUP_1, AWAIT_TIME_10_000);

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
        CompletableFuture<ReplicaMeta> primaryReplicaFuture1 = placementDriver.awaitPrimaryReplica(GROUP_1, AWAIT_TIME_10_000);
        CompletableFuture<ReplicaMeta> primaryReplicaFuture2 = placementDriver.awaitPrimaryReplica(GROUP_1, AWAIT_TIME_10_000);

        assertFalse(primaryReplicaFuture1.isDone());
        assertFalse(primaryReplicaFuture2.isDone());

        // Timeout first waiter, and assert that timeout occurred.
        primaryReplicaFuture1.orTimeout(1, TimeUnit.MILLISECONDS);

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
        CompletableFuture<ReplicaMeta> primaryReplicaFuture = placementDriver.awaitPrimaryReplica(GROUP_1, AWAIT_TIME_10_000);
        assertFalse(primaryReplicaFuture.isDone());

        // Publish primary replica for an interval [1, 15].
        publishLease(LEASE_FROM_1_TO_15_000);

        // Assert that primary await future will succeed fast.
        assertThat(primaryReplicaFuture, willSucceedFast());

        // Assert that retrieved primary replica for same awaiting timestamp as within await ones will be completed immediately.
        CompletableFuture<ReplicaMeta> retrievedPrimaryReplicaSameTime = placementDriver.getPrimaryReplica(GROUP_1, AWAIT_TIME_10_000);
        assertTrue(retrievedPrimaryReplicaSameTime.isDone());

        // Assert that retrieved primary replica for awaiting timestamp lt lease expiration time will be completed immediately.
        CompletableFuture<ReplicaMeta> retrievedPrimaryReplicaTimeLtLeaseExpiration =
                placementDriver.getPrimaryReplica(GROUP_1, new HybridTimestamp(14_000, 0));
        assertTrue(retrievedPrimaryReplicaTimeLtLeaseExpiration.isDone());

        // Assert that retrieved primary replica for awaiting timestamp gt lease expiration time will be completed soon with null.
        CompletableFuture<ReplicaMeta> retrievedPrimaryReplicaTimeGtLeaseExpiration =
                placementDriver.getPrimaryReplica(GROUP_1, new HybridTimestamp(16_000, 0));

        assertThat(retrievedPrimaryReplicaTimeGtLeaseExpiration, willSucceedFast());
        assertNull(retrievedPrimaryReplicaTimeGtLeaseExpiration.get());

        assertEquals(LEASEHOLDER_1, retrievedPrimaryReplicaSameTime.get().getLeaseholder());
        assertEquals(LEASE_FROM_1_TO_15_000.getStartTime(), retrievedPrimaryReplicaSameTime.get().getStartTime());
        assertEquals(LEASE_FROM_1_TO_15_000.getExpirationTime(), retrievedPrimaryReplicaSameTime.get().getExpirationTime());

        assertEquals(LEASEHOLDER_1, retrievedPrimaryReplicaTimeLtLeaseExpiration.get().getLeaseholder());
        assertEquals(LEASE_FROM_1_TO_15_000.getStartTime(), retrievedPrimaryReplicaTimeLtLeaseExpiration.get().getStartTime());
        assertEquals(LEASE_FROM_1_TO_15_000.getExpirationTime(), retrievedPrimaryReplicaTimeLtLeaseExpiration.get().getExpirationTime());
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
    void testListenReplicaBecomePrimaryEventOnStartPlacementDriver() {
        long newRecoveryRevision = publishLease(LEASE_FROM_1_TO_5_000);

        placementDriver.stopTrack();

        placementDriver = createPlacementDriver();

        CompletableFuture<PrimaryReplicaEventParameters> eventParametersFuture = listenAnyReplicaBecomePrimaryEvent();

        placementDriver.startTrackAsync(newRecoveryRevision);

        assertThat(eventParametersFuture, willCompleteSuccessfully());

        PrimaryReplicaEventParameters parameters = eventParametersFuture.join();

        assertThat(parameters.causalityToken(), equalTo(newRecoveryRevision));

        checkReplicaBecomePrimaryEventParameters(LEASE_FROM_1_TO_5_000, parameters);
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
                new HybridTimestamp(1, 0),
                new HybridTimestamp(15_000, 0),
                false,
                true,
                new TablePartitionId(groupId.tableId() + 1, groupId.partitionId() + 1)
        );

        publishLeases(lease, neighborGroupLease);

        assertThat(eventParametersFuture, willTimeoutFast());
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

    private CompletableFuture<PrimaryReplicaEventParameters> listenAnyReplicaBecomePrimaryEvent() {
        return listenReplicaBecomePrimaryEvent(null);
    }

    private CompletableFuture<PrimaryReplicaEventParameters> listenSpecificGroupReplicaBecomePrimaryEvent(ReplicationGroupId groupId) {
        return listenReplicaBecomePrimaryEvent(Objects.requireNonNull(groupId));
    }

    private CompletableFuture<PrimaryReplicaEventParameters> listenReplicaBecomePrimaryEvent(@Nullable ReplicationGroupId groupId) {
        var eventParametersFuture = new CompletableFuture<PrimaryReplicaEventParameters>();

        placementDriver.listen(PRIMARY_REPLICA_ELECTED, (parameters, exception) -> {
            if (groupId == null || groupId.equals(parameters.groupId())) {
                if (exception != null) {
                    eventParametersFuture.completeExceptionally(exception);
                } else {
                    eventParametersFuture.complete(parameters);
                }
            }

            return completedFuture(false);
        });

        return eventParametersFuture;
    }

    private static void checkReplicaBecomePrimaryEventParameters(
            Lease expLease,
            PrimaryReplicaEventParameters parameters
    ) {
        assertThat(parameters.groupId(), equalTo(expLease.replicationGroupId()));
        assertThat(parameters.leaseholder(), equalTo(expLease.getLeaseholder()));
    }

    private LeaseTracker createPlacementDriver() {
        return new LeaseTracker(metastore);
    }
}
