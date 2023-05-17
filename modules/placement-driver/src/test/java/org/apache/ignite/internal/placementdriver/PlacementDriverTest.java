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

import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_PREFIX;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.leases.LeaseTracker;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.ByteArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests to verify {@link PlacementDriver} implemented by {@link LeaseTracker}. */
public class PlacementDriverTest {
    private static final ByteArray FAKE_KEY = new ByteArray("foobar");
    private static final TablePartitionId GROUP_1 = new TablePartitionId(UUID.randomUUID(), 0);
    private static final ByteArray MS_LEASE_KEY = ByteArray.fromString(PLACEMENTDRIVER_PREFIX + GROUP_1);

    private static final String LEASEHOLDER_1 = "leaseholder1";
    private static final Lease LEASE_FROM_1_TO_5 = new Lease(
            LEASEHOLDER_1,
            new HybridTimestamp(1, 0),
            new HybridTimestamp(5, 0)
    );

    private static final Lease LEASE_FROM_1_TO_15 = new Lease(
            LEASEHOLDER_1,
            new HybridTimestamp(1, 0),
            new HybridTimestamp(15, 0)
    );

    private static final Lease LEASE_FROM_15_TO_30 = new Lease(
            LEASEHOLDER_1,
            new HybridTimestamp(15, 0),
            new HybridTimestamp(30, 0)
    );

    private static final int AWAIT_PERIOD_FOR_LOCAL_NODE_TO_BE_NOTIFIED_ABOUT_LEASE_UPDATES = 1_000;

    private VaultManager vault;

    private MetaStorageManager metastore;

    private LeaseTracker placementDriver;

    @BeforeEach
    void setUp() throws Exception {
        vault = new VaultManager(new InMemoryVaultService());

        metastore = StandaloneMetaStorageManager.create(vault);

        placementDriver = new LeaseTracker(
                vault,
                metastore,
                100,
                new IgniteSpinBusyLock()
        );

        vault.start();
        metastore.start();
        placementDriver.startTrack();

        metastore.deployWatches();
    }

    @AfterEach
    void tearDown() throws Exception {
        placementDriver.stopTrack();
        metastore.stop();
        vault.stop();
    }

    /**
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
        CompletableFuture<LeaseMeta> primaryReplicaFuture = placementDriver.awaitPrimaryReplica(GROUP_1,
                new HybridTimestamp(10, 0));
        assertFalse(primaryReplicaFuture.isDone());

        // Publish primary replica for an interval [1, 5].
        publishLease(LEASE_FROM_1_TO_5);

        // Await local node to be notified about new primary replica.
        assertTrue(waitForCondition(() -> placementDriver.getLease(GROUP_1).equals(LEASE_FROM_1_TO_5), 1_000));

        // Assert that primary await future isn't completed yet because corresponding await time 10 is greater than lease expiration time 5.
        assertFalse(primaryReplicaFuture.isDone());

        // Prolongate primary replica lease to an interval [1, 15]
        publishLease(LEASE_FROM_1_TO_15);

        // Assert that primary await future will succeed fast.
        assertThat(primaryReplicaFuture, CompletableFutureMatcher.willSucceedFast());

        assertEquals(LEASEHOLDER_1, primaryReplicaFuture.get().getLeaseholder());
        assertEquals(LEASE_FROM_1_TO_15.getStartTime(), primaryReplicaFuture.get().getStartTime());
        assertEquals(LEASE_FROM_1_TO_15.getExpirationTime(), primaryReplicaFuture.get().getExpirationTime());
    }

    /**
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
        CompletableFuture<LeaseMeta> primaryReplicaFuture = placementDriver.awaitPrimaryReplica(GROUP_1,
                new HybridTimestamp(10, 0));
        assertFalse(primaryReplicaFuture.isDone());

        // Publish primary replica for an interval [1, 5].
        publishLease(LEASE_FROM_1_TO_5);

        // Await local node to be notified about new primary replica.
        assertTrue(waitForCondition(() -> placementDriver.getLease(GROUP_1).equals(LEASE_FROM_1_TO_5), 1_000));

        // Assert that primary await future isn't completed yet because corresponding await time 10 is greater than lease expiration time 5.
        assertFalse(primaryReplicaFuture.isDone());

        // Publish primary replica for non-overlapping interval [15, 30] with left border gt than await time 10.
        publishLease(LEASE_FROM_15_TO_30);

        // Assert that primary await future will succeed fast.
        assertThat(primaryReplicaFuture, CompletableFutureMatcher.willSucceedFast());

        assertEquals(LEASEHOLDER_1, primaryReplicaFuture.get().getLeaseholder());
        assertEquals(LEASE_FROM_15_TO_30.getStartTime(), primaryReplicaFuture.get().getStartTime());
        assertEquals(LEASE_FROM_15_TO_30.getExpirationTime(), primaryReplicaFuture.get().getExpirationTime());
    }

    /**
     * <ol>
     *     <li>Publish primary replica for an interval [1, 15].</li>
     *     <li>Await primary replica for time 10.</li>
     *     <li>Assert that primary waiter is completed.</li>
     * </ol>
     */
    @Test
    public void testAwaitPrimaryReplicaBeforeIntervalAfterPublishing() throws Exception {
        // Publish primary replica for an interval [1, 15].
        publishLease(LEASE_FROM_1_TO_15);

        // Await local node to be notified about new primary replica.
        assertTrue(waitForCondition(() -> placementDriver.getLease(GROUP_1).equals(LEASE_FROM_1_TO_15),
                AWAIT_PERIOD_FOR_LOCAL_NODE_TO_BE_NOTIFIED_ABOUT_LEASE_UPDATES));

        // Await primary replica for time 10.
        CompletableFuture<LeaseMeta> primaryReplicaFuture = placementDriver.awaitPrimaryReplica(GROUP_1,
                new HybridTimestamp(10, 0));

        // Assert that primary waiter is completed.
        assertTrue(primaryReplicaFuture.isDone());

        assertEquals(LEASEHOLDER_1, primaryReplicaFuture.get().getLeaseholder());
        assertEquals(LEASE_FROM_1_TO_15.getStartTime(), primaryReplicaFuture.get().getStartTime());
        assertEquals(LEASE_FROM_1_TO_15.getExpirationTime(), primaryReplicaFuture.get().getExpirationTime());
    }

    /**
     * <ol>
     *     <li>Await primary replica for time 10 twice.</li>
     *     <li>Publish primary replica for an interval [15, 30] with left border gt than await time 10.</li>
     *     <li>Assert that both primary await futures will succeed fast.</li>
     * </ol>
     */
    @Test
    public void testTwoWaitersSameTime() throws Exception {
        // Await primary replica for time 10 twice.
        CompletableFuture<LeaseMeta> primaryReplicaFuture1 = placementDriver.awaitPrimaryReplica(GROUP_1,
                new HybridTimestamp(10, 0));
        CompletableFuture<LeaseMeta> primaryReplicaFuture2 = placementDriver.awaitPrimaryReplica(GROUP_1,
                new HybridTimestamp(10, 0));

        assertFalse(primaryReplicaFuture1.isDone());
        assertFalse(primaryReplicaFuture2.isDone());

        // Publish primary replica for an interval [15, 30] with left border gt than await time 10.
        publishLease(LEASE_FROM_15_TO_30);

        // Assert that both primary await futures will succeed fast.
        assertThat(primaryReplicaFuture1, CompletableFutureMatcher.willSucceedFast());
        assertThat(primaryReplicaFuture2, CompletableFutureMatcher.willSucceedFast());

        assertEquals(LEASEHOLDER_1, primaryReplicaFuture1.get().getLeaseholder());
        assertEquals(LEASE_FROM_15_TO_30.getStartTime(), primaryReplicaFuture1.get().getStartTime());
        assertEquals(LEASE_FROM_15_TO_30.getExpirationTime(), primaryReplicaFuture1.get().getExpirationTime());

        assertEquals(LEASEHOLDER_1, primaryReplicaFuture2.get().getLeaseholder());
        assertEquals(LEASE_FROM_15_TO_30.getStartTime(), primaryReplicaFuture2.get().getStartTime());
        assertEquals(LEASE_FROM_15_TO_30.getExpirationTime(), primaryReplicaFuture2.get().getExpirationTime());
    }

    /**
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
        CompletableFuture<LeaseMeta> primaryReplicaFuture1 = placementDriver.awaitPrimaryReplica(GROUP_1,
                new HybridTimestamp(10, 0));
        CompletableFuture<LeaseMeta> primaryReplicaFuture2 = placementDriver.awaitPrimaryReplica(GROUP_1,
                new HybridTimestamp(10, 0));

        assertFalse(primaryReplicaFuture1.isDone());
        assertFalse(primaryReplicaFuture2.isDone());

        // Timeout first waiter, and assert that timeout occurred.
        primaryReplicaFuture1.orTimeout(1, TimeUnit.MILLISECONDS);

        //noinspection ThrowableNotThrown
        assertThrowsWithCause(primaryReplicaFuture1::get, TimeoutException.class);
        assertFalse(primaryReplicaFuture2.isDone());

        // Publish primary replica for an interval [15, 30] with left border gt than await time 10.
        publishLease(LEASE_FROM_15_TO_30);

        // Assert that second primary await future will succeed fast.
        assertThat(primaryReplicaFuture2, CompletableFutureMatcher.willSucceedFast());

        assertEquals(LEASEHOLDER_1, primaryReplicaFuture2.get().getLeaseholder());
        assertEquals(LEASE_FROM_15_TO_30.getStartTime(), primaryReplicaFuture2.get().getStartTime());
        assertEquals(LEASE_FROM_15_TO_30.getExpirationTime(), primaryReplicaFuture2.get().getExpirationTime());
    }

    /**
     * <ol>
     *     <li>Await primary replica for time 10.</li>
     *     <li>Publish primary replica for an interval [1, 15].</li>
     *     <li>Assert that primary await future will succeed fast.</li>
     *     <li>Assert that retrieved primary replica for same awaiting timestamp as within await ones will be completed immediately.</li>
     *     <li>Assert that retrieved primary replica for awaiting timestamp lt lease expiration time will be completed immediately.</li>
     *     <li>Assert that retrieved primary replica for awaiting timestamp gt lease expiration will throw timeout exception.</li>
     * </ol>
     */
    @Test
    public void testGetPrimaryReplica() throws Exception {
        HybridTimestamp awaitTime = new HybridTimestamp(10, 0);

        // Await primary replica for time 10.
        CompletableFuture<LeaseMeta> primaryReplicaFuture = placementDriver.awaitPrimaryReplica(GROUP_1,
                awaitTime);
        assertFalse(primaryReplicaFuture.isDone());

        // Publish primary replica for an interval [1, 15].
        publishLease(LEASE_FROM_1_TO_15);

        // Assert that primary await future will succeed fast.
        assertThat(primaryReplicaFuture, CompletableFutureMatcher.willSucceedFast());

        // Assert that retrieved primary replica for same awaiting timestamp as within await ones will be completed immediately.
        CompletableFuture<LeaseMeta> retrievedPrimaryReplicaSameTime =
                placementDriver.getPrimaryReplica(GROUP_1, awaitTime);
        assertTrue(retrievedPrimaryReplicaSameTime.isDone());

        // Assert that retrieved primary replica for awaiting timestamp lt lease expiration time will be completed immediately.
        CompletableFuture<LeaseMeta> retrievedPrimaryReplicaTimeLtLeaseExpiration =
                placementDriver.getPrimaryReplica(GROUP_1, new HybridTimestamp(14, 0));
        assertTrue(retrievedPrimaryReplicaTimeLtLeaseExpiration.isDone());

        // Assert that retrieved primary replica for awaiting timestamp gt lease expiration time isn't done.
        CompletableFuture<LeaseMeta> retrievedPrimaryReplicaTimeGtLeaseExpiration =
                placementDriver.getPrimaryReplica(GROUP_1, new HybridTimestamp(16, 0));
        assertFalse(retrievedPrimaryReplicaTimeGtLeaseExpiration.isDone());

        // Assert that retrieved primary replica for awaiting timestamp gt lease expiration will throw timeout exception.
        //noinspection ThrowableNotThrown
        assertThrowsWithCause(retrievedPrimaryReplicaTimeGtLeaseExpiration::get, TimeoutException.class);

        assertEquals(LEASEHOLDER_1, retrievedPrimaryReplicaSameTime.get().getLeaseholder());
        assertEquals(LEASE_FROM_1_TO_15.getStartTime(), retrievedPrimaryReplicaSameTime.get().getStartTime());
        assertEquals(LEASE_FROM_1_TO_15.getExpirationTime(), retrievedPrimaryReplicaSameTime.get().getExpirationTime());

        assertEquals(LEASEHOLDER_1, retrievedPrimaryReplicaTimeLtLeaseExpiration.get().getLeaseholder());
        assertEquals(LEASE_FROM_1_TO_15.getStartTime(), retrievedPrimaryReplicaTimeLtLeaseExpiration.get().getStartTime());
        assertEquals(LEASE_FROM_1_TO_15.getExpirationTime(), retrievedPrimaryReplicaTimeLtLeaseExpiration.get().getExpirationTime());
    }

    private void publishLease(Lease leaseFrom1To5) {
        metastore.invoke(
                Conditions.notExists(FAKE_KEY),
                put(MS_LEASE_KEY, ByteUtils.toBytes(leaseFrom1To5)),
                noop()
        );
    }
}
