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

import static java.util.Collections.synchronizedList;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_LEASES_KEY;
import static org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED;
import static org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.internal.distributionzones.exception.EmptyDataNodesException;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.leases.LeaseBatch;
import org.apache.ignite.internal.placementdriver.leases.LeaseTracker;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for lease tracker.
 */
@ExtendWith(MockitoExtension.class)
public class LeaseTrackerTest extends BaseIgniteAbstractTest {
    private MetaStorageManager msManager;

    private LeaseTracker leaseTracker;

    @Mock
    private ClusterNodeResolver clusterNodeResolver;

    private DataNodesProvider dataNodesProvider;

    @BeforeEach
    void setUp() {
        msManager = StandaloneMetaStorageManager.create();

        HybridClockImpl clock = new HybridClockImpl();

        dataNodesProvider = new DataNodesProvider();

        leaseTracker = new LeaseTracker(
                msManager,
                clusterNodeResolver,
                new TestClockService(clock),
                dataNodesProvider
        );

        assertThat(msManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        assertThat(msManager.deployWatches(), willCompleteSuccessfully());

        leaseTracker.startTrack(0L);
    }

    @AfterEach
    void tearDown() {
        leaseTracker.stopTrack();

        msManager.beforeNodeStop();

        assertThat(msManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @Test
    void testLeaseCleanup() {
        AtomicReference<PrimaryReplicaEventParameters> parametersRef = new AtomicReference<>();

        leaseTracker.listen(PRIMARY_REPLICA_EXPIRED, p -> {
            parametersRef.set(p);
            return falseCompletedFuture();
        });

        var partId0 = new ZonePartitionId(0, 0);
        var partId1 = new ZonePartitionId(0, 1);

        HybridTimestamp startTime = new HybridTimestamp(1, 0);
        HybridTimestamp expirationTime = new HybridTimestamp(1000, 0);

        String leaseholder0 = "notAccepted";
        String leaseholder1 = "accepted";

        Lease lease0 = new Lease(leaseholder0, randomUUID(), startTime, expirationTime, partId0);
        Lease lease1 = new Lease(leaseholder1, randomUUID(), startTime, expirationTime, partId1)
                .acceptLease(new HybridTimestamp(2000, 0));

        // In batch0, there are leases for partition ids partId0 and partId1. In batch1, there is only partId0, so partId1 is expired.
        var batch0 = new LeaseBatch(List.of(lease0, lease1));
        var batch1 = new LeaseBatch(List.of(lease0));

        assertThat(
                msManager.put(PLACEMENTDRIVER_LEASES_KEY, batch0.bytes()),
                willCompleteSuccessfully()
        );

        await()
                .during(100, TimeUnit.MILLISECONDS)
                .until(() -> parametersRef.get() == null);

        // Check that the absence of accepted lease triggers the event.
        assertThat(
                msManager.put(PLACEMENTDRIVER_LEASES_KEY, batch1.bytes()),
                willCompleteSuccessfully()
        );

        await().until(() -> parametersRef.get() != null);

        assertEquals(partId1, parametersRef.get().groupId());

        parametersRef.set(null);

        // Check that the absence of not accepted lease doesn't trigger the event.
        assertThat(
                msManager.put(PLACEMENTDRIVER_LEASES_KEY, new LeaseBatch(List.of()).bytes()),
                willCompleteSuccessfully()
        );

        await()
                .during(100, TimeUnit.MILLISECONDS)
                .until(() -> parametersRef.get() == null);
    }

    /**
     * Tests that when a new replica is elected, an expiration event is always before the next election event.
     */
    @Test
    void replicaExpirationIsFiredBeforeReplicaElection() {
        var events = synchronizedList(new ArrayList<PrimaryReplicaEvent>());

        leaseTracker.listen(PRIMARY_REPLICA_EXPIRED, params -> {
            events.add(PRIMARY_REPLICA_EXPIRED);

            return falseCompletedFuture();
        });

        leaseTracker.listen(PRIMARY_REPLICA_ELECTED, params -> {
            events.add(PRIMARY_REPLICA_ELECTED);

            return falseCompletedFuture();
        });

        var partId = new ZonePartitionId(0, 0);

        HybridTimestamp expirationTime = new HybridTimestamp(1000, 0);

        Lease lease0 = new Lease("test", randomUUID(), new HybridTimestamp(1, 0), expirationTime, partId)
                .acceptLease(new HybridTimestamp(1000, 0));
        Lease lease1 = new Lease("test", randomUUID(), new HybridTimestamp(2, 0), expirationTime, partId)
                .acceptLease(new HybridTimestamp(2000, 0));

        assertThat(
                msManager.put(PLACEMENTDRIVER_LEASES_KEY, new LeaseBatch(List.of(lease0)).bytes()),
                willCompleteSuccessfully()
        );

        assertThat(
                msManager.put(PLACEMENTDRIVER_LEASES_KEY, new LeaseBatch(List.of(lease1)).bytes()),
                willCompleteSuccessfully()
        );

        await().until(() -> events, hasSize(3));

        assertThat(events, contains(PRIMARY_REPLICA_ELECTED, PRIMARY_REPLICA_EXPIRED, PRIMARY_REPLICA_ELECTED));
    }

    @Test
    void awaitPrimaryReplicaPropagatesExceptions() {
        when(clusterNodeResolver.getById(any())).thenThrow(new RuntimeException("test"));

        var groupId = new ZonePartitionId(0, 0);

        CompletableFuture<?> future = leaseTracker.awaitPrimaryReplica(
                groupId,
                HybridTimestamp.MAX_VALUE,
                30,
                TimeUnit.SECONDS
        );

        var lease = new Lease("test", randomUUID(), new HybridTimestamp(1, 0), HybridTimestamp.MAX_VALUE, groupId)
                .acceptLease(HybridTimestamp.MAX_VALUE);

        assertThat(
                msManager.put(PLACEMENTDRIVER_LEASES_KEY, new LeaseBatch(List.of(lease)).bytes()),
                willCompleteSuccessfully()
        );

        assertThat(future, willThrowWithCauseOrSuppressed(RuntimeException.class, "test"));
    }

    @Test
    void awaitPrimaryReplicaPropagatesExceptionsOnStop() {
        CompletableFuture<?> future = leaseTracker.awaitPrimaryReplica(
                new ZonePartitionId(0, 0),
                HybridTimestamp.MAX_VALUE,
                30,
                TimeUnit.SECONDS
        );

        leaseTracker.stopTrack();

        assertThat(future, willThrow(NodeStoppingException.class));
    }

    @Test
    void awaitPrimaryReplicaThrowsOnTimeout() {
        CompletableFuture<?> future = leaseTracker.awaitPrimaryReplica(
                new ZonePartitionId(0, 0),
                HybridTimestamp.MAX_VALUE,
                1,
                TimeUnit.MILLISECONDS
        );

        assertThat(future, willThrow(PrimaryReplicaAwaitTimeoutException.class));
    }

    @Test
    void awaitPrimaryReplicaPropagatesExceptionsWhenNodesIsEmpty() {
        dataNodesProvider.emulateEmptyDataNodes();

        CompletableFuture<?> future = leaseTracker.awaitPrimaryReplica(
                new ZonePartitionId(0, 0),
                HybridTimestamp.MAX_VALUE,
                30,
                TimeUnit.MILLISECONDS
        );

        assertThat(future, willThrow(EmptyDataNodesException.class));
    }

    private static class DataNodesProvider implements Function<Integer, CompletableFuture<Set<String>>> {
        private volatile boolean emptyNodes;

        void emulateEmptyDataNodes() {
            emptyNodes = true;
        }

        @Override
        public CompletableFuture<Set<String>> apply(Integer integer) {
            return emptyNodes ? completedFuture(Set.of()) : completedFuture(Set.of("test-node"));
        }
    }
}
