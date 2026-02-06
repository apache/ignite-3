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

package org.apache.ignite.internal.partition.replicator;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.bypassingThreadAssertions;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.ZoneResourcesManager.ZonePartitionResources;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.SafeTimeValuesTracker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
class ZoneResourcesManagerTest extends IgniteAbstractTest {
    private TxStateRocksDbSharedStorage sharedStorage;

    private ZoneResourcesManager manager;

    @BeforeEach
    void init(
            @Mock LogSyncer logSyncer,
            @Mock TxManager txManager,
            @Mock OutgoingSnapshotsManager outgoingSnapshotsManager,
            @Mock TopologyService topologyService,
            @Mock CatalogService catalogService,
            @Mock ReplicaManager replicaManager,
            @InjectExecutorService ScheduledExecutorService scheduler,
            @InjectExecutorService ExecutorService executor
    ) {
        sharedStorage = new TxStateRocksDbSharedStorage(
                "test",
                workDir,
                scheduler,
                executor,
                logSyncer,
                mock(FailureProcessor.class),
                () -> 0
        );

        manager = new ZoneResourcesManager(
                sharedStorage,
                txManager,
                outgoingSnapshotsManager,
                topologyService,
                catalogService,
                mock(FailureProcessor.class),
                executor,
                replicaManager
        );
        assertThat(sharedStorage.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    private static PendingComparableValuesTracker<Long, Void> newStorageIndexTracker() {
        return new PendingComparableValuesTracker<>(0L);
    }

    @AfterEach
    void cleanup() {
        manager.close();

        assertThat(sharedStorage.stopAsync(), willCompleteSuccessfully());
    }

    @Test
    void allocatesResources() {
        ZonePartitionResources resources = allocatePartitionResources(new ZonePartitionId(1, 1), 10);

        assertThat(resources.txStatePartitionStorage(), is(notNullValue()));
        assertThat(resources.raftListener(), is(notNullValue()));
        assertThat(resources.snapshotStorage(), is(notNullValue()));
        assertThat(resources.replicaListenerFuture().isDone(), is(false));
    }

    private static SafeTimeValuesTracker newSafeTimeTracker() {
        return new SafeTimeValuesTracker(HybridTimestamp.MIN_VALUE);
    }

    @Test
    void closesResourcesOnShutdown() {
        SafeTimeValuesTracker zone1SafeTimeTracker1 = spy(newSafeTimeTracker());
        SafeTimeValuesTracker zone1SafeTimeTracker5 = spy(newSafeTimeTracker());
        SafeTimeValuesTracker zone2SafeTimeTracker3 = spy(newSafeTimeTracker());

        PendingComparableValuesTracker<Long, Void> zone1IndexTracker1 = spy(newStorageIndexTracker());
        PendingComparableValuesTracker<Long, Void> zone1IndexTracker5 = spy(newStorageIndexTracker());
        PendingComparableValuesTracker<Long, Void> zone2IndexTracker3 = spy(newStorageIndexTracker());

        ZonePartitionResources zone1storage1 = allocatePartitionResources(
                new ZonePartitionId(1, 1),
                10,
                zone1SafeTimeTracker1,
                zone1IndexTracker1
        );
        ZonePartitionResources zone1storage5 = allocatePartitionResources(
                new ZonePartitionId(1, 5),
                10,
                zone1SafeTimeTracker5,
                zone1IndexTracker5
        );
        ZonePartitionResources zone2storage3 = allocatePartitionResources(
                new ZonePartitionId(2, 3),
                10,
                zone2SafeTimeTracker3,
                zone2IndexTracker3
        );

        manager.close();

        assertThatStorageIsStopped(zone1storage1);
        assertThatStorageIsStopped(zone1storage5);
        assertThatStorageIsStopped(zone2storage3);

        verify(zone1SafeTimeTracker1).close();
        verify(zone1SafeTimeTracker5).close();
        verify(zone2SafeTimeTracker3).close();

        verify(zone1IndexTracker1).close();
        verify(zone1IndexTracker5).close();
        verify(zone2IndexTracker3).close();
    }

    @Test
    void removesTxStatePartitionStorageOnDestroy() {
        int zoneId = 1;

        allocatePartitionResources(new ZonePartitionId(zoneId, 1), 10);
        allocatePartitionResources(new ZonePartitionId(zoneId, 2), 10);

        assertThat(manager.txStatePartitionStorage(zoneId, 1), is(notNullValue()));
        assertThat(manager.txStatePartitionStorage(zoneId, 2), is(notNullValue()));

        bypassingThreadAssertions(() -> manager.destroyZonePartitionResources(new ZonePartitionId(zoneId, 1)));

        assertThat(manager.txStatePartitionStorage(zoneId, 1), is(nullValue()));
        assertThat(manager.txStatePartitionStorage(zoneId, 2), is(notNullValue()));
    }

    @Test
    void supportsParallelAllocation(@InjectExecutorService ExecutorService executor) {
        int partCount = 1000;
        int zoneId = 1;

        CompletableFuture<?>[] futures = IntStream.range(0, partCount)
                .mapToObj(partId -> runAsync(
                        () -> allocatePartitionResources(new ZonePartitionId(zoneId, partId), partCount),
                        executor
                ))
                .toArray(CompletableFuture[]::new);

        assertThat(allOf(futures), willCompleteSuccessfully());
    }

    @SuppressWarnings("ThrowableNotThrown")
    private static void assertThatStorageIsStopped(ZonePartitionResources resources) {
        assertThrows(
                IgniteInternalException.class,
                () -> bypassingThreadAssertions(() -> resources.txStatePartitionStorage().get(UUID.randomUUID())),
                "Transaction state storage is stopped"
        );
    }

    private ZonePartitionResources allocatePartitionResources(
            ZonePartitionId zonePartitionId,
            int partitionCount
    ) {
        return allocatePartitionResources(zonePartitionId, partitionCount, newSafeTimeTracker(), newStorageIndexTracker());
    }

    private ZonePartitionResources allocatePartitionResources(
            ZonePartitionId zonePartitionId,
            int partitionCount,
            SafeTimeValuesTracker safeTimeTracker,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker
    ) {
        return bypassingThreadAssertions(() -> manager.allocateZonePartitionResources(
                zonePartitionId,
                partitionCount,
                safeTimeTracker,
                storageIndexTracker
        ));
    }
}
