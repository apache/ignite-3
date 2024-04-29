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

package org.apache.ignite.internal.distributionzones;

import static java.util.Collections.reverse;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.createTestCatalogManager;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.REBALANCE_SCHEDULER_POOL_SIZE;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/** Base class for {@link DistributionZoneManager} unit tests. */
@ExtendWith(ConfigurationExtension.class)
public abstract class BaseDistributionZoneManagerTest extends BaseIgniteAbstractTest {
    protected static final String ZONE_NAME = "zone1";

    protected static final long ZONE_MODIFICATION_AWAIT_TIMEOUT = 10_000L;

    protected static final int COMMON_UP_DOWN_AUTOADJUST_TIMER_SECONDS = 10_000;

    protected DistributionZoneManager distributionZoneManager;

    SimpleInMemoryKeyValueStorage keyValueStorage;

    protected LogicalTopology topology;

    protected ClusterStateStorage clusterStateStorage;

    protected MetaStorageManager metaStorageManager;

    private final HybridClock clock = new HybridClockImpl();

    protected CatalogManager catalogManager;

    private final List<IgniteComponent> components = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        String nodeName = "test";

        keyValueStorage = spy(new SimpleInMemoryKeyValueStorage(nodeName));

        metaStorageManager = spy(StandaloneMetaStorageManager.create(keyValueStorage));

        components.add(metaStorageManager);

        clusterStateStorage = new TestClusterStateStorage();

        components.add(clusterStateStorage);

        topology = new LogicalTopologyImpl(clusterStateStorage);

        ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);

        when(cmgManager.logicalTopology()).thenAnswer(invocation -> completedFuture(topology.getLogicalTopology()));

        Consumer<LongFunction<CompletableFuture<?>>> revisionUpdater = (LongFunction<CompletableFuture<?>> function) ->
                metaStorageManager.registerRevisionUpdateListener(function::apply);

        catalogManager = createTestCatalogManager(nodeName, clock, metaStorageManager);
        components.add(catalogManager);

        ScheduledExecutorService rebalanceScheduler = new ScheduledThreadPoolExecutor(REBALANCE_SCHEDULER_POOL_SIZE,
                NamedThreadFactory.create(nodeName, "test-rebalance-scheduler", logger()));

        distributionZoneManager = new DistributionZoneManager(
                nodeName,
                revisionUpdater,
                metaStorageManager,
                new LogicalTopologyServiceImpl(topology, cmgManager),
                catalogManager,
                rebalanceScheduler
        );

        // Not adding 'distributionZoneManager' on purpose, it's started manually.
        assertThat(startAsync(components), willCompleteSuccessfully());
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (distributionZoneManager != null) {
            components.add(distributionZoneManager);
        }

        reverse(components);

        closeAll(Stream.concat(
                components.stream().map(c -> c::beforeNodeStop),
                Stream.of(() -> assertThat(stopAsync(components), willCompleteSuccessfully()))
        ));
    }

    void startDistributionZoneManager() {
        assertThat(
                distributionZoneManager.startAsync()
                        .thenCompose(unused -> metaStorageManager.deployWatches()),
                willCompleteSuccessfully()
        );
    }

    protected void createZone(
            String zoneName,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter
    ) {
        DistributionZonesTestUtil.createZone(
                catalogManager,
                zoneName,
                dataNodesAutoAdjustScaleUp,
                dataNodesAutoAdjustScaleDown,
                filter
        );
    }

    protected void createZone(
            String zoneName,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter,
            String storageProfiles
    ) {
        DistributionZonesTestUtil.createZone(
                catalogManager,
                zoneName,
                dataNodesAutoAdjustScaleUp,
                dataNodesAutoAdjustScaleDown,
                filter,
                storageProfiles
        );
    }

    protected void alterZone(
            String zoneName,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter
    ) {
        DistributionZonesTestUtil.alterZone(
                catalogManager,
                zoneName,
                dataNodesAutoAdjustScaleUp,
                dataNodesAutoAdjustScaleDown,
                filter
        );
    }

    protected void dropZone(String zoneName) {
        DistributionZonesTestUtil.dropZone(catalogManager, zoneName);
    }

    protected int getZoneId(String zoneName) {
        return DistributionZonesTestUtil.getZoneIdStrict(catalogManager, zoneName, clock.nowLong());
    }

    protected CatalogZoneDescriptor getDefaultZone() {
        return DistributionZonesTestUtil.getDefaultZone(catalogManager, clock.nowLong());
    }
}
