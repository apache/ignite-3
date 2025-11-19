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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.PARTITION_DISTRIBUTION_RESET_TIMEOUT;
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
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.MetaStorageRevisionListenerRegistry;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/** Base class for {@link DistributionZoneManager} unit tests. */
@ExtendWith(ConfigurationExtension.class)
public abstract class BaseDistributionZoneManagerTest extends BaseIgniteAbstractTest {
    private static final int DELAY_DURATION_MS = 100;

    protected static final String ZONE_NAME = "zone1";

    protected static final long ZONE_MODIFICATION_AWAIT_TIMEOUT = 10_000L;

    protected static final int COMMON_UP_DOWN_AUTOADJUST_TIMER_SECONDS = 10_000;

    protected DistributionZoneManager distributionZoneManager;

    protected SimpleInMemoryKeyValueStorage keyValueStorage;

    protected LogicalTopology topology;

    protected ClusterStateStorage clusterStateStorage;

    protected MetaStorageManager metaStorageManager;

    protected final HybridClock clock = new HybridClockImpl();

    protected CatalogManager catalogManager;

    private final List<IgniteComponent> components = new ArrayList<>();

    @InjectConfiguration("mock.properties." + PARTITION_DISTRIBUTION_RESET_TIMEOUT + " = \"" + IMMEDIATE_TIMER_VALUE + "\"")
    SystemDistributedConfiguration systemDistributedConfiguration;

    @InjectConfiguration("mock.lowWatermark: { dataAvailabilityTimeMillis: 600000}")
    GcConfiguration gcConfiguration;

    @BeforeEach
    void setUp() throws Exception {
        String nodeName = "test";
        UUID nodeId = UUID.randomUUID();

        var readOperationForCompactionTracker = new ReadOperationForCompactionTracker();

        keyValueStorage = spy(new SimpleInMemoryKeyValueStorage(nodeName, readOperationForCompactionTracker));

        metaStorageManager = spy(StandaloneMetaStorageManager.create(keyValueStorage, readOperationForCompactionTracker));
        assertThat(metaStorageManager.startAsync(new ComponentContext()), willCompleteSuccessfully());
        assertThat(metaStorageManager.recoveryFinishedFuture(), willCompleteSuccessfully());

        clusterStateStorage = TestClusterStateStorage.initializedClusterStateStorage();

        components.add(clusterStateStorage);

        topology = new LogicalTopologyImpl(clusterStateStorage, new NoOpFailureManager());

        ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);

        when(cmgManager.logicalTopology()).thenAnswer(invocation -> completedFuture(topology.getLogicalTopology()));

        var revisionUpdater = new MetaStorageRevisionListenerRegistry(metaStorageManager);

        catalogManager = createTestCatalogManager(nodeName, clock, metaStorageManager, () -> DELAY_DURATION_MS, () -> null);
        components.add(catalogManager);

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                IgniteThreadFactory.create(nodeName, "distribution-zone-manager-test-scheduled-executor", log)
        );

        distributionZoneManager = new DistributionZoneManager(
                nodeName,
                () -> nodeId,
                revisionUpdater,
                metaStorageManager,
                new LogicalTopologyServiceImpl(topology, cmgManager),
                catalogManager,
                systemDistributedConfiguration,
                new TestClockService(clock, new ClockWaiter(nodeName, clock, scheduledExecutorService)),
                new NoOpMetricManager(),
                gcConfiguration
        );

        // Not adding 'distributionZoneManager' on purpose, it's started manually.
        assertThat(
                startAsync(new ComponentContext(), components),
                willCompleteSuccessfully()
        );
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (distributionZoneManager != null) {
            components.add(distributionZoneManager);
        }

        reverse(components);

        var toCloseList = new ArrayList<AutoCloseable>();

        components.forEach(component -> toCloseList.add(component::beforeNodeStop));
        toCloseList.add(() -> assertThat(stopAsync(new ComponentContext(), components), willCompleteSuccessfully()));

        toCloseList.add(() -> metaStorageManager.beforeNodeStop());
        toCloseList.add(() -> assertThat(metaStorageManager.stopAsync(new ComponentContext()), willCompleteSuccessfully()));

        toCloseList.add(keyValueStorage::close);

        closeAll(toCloseList);
    }

    protected void startDistributionZoneManager() {
        assertThat(
                distributionZoneManager.startAsync(new ComponentContext())
                        .thenCompose(unused -> metaStorageManager.deployWatches())
                        .thenCompose(unused -> catalogManager.catalogInitializationFuture()),
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
            @Nullable ConsistencyMode consistencyMode,
            String storageProfiles
    ) {
        DistributionZonesTestUtil.createZone(
                catalogManager,
                zoneName,
                dataNodesAutoAdjustScaleUp,
                dataNodesAutoAdjustScaleDown,
                filter,
                consistencyMode,
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

    protected void setDefaultZone(String zoneName) {
        DistributionZonesTestUtil.setDefaultZone(catalogManager, zoneName);
    }

    protected CatalogZoneDescriptor getDefaultZone() {
        assertThat("Catalog initialization", catalogManager.catalogInitializationFuture(), willCompleteSuccessfully());

        return DistributionZonesTestUtil.getDefaultZone(catalogManager, clock.nowLong());
    }
}
