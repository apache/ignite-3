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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.createTestCatalogManager;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.descriptors.ConsistencyMode.HIGH_AVAILABILITY;
import static org.apache.ignite.internal.catalog.descriptors.ConsistencyMode.STRONG_CONSISTENCY;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link DataNodesManager}.
 */
@ExtendWith(ExecutorServiceExtension.class)
public class DataNodesManagerTest extends BaseIgniteAbstractTest {
    private static final String ZONE_NAME_1 = "test_zone_1";
    private static final String ZONE_NAME_2 = "test_zone_2";

    private static final NodeWithAttributes A = new NodeWithAttributes("A", UUID.randomUUID(), emptyMap(), List.of("default"));
    private static final NodeWithAttributes B = new NodeWithAttributes("B", UUID.randomUUID(), emptyMap(), List.of("default"));

    private static final NodeWithAttributes C = new NodeWithAttributes(
            "C",
            UUID.randomUUID(),
            Map.of("region", "US"),
            List.of("default")
    );

    private static final NodeWithAttributes D = new NodeWithAttributes("D", UUID.randomUUID(), emptyMap(), List.of("default"));

    @InjectExecutorService
    private ScheduledExecutorService scheduledExecutor;

    private final String nodeName = "node";
    private KeyValueStorage storage = new SimpleInMemoryKeyValueStorage(nodeName);
    private HybridClock clock = new HybridClockImpl();
    private MetaStorageManager metaStorageManager = StandaloneMetaStorageManager
            .create(storage, clock, new ReadOperationForCompactionTracker());
    private CatalogManager catalogManager = createTestCatalogManager(nodeName, clock, metaStorageManager);
    private DataNodesManager dataNodesManager =
            new DataNodesManager(nodeName, new IgniteSpinBusyLock(), metaStorageManager, catalogManager);

    private Set<NodeWithAttributes> currentTopology = new HashSet<>(Set.of(A));

    @BeforeEach
    public void setUp() {
        ComponentContext startComponentContext = new ComponentContext();

        ReadOperationForCompactionTracker readOperationForCompactionTracker = new ReadOperationForCompactionTracker();
        storage = new SimpleInMemoryKeyValueStorage(nodeName, readOperationForCompactionTracker);
        clock = new HybridClockImpl();

        metaStorageManager = StandaloneMetaStorageManager.create(storage, readOperationForCompactionTracker);
        assertThat(metaStorageManager.startAsync(startComponentContext), willCompleteSuccessfully());
        assertThat(metaStorageManager.recoveryFinishedFuture(), willCompleteSuccessfully());

        catalogManager = createTestCatalogManager(nodeName, clock, metaStorageManager);

        dataNodesManager = new DataNodesManager(nodeName, new IgniteSpinBusyLock(), metaStorageManager, catalogManager);

        currentTopology = new HashSet<>(Set.of(A, B));

        assertThat(catalogManager.startAsync(startComponentContext), willCompleteSuccessfully());
        dataNodesManager.start(emptyList());

        metaStorageManager.deployWatches();

        createZone(ZONE_NAME_1, STRONG_CONSISTENCY);
        createZone(ZONE_NAME_2, HIGH_AVAILABILITY);

        dataNodesManager.onZoneCreate(0, clock.now(), currentTopology);
    }

    private void createZone(String name, ConsistencyMode consistencyMode) {
        DistributionZonesTestUtil.createZone(catalogManager, name, consistencyMode);

        assertThat(dataNodesManager.onZoneCreate(zoneId(name), clock.now(), currentTopology), willCompleteSuccessfully());
    }

    private void alterZone(
            String zoneName,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter
    ) {
        CatalogZoneDescriptor oldDescriptor = descriptor(zoneName);

        DistributionZonesTestUtil.alterZone(catalogManager, zoneName, dataNodesAutoAdjustScaleUp, dataNodesAutoAdjustScaleDown, filter);

        if (dataNodesAutoAdjustScaleUp != null || dataNodesAutoAdjustScaleDown != null) {
            dataNodesManager.onAutoAdjustAlteration(
                    descriptor(zoneName),
                    clock.now(),
                    oldDescriptor.dataNodesAutoAdjustScaleUp(),
                    oldDescriptor.dataNodesAutoAdjustScaleDown()
            );
        }

        if (filter != null) {
            dataNodesManager.onZoneFilterChange(descriptor(ZONE_NAME_1), clock.now(), currentTopology);
        }
    }

    @Test
    public void addNodesScaleUpImmediate() throws InterruptedException {
        addNodes(Set.of(C));
        waitForDataNodes(ZONE_NAME_1, Set.of(A.nodeName(), B.nodeName(), C.nodeName()));
    }

    @Test
    public void addMultipleNodesScaleUpImmediate() throws InterruptedException {
        addNodes(Set.of(C, D));
        waitForDataNodes(ZONE_NAME_1, Set.of(A.nodeName(), B.nodeName(), C.nodeName(), D.nodeName()));
    }

    @Test
    public void addNodesScaleUpChangedToInfinite() throws InterruptedException {
        alterZone(ZONE_NAME_1, INFINITE_TIMER_VALUE, null, null);

        addNodes(Set.of(C));

        assertFalse(scaleUpScheduled(ZONE_NAME_1));
        checkDataNodes(ZONE_NAME_1, clock.now(), Set.of(A.nodeName(), B.nodeName()));

        // Other zone was not altered and should change its data nodes.
        waitForDataNodes(ZONE_NAME_2, Set.of(A.nodeName(), B.nodeName(), C.nodeName()));
    }

    @Test
    public void addNodesWithScheduledScaleUp() throws InterruptedException {
        alterZone(ZONE_NAME_1, 1, null, null);

        addNodes(Set.of(C));

        // Task is scheduled after C added.
        assertTrue(scaleUpScheduled(ZONE_NAME_1));
        assertFalse(scaleDownScheduled(ZONE_NAME_1));

        addNodes(Set.of(D));

        // Check that scale up tasks will be merged and both nodes will be finally added.
        waitForDataNodes(ZONE_NAME_1, Set.of(A.nodeName(), B.nodeName(), C.nodeName(), D.nodeName()));
    }

    @Test
    public void removeNodesScaleDown() {
        removeNodes(Set.of(B));

        assertFalse(scaleDownScheduled(ZONE_NAME_1));
        checkDataNodes(ZONE_NAME_1, clock.now(), Set.of(A.nodeName(), B.nodeName()));
    }

    @Test
    public void removeMultipleNodesScaleDownImmediate() throws InterruptedException {
        alterZone(ZONE_NAME_1, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        removeNodes(Set.of(A, B));

        waitForDataNodes(ZONE_NAME_1, Set.of());
    }

    @Test
    public void removeNodesScaleDownChangedToImmediate() throws InterruptedException {
        alterZone(ZONE_NAME_1, null, IMMEDIATE_TIMER_VALUE, null);

        removeNodes(Set.of(B));

        waitForDataNodes(ZONE_NAME_1, Set.of(A.nodeName()));

        // Other zone was not altered and should change its data nodes.
        assertFalse(scaleDownScheduled(ZONE_NAME_2));
        checkDataNodes(ZONE_NAME_2, clock.now(), Set.of(A.nodeName(), B.nodeName()));
    }

    @Test
    public void removeNodesWithScheduledScaleDown() throws InterruptedException {
        alterZone(ZONE_NAME_1, null, 1, null);

        removeNodes(Set.of(A));

        // Task is scheduled after A removed.
        assertFalse(scaleUpScheduled(ZONE_NAME_1));
        assertTrue(scaleDownScheduled(ZONE_NAME_1));

        removeNodes(Set.of(B));

        // Check that scale down tasks will be merged and both nodes will be finally removed.
        waitForDataNodes(ZONE_NAME_1, Set.of());
    }

    @Test
    public void filterChange() throws InterruptedException {
        alterZone(ZONE_NAME_1, 10, 10, null);

        addNodes(Set.of(C));

        assertTrue(scaleUpScheduled(ZONE_NAME_1));

        removeNodes(Set.of(A));

        assertTrue(scaleDownScheduled(ZONE_NAME_1));

        String newFilter = "$[?(@.region == \"US\")]";

        alterZone(ZONE_NAME_1, null, null, newFilter);

        checkDataNodes(ZONE_NAME_1, clock.now(), Set.of(C.nodeName()));

        // Timers are discarded earlier than scheduled.
        assertTrue(waitForCondition(() -> !scaleUpScheduled(ZONE_NAME_1) && !scaleDownScheduled(ZONE_NAME_1), 2000));
    }

    @Test
    public void scaleUpChange() throws InterruptedException {
        // Scale up time is big enough.
        alterZone(ZONE_NAME_1, 100, null, null);

        addNodes(Set.of(C));

        assertTrue(scaleUpScheduled(ZONE_NAME_1));

        alterZone(ZONE_NAME_1, IMMEDIATE_TIMER_VALUE, null, null);

        // After the change, scheduled scale up is applied.
        waitForDataNodes(ZONE_NAME_1, Set.of(A.nodeName(), B.nodeName(), C.nodeName()));
    }

    @Test
    public void scaleDownChange() throws InterruptedException {
        // Scale down time is big enough.
        alterZone(ZONE_NAME_1, null, 100, null);

        removeNodes(Set.of(A));

        assertTrue(scaleDownScheduled(ZONE_NAME_1));

        alterZone(ZONE_NAME_1, null, IMMEDIATE_TIMER_VALUE, null);

        // After the change, scheduled scale up is applied.
        waitForDataNodes(ZONE_NAME_1, Set.of(B.nodeName()));
    }

    @Test
    public void dataNodesManagerRecovery() {
        alterZone(ZONE_NAME_1, 100, 100, null);

        addNodes(Set.of(C));
        removeNodes(Set.of(A));

        assertTrue(scaleUpScheduled(ZONE_NAME_1));
        assertTrue(scaleDownScheduled(ZONE_NAME_1));

        dataNodesManager.stop();

        assertFalse(scaleUpScheduled(ZONE_NAME_1));
        assertFalse(scaleDownScheduled(ZONE_NAME_1));

        dataNodesManager.start(catalogManager.activeCatalog(clock.now().longValue()).zones());

        assertTrue(scaleUpScheduled(ZONE_NAME_1));
        assertTrue(scaleDownScheduled(ZONE_NAME_1));
    }

    @Test
    public void partitionResetTriggeredOnNodeRemove() throws InterruptedException {
        CatalogZoneDescriptor zone = descriptor(ZONE_NAME_2);

        AtomicBoolean partitionResetTriggered = new AtomicBoolean();

        dataNodesManager.onTopologyChangeHandler(zone, clock.now(), currentTopology, 1, () -> partitionResetTriggered.set(true));

        // Partition reset is not triggered if no nodes were removed.
        assertFalse(partitionResetTriggered.get());

        removeNodes(Set.of(A));

        dataNodesManager.onTopologyChangeHandler(zone, clock.now(), currentTopology, 1, () -> partitionResetTriggered.set(true));

        assertTrue(waitForCondition(partitionResetTriggered::get, 2000));
    }

    private void addNodes(Set<NodeWithAttributes> nodes) {
        currentTopology.addAll(nodes);

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        assertThat(
                allOf(
                        catalogManager.activeCatalog(clock.now().longValue()).zones()
                                .stream()
                                .map(zone -> dataNodesManager.onTopologyChangeHandler(zone, clock.now(), currentTopology, 0, () -> {}))
                                .collect(toList())
                ),
                willCompleteSuccessfully()
        );
    }

    private void removeNodes(Set<NodeWithAttributes> nodes) {
        currentTopology.removeAll(nodes);

        assertThat(
                allOf(
                        catalogManager.activeCatalog(clock.now().longValue()).zones()
                                .stream()
                                .map(zone -> dataNodesManager.onTopologyChangeHandler(zone, clock.now(), currentTopology, 0, () -> {}))
                                .collect(toList())
                ),
                willCompleteSuccessfully()
        );
    }

    private void waitForDataNodes(String zoneName, Set<String> expectedNodes) throws InterruptedException {
        CatalogZoneDescriptor zone = catalogManager.activeCatalog(clock.now().longValue()).zone(zoneName);
        int zoneId = zone.id();

        boolean success = waitForCondition(() -> {
            CompletableFuture<Set<String>> dataNodesFuture = dataNodesManager.dataNodes(zoneId, clock.now());
            assertThat(dataNodesFuture, willSucceedFast());
            return dataNodesFuture.join().equals(expectedNodes);
        }, 2000);

        if (!success) {
            System.out.println("Expected: " + expectedNodes);
            System.out.println("Actual: " + dataNodesManager.dataNodes(zoneId, clock.now()).join());
        }

        assertTrue(success);
    }

    private void checkDataNodes(String zoneName, HybridTimestamp timestamp, Set<String> expectedNodes) {
        CatalogZoneDescriptor zone = catalogManager.activeCatalog(timestamp.longValue()).zone(zoneName);
        int zoneId = zone.id();

        CompletableFuture<Set<String>> dataNodesFuture = dataNodesManager.dataNodes(zoneId, clock.now());
        assertThat(dataNodesFuture, willSucceedFast());
        assertEquals(expectedNodes, dataNodesFuture.join());
    }

    private CatalogZoneDescriptor descriptor(String zoneName) {
        CatalogZoneDescriptor zoneDescriptor =  catalogManager.activeCatalog(clock.now().longValue()).zone(zoneName);
        assertNotNull(zoneDescriptor);
        return zoneDescriptor;
    }

    private int zoneId(String zoneName) {
        return descriptor(zoneName).id();
    }

    private boolean scaleUpScheduled(String zoneName) {
        return dataNodesManager.zoneTimers(zoneId(zoneName)).scaleUp.taskIsScheduled();
    }

    private boolean scaleDownScheduled(String zoneName) {
        return dataNodesManager.zoneTimers(zoneId(zoneName)).scaleDown.taskIsScheduled();
    }
}
