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
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.createTestCatalogManager;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.descriptors.ConsistencyMode.HIGH_AVAILABILITY;
import static org.apache.ignite.internal.catalog.descriptors.ConsistencyMode.STRONG_CONSISTENCY;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesHistoryKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.distributionzones.DataNodesHistory.DataNodesHistorySerializer;
import org.apache.ignite.internal.distributionzones.DataNodesManager.ZoneTimerSchedule;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.PartitionResetClosure;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DataNodesManager}.
 */
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

    private static final NodeWithAttributes C_DIFFERENT_ATTRS = new NodeWithAttributes(
            "C",
            UUID.randomUUID(),
            Map.of("region", "EU"),
            List.of("default")
    );

    private static final NodeWithAttributes D = new NodeWithAttributes("D", UUID.randomUUID(), emptyMap(), List.of("default"));

    private static final String NODE_NAME = "node";

    private KeyValueStorage storage;
    private HybridClock clock;
    private MetaStorageManager metaStorageManager;
    private CatalogManager catalogManager;
    private DataNodesManager dataNodesManager;

    private Set<NodeWithAttributes> currentTopology;

    private final AtomicBoolean partitionResetTriggered = new AtomicBoolean();

    private final PartitionResetClosure partitionResetClosure = (revision, zoneId) -> partitionResetTriggered.set(true);

    @BeforeEach
    public void setUp() {
        ComponentContext startComponentContext = new ComponentContext();

        ReadOperationForCompactionTracker readOperationForCompactionTracker = new ReadOperationForCompactionTracker();
        storage = new SimpleInMemoryKeyValueStorage(NODE_NAME, readOperationForCompactionTracker);
        clock = new HybridClockImpl();

        metaStorageManager = StandaloneMetaStorageManager.create(storage, readOperationForCompactionTracker);
        assertThat(metaStorageManager.startAsync(startComponentContext), willCompleteSuccessfully());
        assertThat(metaStorageManager.recoveryFinishedFuture(), willCompleteSuccessfully());

        catalogManager = createTestCatalogManager(NODE_NAME, clock, metaStorageManager);

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                IgniteThreadFactory.create(NODE_NAME, "data-nodes-manager-test-scheduled-executor", log)
        );

        ClockService clockService = new TestClockService(clock, new ClockWaiter(NODE_NAME, clock, scheduledExecutorService));

        UUID nodeId = UUID.randomUUID();

        dataNodesManager = new DataNodesManager(
                NODE_NAME,
                () -> nodeId,
                new IgniteSpinBusyLock(),
                metaStorageManager,
                catalogManager,
                clockService,
                new NoOpFailureManager(),
                partitionResetClosure,
                () -> 1,
                Collections::emptySet
        );

        currentTopology = new HashSet<>(Set.of(A, B));

        assertThat(catalogManager.startAsync(startComponentContext), willCompleteSuccessfully());
        assertThat(dataNodesManager.startAsync(emptyList(), 1), willCompleteSuccessfully());

        metaStorageManager.deployWatches();

        createZone(ZONE_NAME_1, STRONG_CONSISTENCY);
        createZone(ZONE_NAME_2, HIGH_AVAILABILITY);

        dataNodesManager.onZoneCreate(0, clock.now(), currentTopology);
    }

    @AfterEach
    void cleanup() {
        List.of(catalogManager, metaStorageManager).forEach(IgniteComponent::beforeNodeStop);
        assertThat(IgniteUtils.stopAsync(new ComponentContext(), catalogManager, metaStorageManager), willCompleteSuccessfully());
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
        DistributionZonesTestUtil.alterZone(catalogManager, zoneName, dataNodesAutoAdjustScaleUp, dataNodesAutoAdjustScaleDown, filter);

        if (dataNodesAutoAdjustScaleUp != null || dataNodesAutoAdjustScaleDown != null) {
            dataNodesManager.onAutoAdjustAlteration(
                    descriptor(zoneName),
                    clock.now()
            );
        }

        if (filter != null) {
            dataNodesManager.onZoneFilterChange(descriptor(ZONE_NAME_1), clock.now(), currentTopology);
        }
    }

    @Test
    void addNodeAndChangeScaleUpTimerToImmediate() throws Exception {
        String zoneName = "Default";

        // Setup the scale up timer to 50 seconds.
        alterZone(zoneName, 50, null, null);

        // Add new node to the topology that is A and B nodes. This should setup the scale up timer.
        addNodes(Set.of(C));

        HybridTimestamp t1 = clock.now();

        // Change the scale up timer to immediate.
        // The topology should be changed to A, B and C.
        // A new history entry should be created and added to the history.
        // It is assumed that a timestamp of that entry is greater than `t1`.
        alterZone(zoneName, IMMEDIATE_TIMER_VALUE, null, null);

        waitForDataNodes(zoneName, nodeNames(A, B, C));

        DataNodesHistory history = dataNodesHistory(zoneName);

        assertThat(history.dataNodesForTimestamp(t1).dataNodes().size(), is(2));
    }

    @Test
    public void addNodesScaleUpImmediate() throws InterruptedException {
        addNodes(Set.of(C));
        waitForDataNodes(ZONE_NAME_1, nodeNames(A, B, C));
    }

    @Test
    public void addMultipleNodesScaleUpImmediate() throws InterruptedException {
        addNodes(Set.of(C, D));
        waitForDataNodes(ZONE_NAME_1, nodeNames(A, B, C, D));
    }

    @Test
    public void addNodesScaleUpChangedToInfinite() throws InterruptedException {
        alterZone(ZONE_NAME_1, INFINITE_TIMER_VALUE, null, null);

        addNodes(Set.of(C));

        assertScaleUpNotScheduled(ZONE_NAME_1);
        checkDataNodes(ZONE_NAME_1, clock.now(), nodeNames(A, B));

        // Other zone was not altered and should change its data nodes.
        waitForDataNodes(ZONE_NAME_2, nodeNames(A, B, C));
    }

    @Test
    public void addNodesWithScheduledScaleUp() throws InterruptedException {
        alterZone(ZONE_NAME_1, 5, null, null);

        addNodes(Set.of(C));

        // Task is scheduled after C added.
        assertScaleUpScheduledOrDone(ZONE_NAME_1);
        assertScaleDownNotScheduled(ZONE_NAME_1);

        addNodes(Set.of(D));

        // Check that scale up tasks will be merged and both nodes will be finally added.
        waitForDataNodes(ZONE_NAME_1, nodeNames(A, B, C, D));
    }

    @Test
    public void removeNodesScaleDown() throws InterruptedException {
        removeNodes(Set.of(B));

        assertScaleDownNotScheduled(ZONE_NAME_1);
        checkDataNodes(ZONE_NAME_1, clock.now(), nodeNames(A, B));
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

        waitForDataNodes(ZONE_NAME_1, nodeNames(A));

        // Other zone was not altered and should change its data nodes.
        assertScaleDownNotScheduled(ZONE_NAME_2);
        checkDataNodes(ZONE_NAME_2, clock.now(), nodeNames(A, B));
    }

    @Test
    public void removeNodesWithScheduledScaleDown() throws InterruptedException {
        alterZone(ZONE_NAME_1, null, 5, null);

        removeNodes(Set.of(A));

        // Task is scheduled after A removed.
        assertScaleUpNotScheduled(ZONE_NAME_1);
        assertScaleDownScheduledOrDone(ZONE_NAME_1);

        removeNodes(Set.of(B));

        // Check that scale down tasks will be merged and both nodes will be finally removed.
        waitForDataNodes(ZONE_NAME_1, Set.of());
    }

    @Test
    public void previousDataNodesAreAvailable() throws InterruptedException {
        HybridTimestamp time = clock.now();

        addNodes(Set.of(C));

        waitForDataNodes(ZONE_NAME_1, nodeNames(A, B, C));

        CompletableFuture<Set<String>> previousDataNodes = dataNodesManager.dataNodes(zoneId(ZONE_NAME_1), time);
        assertThat(previousDataNodes, willCompleteSuccessfully());
        assertEquals(nodeNames(A, B), previousDataNodes.join());
    }

    @Test
    public void filterChange() throws InterruptedException {
        alterZone(ZONE_NAME_1, 10, 10, null);

        addNodes(Set.of(C));

        assertScaleUpScheduledOrDone(ZONE_NAME_1);

        removeNodes(Set.of(A));

        assertScaleDownScheduledOrDone(ZONE_NAME_1);

        String newFilter = "$[?(@.region == \"US\")]";

        alterZone(ZONE_NAME_1, null, null, newFilter);

        checkDataNodes(ZONE_NAME_1, clock.now(), nodeNames(C));

        // Timers are discarded earlier than scheduled.
        assertTrue(waitForCondition(() -> !scaleUpScheduled(ZONE_NAME_1) && !scaleDownScheduled(ZONE_NAME_1), 2000));
    }

    @Test
    public void scaleUpChange() throws InterruptedException {
        // Scale up time is big enough.
        alterZone(ZONE_NAME_1, 100, null, null);

        addNodes(Set.of(C));

        assertScaleUpScheduledOrDone(ZONE_NAME_1);

        alterZone(ZONE_NAME_1, IMMEDIATE_TIMER_VALUE, null, null);

        // After the change, scheduled scale up is applied.
        waitForDataNodes(ZONE_NAME_1, nodeNames(A, B, C));
    }

    @Test
    public void scaleDownChange() throws InterruptedException {
        // Scale down time is big enough.
        alterZone(ZONE_NAME_1, null, 100, null);

        removeNodes(Set.of(A));

        assertScaleDownScheduledOrDone(ZONE_NAME_1);

        alterZone(ZONE_NAME_1, null, IMMEDIATE_TIMER_VALUE, null);

        // After the change, scheduled scale up is applied.
        waitForDataNodes(ZONE_NAME_1, nodeNames(B));
    }

    @Test
    public void dataNodesManagerRecovery() throws InterruptedException {
        alterZone(ZONE_NAME_1, 100, 100, null);

        addNodes(Set.of(C));
        removeNodes(Set.of(A));

        assertScaleUpScheduledOrDone(ZONE_NAME_1);
        assertScaleDownScheduledOrDone(ZONE_NAME_1);

        dataNodesManager.stop();

        assertScaleUpNotScheduled(ZONE_NAME_1);
        assertScaleDownNotScheduled(ZONE_NAME_1);

        assertThat(
                dataNodesManager.startAsync(catalogManager.activeCatalog(clock.now().longValue()).zones(), 1), willCompleteSuccessfully()
        );

        assertScaleUpScheduledOrDone(ZONE_NAME_1);
        assertScaleDownScheduledOrDone(ZONE_NAME_1);
    }

    @Test
    public void partitionResetTriggeredOnNodeRemove() throws InterruptedException {
        partitionResetTriggered.set(false);

        CatalogZoneDescriptor zone = descriptor(ZONE_NAME_2);

        dataNodesManager.onTopologyChange(zone, 1, clock.now(), currentTopology, currentTopology);

        // Partition reset is not triggered if no nodes were removed.
        assertFalse(partitionResetTriggered.get());

        Set<NodeWithAttributes> newTopology = new HashSet<>(currentTopology);
        newTopology.remove(A);

        dataNodesManager.onTopologyChange(zone, 1, clock.now(), newTopology, currentTopology);

        assertTrue(waitForCondition(partitionResetTriggered::get, 2000));
    }

    @Test
    public void nodeRejoin() throws InterruptedException {
        removeNodes(Set.of(A));
        addNodes(Set.of(A));

        waitForDataNodes(ZONE_NAME_1, nodeNames(A, B));
    }

    @Test
    public void nodeRejoinWithAnotherNodeJoin() throws InterruptedException {
        addNodes(Set.of(C));
        removeNodes(Set.of(A));
        addNodes(Set.of(A));

        waitForDataNodes(ZONE_NAME_1, nodeNames(A, B, C));
    }

    @Test
    public void nodeRejoinWithAnotherNodeJoinWithAutoAdjustChange() throws InterruptedException {
        alterZone(ZONE_NAME_1, 100, 100, null);

        addNodes(Set.of(C));
        removeNodes(Set.of(A));
        addNodes(Set.of(A));

        alterZone(ZONE_NAME_1, 0, 0, null);

        waitForDataNodes(ZONE_NAME_1, nodeNames(A, B, C));
    }

    @Test
    public void nodeRejoinWithDifferentAttributes() throws InterruptedException {
        removeNodes(Set.of(C));
        addNodes(Set.of(C_DIFFERENT_ATTRS));

        waitForDataNodes(ZONE_NAME_1, nodeNames(A, B, C));

        NodeWithAttributes c = nodeFromHistory(dataNodesHistory(ZONE_NAME_1), C.nodeName(), HybridTimestamp.MAX_VALUE);

        assertEquals(C_DIFFERENT_ATTRS.userAttributes().get("region"), c.userAttributes().get("region"));
    }

    @Test
    public void nodeRejoinWithDifferentAttributesWithAutoAdjustAlteration() throws InterruptedException {
        alterZone(ZONE_NAME_1, 100, 100, null);

        removeNodes(Set.of(C));
        addNodes(Set.of(C_DIFFERENT_ATTRS));

        alterZone(ZONE_NAME_1, 0, 0, null);

        waitForDataNodes(ZONE_NAME_1, nodeNames(A, B, C));

        NodeWithAttributes c = nodeFromHistory(dataNodesHistory(ZONE_NAME_1), C.nodeName(), HybridTimestamp.MAX_VALUE);

        assertEquals(C_DIFFERENT_ATTRS.userAttributes().get("region"), c.userAttributes().get("region"));
    }

    private static NodeWithAttributes nodeFromHistory(DataNodesHistory history, String nodeName, HybridTimestamp timestamp) {
        return history.dataNodesForTimestamp(timestamp).dataNodes().stream()
                .filter(n -> n.nodeName().equals(nodeName))
                .findFirst()
                .orElseThrow();
    }

    private DataNodesHistory dataNodesHistory(String zoneName) {
        CompletableFuture<Entry> entryFut = metaStorageManager.get(zoneDataNodesHistoryKey(zoneId(zoneName)));
        assertThat(entryFut, willCompleteSuccessfully());
        Entry e = entryFut.join();

        assertNotNull(e);
        assertNotNull(e.value());

        return DataNodesHistorySerializer.deserialize(e.value());
    }

    private void addNodes(Set<NodeWithAttributes> nodes) {
        Set<NodeWithAttributes> oldTopology;
        if (currentTopology.isEmpty()) {
            oldTopology = new HashSet<>(nodes);
        } else {
            oldTopology = new HashSet<>(currentTopology);
        }

        currentTopology.addAll(nodes);

        assertThat(
                allOf(
                        catalogManager.activeCatalog(clock.now().longValue()).zones()
                                .stream()
                                .map(zone -> dataNodesManager
                                        .onTopologyChange(zone, 1, clock.now(), currentTopology, oldTopology))
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
                                .map(zone -> dataNodesManager
                                        .onTopologyChange(zone, 1, clock.now(), currentTopology, currentTopology))
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
        }, 10_000);

        if (!success) {
            log.info("Expected: " + expectedNodes);
            log.info("Actual: " + dataNodesManager.dataNodes(zoneId, clock.now()).join());
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

    private void assertScaleUpScheduledOrDone(String zoneName) throws InterruptedException {
        boolean success = waitForCondition(() -> {
            ZoneTimerSchedule schedule = dataNodesManager.zoneTimers(zoneId(zoneName)).scaleUp;
            return schedule.taskIsScheduled() || schedule.taskIsDone();
        }, 2000);

        if (!success) {
            ZoneTimerSchedule schedule = dataNodesManager.zoneTimers(zoneId(zoneName)).scaleUp;
            log.info("Unsuccessful schedule [taskIsScheduled={}, taskIsCancelled={}, taskIsDone={}]."
                    + schedule.taskIsScheduled(), schedule.taskIsCancelled(), schedule.taskIsDone());
        }

        assertTrue(success);
    }

    private void assertScaleUpNotScheduled(String zoneName) throws InterruptedException {
        assertFalse(waitForCondition(() -> dataNodesManager.zoneTimers(zoneId(zoneName)).scaleUp.taskIsScheduled(), 1000));
    }

    private void assertScaleDownScheduledOrDone(String zoneName) throws InterruptedException {
        boolean success = waitForCondition(() -> {
            ZoneTimerSchedule schedule = dataNodesManager.zoneTimers(zoneId(zoneName)).scaleDown;
            return schedule.taskIsScheduled() || schedule.taskIsDone();
        }, 2000);

        if (!success) {
            ZoneTimerSchedule schedule = dataNodesManager.zoneTimers(zoneId(zoneName)).scaleDown;
            log.info("Unsuccessful schedule [taskIsScheduled={}, taskIsCancelled={}, taskIsDone={}]."
                    + schedule.taskIsScheduled(), schedule.taskIsCancelled(), schedule.taskIsDone());
        }

        assertTrue(success);
    }

    private void assertScaleDownNotScheduled(String zoneName) throws InterruptedException {
        assertFalse(waitForCondition(() -> dataNodesManager.zoneTimers(zoneId(zoneName)).scaleDown.taskIsScheduled(), 1000));
    }

    private boolean scaleUpScheduled(String zoneName) {
        return dataNodesManager.zoneTimers(zoneId(zoneName)).scaleUp.taskIsScheduled();
    }

    private boolean scaleDownScheduled(String zoneName) {
        return dataNodesManager.zoneTimers(zoneId(zoneName)).scaleDown.taskIsScheduled();
    }

    private static Set<String> nodeNames(NodeWithAttributes... nodes) {
        return asList(nodes).stream().map(NodeWithAttributes::nodeName).collect(toSet());
    }
}
