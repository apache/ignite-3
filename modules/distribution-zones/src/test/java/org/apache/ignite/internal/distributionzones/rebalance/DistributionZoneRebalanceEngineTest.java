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

package org.apache.ignite.internal.distributionzones.rebalance;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.createTestCatalogManager;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createDefaultZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.getDefaultZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.getZoneIdStrict;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesHistoryKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.metastorage.server.KeyValueUpdateContext.kvContext;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignments;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.components.SystemPropertiesNodeProperties;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.distributionzones.DataNodesHistory;
import org.apache.ignite.internal.distributionzones.DataNodesHistory.DataNodesHistorySerializer;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.MetaStorageWriteCommand;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.impl.CommandIdGenerator;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests the distribution zone dataNodes watch listener in {@link DistributionZoneRebalanceEngine}.
 */
@ExtendWith(ConfigurationExtension.class)
public class DistributionZoneRebalanceEngineTest extends IgniteAbstractTest {
    private static final String ZONE_NAME_0 = "zone0";

    private static final String ZONE_NAME_1 = "zone1";

    private static final String TABLE_NAME = "table";

    private SimpleInMemoryKeyValueStorage keyValueStorage;

    private final ClusterService clusterService = mock(ClusterService.class);

    private final MetaStorageManager metaStorageManager = mock(MetaStorageManager.class);

    private final DistributionZoneManager distributionZoneManager = mock(DistributionZoneManager.class);

    private DistributionZoneRebalanceEngine rebalanceEngine;

    private WatchListener watchListener;

    private final HybridClock clock = new HybridClockImpl();

    private CatalogManager catalogManager;

    private Map<UUID, NodeWithAttributes> nodeWithAttributesMap;

    @BeforeEach
    public void setUp() {
        String nodeName = "test";

        catalogManager = createTestCatalogManager(nodeName, clock);
        assertThat(catalogManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        createZone(ZONE_NAME_0, 1, 128);
        createZone(ZONE_NAME_1, 2, 128);

        nodeWithAttributesMap = Map.of(
                id(0),  new NodeWithAttributes("node0", id(0), Map.of(), List.of(DEFAULT_STORAGE_PROFILE)),
                id(1),  new NodeWithAttributes("node1", id(1), Map.of(), List.of(DEFAULT_STORAGE_PROFILE)),
                id(2),  new NodeWithAttributes("node2", id(2), Map.of(), List.of(DEFAULT_STORAGE_PROFILE)),
                id(3),  new NodeWithAttributes("node3", id(3), Map.of(), List.of(DEFAULT_STORAGE_PROFILE)),
                id(4),  new NodeWithAttributes("node4", id(4), Map.of(), List.of(DEFAULT_STORAGE_PROFILE)),
                id(5),  new NodeWithAttributes("node5", id(5), Map.of(), List.of(DEFAULT_STORAGE_PROFILE))
        );

        doAnswer(invocation -> {
            ByteArray key = invocation.getArgument(0);

            WatchListener watchListener = invocation.getArgument(1);

            if (new String(key.bytes(), UTF_8).startsWith(DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX)) {
                this.watchListener = watchListener;
            }

            return null;
        }).when(metaStorageManager).registerPrefixWatch(any(), any());

        when(metaStorageManager.recoveryFinishedFuture()).thenReturn(completedFuture(new Revisions(1, -1)));

        AtomicLong raftIndex = new AtomicLong();

        keyValueStorage = spy(new SimpleInMemoryKeyValueStorage(nodeName));

        ClusterTimeImpl clusterTime = new ClusterTimeImpl(nodeName, new IgniteSpinBusyLock(), clock);

        MetaStorageListener metaStorageListener = new MetaStorageListener(keyValueStorage, clock, clusterTime);

        RaftGroupService metaStorageService = mock(RaftGroupService.class);

        // Delegate directly to listener.
        lenient().doAnswer(
                invocationClose -> {
                    Command cmd = invocationClose.getArgument(0);

                    if (cmd instanceof MetaStorageWriteCommand) {
                        ((MetaStorageWriteCommand) cmd).safeTime(hybridTimestamp(10));
                    }

                    long commandIndex = raftIndex.incrementAndGet();

                    CompletableFuture<Serializable> res = new CompletableFuture<>();

                    CommandClosure<WriteCommand> clo = new CommandClosure<>() {
                        @Override
                        public long index() {
                            return commandIndex;
                        }

                        @Override
                        public WriteCommand command() {
                            return (WriteCommand) cmd;
                        }

                        @Override
                        public void result(@Nullable Serializable r) {
                            if (r instanceof Throwable) {
                                res.completeExceptionally((Throwable) r);
                            } else {
                                res.complete(r);
                            }
                        }
                    };

                    try {
                        metaStorageListener.onWrite(List.of(clo).iterator());
                    } catch (Throwable e) {
                        res.completeExceptionally(new IgniteInternalException(e));
                    }

                    return res;
                }
        ).when(metaStorageService).run(any());

        MetaStorageCommandsFactory commandsFactory = new MetaStorageCommandsFactory();

        CommandIdGenerator commandIdGenerator = new CommandIdGenerator(UUID.randomUUID());

        lenient().doAnswer(invocationClose -> {
            Iif iif = invocationClose.getArgument(0);

            MultiInvokeCommand multiInvokeCommand = commandsFactory.multiInvokeCommand()
                    .iif(iif)
                    .id(commandIdGenerator.newId())
                    .initiatorTime(clock.now())
                    .build();

            return metaStorageService.run(multiInvokeCommand);
        }).when(metaStorageManager).invoke(any());

        when(clusterService.messagingService()).thenAnswer(invocation -> {
            MessagingService ret = mock(MessagingService.class);

            return ret;
        });

        // stable partitions for tables
        lenient().doAnswer(invocation -> {
            Set<ByteArray> keys = invocation.getArgument(0);

            Map<ByteArray, Entry> result = new HashMap<>();

            for (var k : keys) {
                var v = keyValueStorage.get(k.bytes());

                result.put(k, v);
            }

            return completedFuture(result);
        }).when(metaStorageManager).getAll(any());
    }

    private static UUID id(int n) {
        return new UUID(0, n);
    }

    @AfterEach
    public void tearDown() throws Exception {
        closeAll(
                catalogManager == null ? null :
                        () -> assertThat(catalogManager.stopAsync(new ComponentContext()), willCompleteSuccessfully()),
                keyValueStorage == null ? null : keyValueStorage::close,
                rebalanceEngine == null ? null : rebalanceEngine::stop
        );
    }

    @Test
    void dataNodesTriggersAssignmentsChanging() {
        createTable(ZONE_NAME_0, TABLE_NAME + 0);
        createTable(ZONE_NAME_0, TABLE_NAME + 1);
        createTable(ZONE_NAME_1, TABLE_NAME + 2);
        createTable(ZONE_NAME_1, TABLE_NAME + 3);
        createTable(ZONE_NAME_1, TABLE_NAME + 4);
        createTable(ZONE_NAME_1, TABLE_NAME + 5);

        createRebalanceEngine();

        rebalanceEngine.startAsync(catalogManager.latestCatalogVersion());

        Set<String> nodes = Set.of("node0", "node1", "node2");

        int zoneId = getZoneId(ZONE_NAME_1);

        watchListenerOnUpdate(zoneId, nodes, 1);

        Map<Integer, Set<String>> zoneNodes = new HashMap<>();

        zoneNodes.put(zoneId, nodes);

        checkAssignments(zoneNodes, RebalanceUtil::pendingPartAssignmentsQueueKey, bytes -> AssignmentsQueue.fromBytes(bytes).poll());

        verify(keyValueStorage, timeout(1000).times(8)).invoke(any(), any(), any());
    }

    @Test
    void sequentialAssignmentsChanging() {
        createTable(ZONE_NAME_0, TABLE_NAME);

        createRebalanceEngine();

        rebalanceEngine.startAsync(catalogManager.latestCatalogVersion());

        Set<String> nodes = Set.of("node0", "node1", "node2");

        int zoneId = getZoneId(ZONE_NAME_0);

        watchListenerOnUpdate(zoneId, nodes, 1);

        Map<Integer, Set<String>> zoneNodes = new HashMap<>();

        zoneNodes.put(zoneId, nodes);

        checkAssignments(zoneNodes, RebalanceUtil::pendingPartAssignmentsQueueKey, bytes -> AssignmentsQueue.fromBytes(bytes).poll());

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any(), any(), any());

        nodes = Set.of("node3", "node4", "node5");

        watchListenerOnUpdate(zoneId, nodes, 2);

        zoneNodes.clear();
        zoneNodes.put(zoneId, nodes);

        checkAssignments(zoneNodes, RebalanceUtil::plannedPartAssignmentsKey);

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any(), any(), any());
    }

    @Test
    void sequentialEmptyAssignmentsChanging() {
        createTable(ZONE_NAME_0, TABLE_NAME);

        createRebalanceEngine();

        rebalanceEngine.startAsync(catalogManager.latestCatalogVersion());

        int zoneId = getZoneId(ZONE_NAME_0);

        watchListenerOnUpdate(zoneId, null, 1);

        Set<String> nodes = Set.of("node0", "node1", "node2");

        watchListenerOnUpdate(zoneId, nodes, 2);

        Map<Integer, Set<String>> zoneNodes = new HashMap<>();

        zoneNodes.put(zoneId, nodes);

        checkAssignments(zoneNodes, RebalanceUtil::pendingPartAssignmentsQueueKey, bytes -> AssignmentsQueue.fromBytes(bytes).poll());

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any(), any(), any());

        Set<String> emptyNodes = emptySet();

        watchListenerOnUpdate(zoneId, emptyNodes, 3);

        zoneNodes.clear();
        zoneNodes.put(zoneId, null);

        checkAssignments(zoneNodes, RebalanceUtil::plannedPartAssignmentsKey);

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any(), any(), any());
    }

    @Test
    void staleDataNodesEvent() {
        createTable(ZONE_NAME_0, TABLE_NAME);

        createRebalanceEngine();

        rebalanceEngine.startAsync(catalogManager.latestCatalogVersion());

        Set<String> nodes = Set.of("node0", "node1", "node2");

        int zoneId = getZoneId(ZONE_NAME_0);

        watchListenerOnUpdate(zoneId, nodes, 1);

        Map<Integer, Set<String>> zoneNodes = new HashMap<>();

        zoneNodes.put(zoneId, nodes);

        checkAssignments(zoneNodes, RebalanceUtil::pendingPartAssignmentsQueueKey, bytes -> AssignmentsQueue.fromBytes(bytes).poll());

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any(), any(), any());

        Set<String> nodes2 = Set.of("node3", "node4", "node5");

        watchListenerOnUpdate(zoneId, nodes2, 1);

        checkAssignments(zoneNodes, RebalanceUtil::pendingPartAssignmentsQueueKey, bytes -> AssignmentsQueue.fromBytes(bytes).poll());

        TablePartitionId partId = new TablePartitionId(getTableId(TABLE_NAME), 0);

        assertNull(keyValueStorage.get(RebalanceUtil.plannedPartAssignmentsKey(partId).bytes()).value());

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any(), any(), any());
    }

    @Test
    void replicasTriggersAssignmentsChangingOnNonDefaultZones() throws Exception {
        createTable(ZONE_NAME_0, TABLE_NAME);

        when(distributionZoneManager.dataNodes(anyInt(), anyInt())).thenReturn(completedFuture(Set.of("node0")));

        int catalogVersion = catalogManager.latestCatalogVersion();
        long timestamp = catalogManager.catalog(catalogVersion).time();

        byte[] assignmentsBytes = Assignments.of(timestamp, Assignment.forPeer("node0")).toBytes();

        keyValueStorage.put(
                stablePartAssignmentsKey(new TablePartitionId(getTableId(TABLE_NAME), 0)).bytes(), assignmentsBytes,
                kvContext(clock.now())
        );

        MetaStorageManager realMetaStorageManager = StandaloneMetaStorageManager.create(keyValueStorage);

        assertThat(realMetaStorageManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        try {
            createRebalanceEngine(realMetaStorageManager);

            rebalanceEngine.startAsync(catalogManager.latestCatalogVersion());

            alterZone(ZONE_NAME_0, 2);

            assertTrue(waitForCondition(() -> keyValueStorage.get("assignments.pending.1_part_0".getBytes(UTF_8)) != null, 10_000));
        } finally {
            assertThat(realMetaStorageManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());
        }
    }

    @Test
    void replicasTriggersAssignmentsChangingOnDefaultZone() throws Exception {
        createDefaultZone(catalogManager);

        createTable(ZONE_NAME_0, TABLE_NAME);

        when(distributionZoneManager.dataNodes(anyInt(), anyInt())).thenReturn(completedFuture(Set.of("node0")));

        int catalogVersion = catalogManager.latestCatalogVersion();
        long timestamp = catalogManager.catalog(catalogVersion).time();

        for (int i = 0; i < 25; i++) {
            byte[] assignmentsBytes = Assignments.of(timestamp, Assignment.forPeer("node0")).toBytes();

            keyValueStorage.put(
                    stablePartAssignmentsKey(new TablePartitionId(getTableId(TABLE_NAME), i)).bytes(), assignmentsBytes,
                    kvContext(clock.now())
            );
        }

        MetaStorageManager realMetaStorageManager = StandaloneMetaStorageManager.create(keyValueStorage);

        assertThat(realMetaStorageManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        try {
            createRebalanceEngine(realMetaStorageManager);

            rebalanceEngine.startAsync(catalogManager.latestCatalogVersion());

            alterZone(getDefaultZone(catalogManager, clock.nowLong()).name(), 2);

            assertTrue(waitForCondition(() -> keyValueStorage.get("assignments.pending.1_part_0".getBytes(UTF_8)) != null, 10_000));
        } finally {
            assertThat(realMetaStorageManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());
        }
    }

    private void createRebalanceEngine() {
        createRebalanceEngine(metaStorageManager);
    }

    private void createRebalanceEngine(MetaStorageManager metaStorageManager) {
        rebalanceEngine = new DistributionZoneRebalanceEngine(
                new IgniteSpinBusyLock(),
                metaStorageManager,
                distributionZoneManager,
                catalogManager,
                new SystemPropertiesNodeProperties()
        );
    }

    private void checkAssignments(Map<Integer, Set<String>> zoneNodes, Function<TablePartitionId, ByteArray> assignmentFunction) {
        checkAssignments(zoneNodes, assignmentFunction, Assignments::fromBytes);
    }

    private void checkAssignments(
            Map<Integer, Set<String>> zoneNodes,
            Function<TablePartitionId, ByteArray> assignmentFunction,
            Function<byte[], Assignments> assignmentsFromBytesFunction
    ) {
        Catalog catalog = catalogManager.catalog(catalogManager.latestCatalogVersion());

        catalog.tables().forEach(tableDescriptor -> {
            int tableId = tableDescriptor.id();

            CatalogZoneDescriptor zoneDescriptor = catalog.zone(tableDescriptor.zoneId());

            assertNotNull(zoneDescriptor, "tableName=" + tableDescriptor.name() + ", zoneId=" + tableDescriptor.zoneId());

            for (int j = 0; j < zoneDescriptor.partitions(); j++) {
                TablePartitionId partId = new TablePartitionId(tableId, j);

                byte[] actualAssignmentsBytes = keyValueStorage.get(assignmentFunction.apply(partId).bytes()).value();

                Set<String> expectedNodes = zoneNodes.get(tableDescriptor.zoneId());

                if (expectedNodes != null) {
                    Set<Assignment> calculatedAssignments = calculateAssignmentForPartition(
                            expectedNodes,
                            j,
                            zoneDescriptor.partitions(),
                            zoneDescriptor.replicas(),
                            zoneDescriptor.consensusGroupSize()
                    );
                    Set<String> expectedAssignments = calculatedAssignments.stream().map(Assignment::consistentId).collect(toSet());

                    assertNotNull(actualAssignmentsBytes);

                    Set<String> actualAssignments = assignmentsFromBytesFunction.apply(actualAssignmentsBytes).nodes()
                            .stream().map(Assignment::consistentId).collect(toSet());

                    assertTrue(expectedAssignments.containsAll(actualAssignments));

                    assertEquals(expectedAssignments.size(), actualAssignments.size());
                } else {
                    assertNull(actualAssignmentsBytes);
                }
            }
        });
    }

    private void watchListenerOnUpdate(int zoneId, @Nullable Set<String> nodes, long rev) {
        byte[] newLogicalTopology;

        if (nodes != null) {
            Set<NodeWithAttributes> nodeWithAttributes = nodes.stream()
                    .map(n -> nodeWithAttributesMap.get(findNodeIdByConsistentId(n)))
                    .collect(toSet());

            DataNodesHistory history = new DataNodesHistory().addHistoryEntry(clock.now(), nodeWithAttributes);

            newLogicalTopology = DataNodesHistorySerializer.serialize(history);
        } else {
            newLogicalTopology = null;
        }

        Entry newEntry = new EntryImpl(zoneDataNodesHistoryKey(zoneId).bytes(), newLogicalTopology, rev, hybridTimestamp(rev));

        EntryEvent entryEvent = new EntryEvent(null, newEntry);

        WatchEvent evt = new WatchEvent(entryEvent);

        watchListener.onUpdate(evt);
    }

    private UUID findNodeIdByConsistentId(String consistentId) {
        return nodeWithAttributesMap.values().stream()
                .filter(node -> node.nodeName().equals(consistentId))
                .findAny()
                .orElseThrow(() -> new RuntimeException("Did not find any node by consistentId='" + consistentId + "'"))
                .nodeId();
    }

    private void createZone(String zoneName, int partitions, int replicas) {
        DistributionZonesTestUtil.createZone(catalogManager, zoneName, partitions, replicas);
    }

    private void alterZone(String zoneName, int replicas) {
        DistributionZonesTestUtil.alterZone(catalogManager, zoneName, replicas);
    }

    private void createTable(String zoneName, String tableName) {
        TableTestUtils.createTable(
                catalogManager,
                SqlCommon.DEFAULT_SCHEMA_NAME,
                zoneName,
                tableName,
                List.of(ColumnParams.builder().name("k1").type(STRING).length(100).build()),
                List.of("k1")
        );

        var tableId = getTableId(tableName);
        var zoneId = getZoneId(zoneName);

        Catalog catalog = catalogManager.catalog(catalogManager.latestCatalogVersion());
        CatalogZoneDescriptor zoneDescriptor = catalog.zone(zoneId);

        Set<String> initialDataNodes = Set.of("node0");
        List<Set<Assignment>> initialAssignments = calculateAssignments(
                initialDataNodes,
                zoneDescriptor.partitions(),
                zoneDescriptor.replicas(),
                zoneDescriptor.consensusGroupSize()
        );

        long timestamp = catalog.time();

        for (int i = 0; i < initialAssignments.size(); i++) {
            var stableAssignmentPartitionKey = stablePartAssignmentsKey(new TablePartitionId(tableId, i)).bytes();

            keyValueStorage.put(stableAssignmentPartitionKey,
                    Assignments.toBytes(initialAssignments.get(i), timestamp),
                    kvContext(clock.now())
            );
        }
    }

    private int getZoneId(String zoneName) {
        return getZoneIdStrict(catalogManager, zoneName, clock.nowLong());
    }

    private int getTableId(String tableName) {
        return getTableIdStrict(catalogManager, tableName, clock.nowLong());
    }
}
