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
import static org.apache.ignite.internal.affinity.AffinityUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.getZoneById;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.toDataNodesMap;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil;
import org.apache.ignite.internal.distributionzones.Node;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.MetaStorageWriteCommand;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.schema.configuration.ExtendedTableChange;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Tests the distribution zone dataNodes watch listener in {@link DistributionZoneRebalanceEngine}.
 */
@ExtendWith({MockitoExtension.class, ConfigurationExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class DistributionZoneRebalanceEngineTest extends IgniteAbstractTest {
    private static final String ZONE_NAME = "zone0";

    private static final String ZONE_NAME_1 = "zone1";

    private SimpleInMemoryKeyValueStorage keyValueStorage;

    @Mock
    private ClusterService clusterService;

    private final MetaStorageManager metaStorageManager = mock(MetaStorageManager.class);

    private final DistributionZoneManager distributionZoneManager = mock(DistributionZoneManager.class);

    private final VaultManager vaultManager = mock(VaultManager.class);

    private DistributionZoneRebalanceEngine rebalanceEngine;

    @InjectConfiguration
            ("mock.distributionZones {"
                    + "zone0 = { partitions = 1, replicas = 128, zoneId = 1},"
                    + "zone1 = { partitions = 2, replicas = 128, zoneId = 2}}")
    private DistributionZonesConfiguration zonesConfig;

    private WatchListener watchListener;

    @BeforeEach
    public void setUp() {
        doAnswer(invocation -> {
            ByteArray key = invocation.getArgument(0);

            WatchListener watchListener = invocation.getArgument(1);

            if (Arrays.equals(key.bytes(), zoneDataNodesKey().bytes())) {
                this.watchListener = watchListener;
            }

            return null;
        }).when(metaStorageManager).registerPrefixWatch(any(), any());

        AtomicLong raftIndex = new AtomicLong();

        keyValueStorage = spy(new SimpleInMemoryKeyValueStorage("test"));

        MetaStorageListener metaStorageListener = new MetaStorageListener(keyValueStorage, mock(ClusterTimeImpl.class));

        RaftGroupService metaStorageService = mock(RaftGroupService.class);

        // Delegate directly to listener.
        lenient().doAnswer(
                invocationClose -> {
                    Command cmd = invocationClose.getArgument(0);

                    if (cmd instanceof MetaStorageWriteCommand) {
                        ((MetaStorageWriteCommand) cmd).safeTimeLong(10);
                    }

                    long commandIndex = raftIndex.incrementAndGet();

                    CompletableFuture<Serializable> res = new CompletableFuture<>();

                    CommandClosure<WriteCommand> clo = new CommandClosure<>() {
                        /** {@inheritDoc} */
                        @Override
                        public long index() {
                            return commandIndex;
                        }

                        /** {@inheritDoc} */
                        @Override
                        public WriteCommand command() {
                            return (WriteCommand) cmd;
                        }

                        /** {@inheritDoc} */
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

        lenient().doAnswer(invocationClose -> {
            Iif iif = invocationClose.getArgument(0);

            MultiInvokeCommand multiInvokeCommand = commandsFactory.multiInvokeCommand().iif(iif).build();

            return metaStorageService.run(multiInvokeCommand);
        }).when(metaStorageManager).invoke(any());

        when(clusterService.messagingService()).thenAnswer(invocation -> {
            MessagingService ret = mock(MessagingService.class);

            return ret;
        });

        // stable partitions for tables are empty
        when(metaStorageManager.getAll(any())).thenReturn(completedFuture(Map.of()));

        when(vaultManager.get(any(ByteArray.class))).thenReturn(completedFuture(null));
        when(vaultManager.put(any(ByteArray.class), any(byte[].class))).thenReturn(completedFuture(null));
    }

    @AfterEach
    public void tearDown() {
        keyValueStorage.close();
        rebalanceEngine.stop();
    }

    @Test
    void dataNodesTriggersAssignmentsChanging(
            @InjectConfiguration
                    ("mock.tables {"
                            + "table0 = { zoneId = 1 },"
                            + "table1 = { zoneId = 1 },"
                            + "table2 = { zoneId = 2 },"
                            + "table3 = { zoneId = 2 },"
                            + "table4 = { zoneId = 2 },"
                            + "table5 = { zoneId = 2 }}")
            TablesConfiguration tablesConfiguration
    ) {
        assignTableIds(tablesConfiguration);
        completeTablesConfigs(tablesConfiguration);

        createRebalanceEngine(tablesConfiguration);

        rebalanceEngine.start();

        Set<String> nodes = Set.of("node0", "node1", "node2");

        int zoneId = getZoneId(ZONE_NAME_1);

        watchListenerOnUpdate(zoneId, nodes, 1);

        Map<Integer, Set<String>> zoneNodes = new HashMap<>();

        zoneNodes.put(zoneId, nodes);

        checkAssignments(tablesConfiguration, zoneNodes, RebalanceUtil::pendingPartAssignmentsKey);

        verify(keyValueStorage, timeout(1000).times(8)).invoke(any(), any());
    }

    private static void assignTableIds(TablesConfiguration tablesConfiguration) {
        tablesConfiguration.change(tablesChange -> {
            tablesChange.changeTables(tablesListChange -> {
                for (int i = 0; i < tablesListChange.size(); i++) {
                    TableView tableView = tablesListChange.get(i);
                    int finalI = i;
                    tablesListChange.update(tableView.name(), tableChange -> tableChange.changeId(finalI + 1));
                }
            });
        }).join();
    }

    @Test
    void sequentialAssignmentsChanging(
            @InjectConfiguration ("mock.tables {table0 = { zoneId = 1 }}") TablesConfiguration tablesConfiguration
    ) {
        assignTableIds(tablesConfiguration);
        completeTablesConfigs(tablesConfiguration);

        createRebalanceEngine(tablesConfiguration);

        rebalanceEngine.start();

        Set<String> nodes = Set.of("node0", "node1", "node2");

        int zoneId = getZoneId(ZONE_NAME);

        watchListenerOnUpdate(zoneId, nodes, 1);

        Map<Integer, Set<String>> zoneNodes = new HashMap<>();

        zoneNodes.put(zoneId, nodes);

        checkAssignments(tablesConfiguration, zoneNodes, RebalanceUtil::pendingPartAssignmentsKey);

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any(), any());

        nodes = Set.of("node3", "node4", "node5");

        watchListenerOnUpdate(zoneId, nodes, 2);

        zoneNodes.clear();
        zoneNodes.put(zoneId, nodes);

        checkAssignments(tablesConfiguration, zoneNodes, RebalanceUtil::plannedPartAssignmentsKey);

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any(), any());
    }

    @Test
    void sequentialEmptyAssignmentsChanging(
            @InjectConfiguration("mock.tables {table0 = { zoneId = 1 }}") TablesConfiguration tablesConfiguration
    ) {
        assignTableIds(tablesConfiguration);
        completeTablesConfigs(tablesConfiguration);

        createRebalanceEngine(tablesConfiguration);

        rebalanceEngine.start();

        int zoneId = getZoneId(ZONE_NAME);

        watchListenerOnUpdate(zoneId, null, 1);

        Set<String> nodes = Set.of("node0", "node1", "node2");

        watchListenerOnUpdate(zoneId, nodes, 2);

        Map<Integer, Set<String>> zoneNodes = new HashMap<>();

        zoneNodes.put(zoneId, nodes);

        checkAssignments(tablesConfiguration, zoneNodes, RebalanceUtil::pendingPartAssignmentsKey);

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any(), any());

        Set<String> emptyNodes = emptySet();

        watchListenerOnUpdate(zoneId, emptyNodes, 3);

        zoneNodes.clear();
        zoneNodes.put(zoneId, null);

        checkAssignments(tablesConfiguration, zoneNodes, RebalanceUtil::plannedPartAssignmentsKey);

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any(), any());
    }

    @Test
    void staleDataNodesEvent(
            @InjectConfiguration("mock.tables {table0 = { zoneId = 1 }}") TablesConfiguration tablesConfiguration
    ) {
        assignTableIds(tablesConfiguration);
        completeTablesConfigs(tablesConfiguration);

        createRebalanceEngine(tablesConfiguration);

        rebalanceEngine.start();

        Set<String> nodes = Set.of("node0", "node1", "node2");

        int zoneId = getZoneId(ZONE_NAME);

        watchListenerOnUpdate(zoneId, nodes, 1);

        Map<Integer, Set<String>> zoneNodes = new HashMap<>();

        zoneNodes.put(zoneId, nodes);

        checkAssignments(tablesConfiguration, zoneNodes, RebalanceUtil::pendingPartAssignmentsKey);

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any(), any());

        Set<String> nodes2 = Set.of("node3", "node4", "node5");

        watchListenerOnUpdate(zoneId, nodes2, 1);

        checkAssignments(tablesConfiguration, zoneNodes, RebalanceUtil::pendingPartAssignmentsKey);

        TablePartitionId partId = new TablePartitionId(1, 0);

        assertNull(keyValueStorage.get(RebalanceUtil.plannedPartAssignmentsKey(partId).bytes()).value());

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any(), any());
    }

    @Test
    void replicasTriggersAssignmentsChangingOnNonDefaultZones(
            @InjectConfiguration
                    ("mock.tables {"
                            + "table0 = { zoneId = 1, id = 1 }}")
            TablesConfiguration tablesConfiguration
    ) throws Exception {
        completeTablesConfigs(tablesConfiguration);

        when(distributionZoneManager.dataNodes(anyLong(), anyInt())).thenReturn(completedFuture(Set.of("node0")));

        keyValueStorage.put(stablePartAssignmentsKey(new TablePartitionId(1, 0)).bytes(), toBytes(Set.of("node0")), someTimestamp());

        MetaStorageManager realMetaStorageManager = StandaloneMetaStorageManager.create(vaultManager, keyValueStorage);

        realMetaStorageManager.start();

        try {
            createRebalanceEngine(tablesConfiguration, realMetaStorageManager);

            rebalanceEngine.start();

            CompletableFuture<Void> changeFuture = zonesConfig.change(zonesChange -> zonesChange.changeDistributionZones(
                    zoneListChange -> zoneListChange.update(ZONE_NAME, zoneChange -> zoneChange.changeReplicas(2))
            ));
            assertThat(changeFuture, willCompleteSuccessfully());

            assertTrue(waitForCondition(() -> keyValueStorage.get("assignments.pending.1_part_0".getBytes(UTF_8)) != null, 10_000));
        } finally {
            realMetaStorageManager.stop();
        }
    }

    private static HybridTimestamp someTimestamp() {
        return new HybridTimestamp(System.currentTimeMillis(), 0);
    }

    @Test
    void replicasTriggersAssignmentsChangingOnDefaultZone(
            @InjectConfiguration
                    ("mock.tables {"
                            + "table0 = { zoneId = 0, id = 1 }}")
            TablesConfiguration tablesConfiguration
    ) throws Exception {
        completeTablesConfigs(tablesConfiguration);

        when(distributionZoneManager.dataNodes(anyLong(), anyInt())).thenReturn(completedFuture(Set.of("node0")));

        for (int i = 0; i < 25; i++) {
            keyValueStorage.put(stablePartAssignmentsKey(new TablePartitionId(1, i)).bytes(), toBytes(Set.of("node0")), someTimestamp());
        }

        MetaStorageManager realMetaStorageManager = StandaloneMetaStorageManager.create(vaultManager, keyValueStorage);

        realMetaStorageManager.start();

        try {
            createRebalanceEngine(tablesConfiguration, realMetaStorageManager);

            rebalanceEngine.start();

            CompletableFuture<Void> changeFuture = zonesConfig.change(zonesChange ->
                    zonesChange.changeDefaultDistributionZone(zoneChange -> {
                        zoneChange.changeReplicas(2);
                    })
            );
            assertThat(changeFuture, willCompleteSuccessfully());

            assertTrue(waitForCondition(() -> keyValueStorage.get("assignments.pending.1_part_0".getBytes(UTF_8)) != null, 10_000));
        } finally {
            realMetaStorageManager.stop();
        }
    }

    private static void completeTablesConfigs(TablesConfiguration tablesConfiguration) {
        CompletableFuture<Void> future = tablesConfiguration.change(tablesChange -> {
            tablesChange.changeTables(tablesListChange -> {
                tablesListChange.forEach(
                        tableView -> tablesListChange.update(tableView.name(), tableChange -> {
                            tableChange.changeColumns(columnsListChange -> columnsListChange.create("k1", columnChange -> {
                                columnChange.changeType(typeChange -> typeChange.changeType("string"));
                            }));

                            tableChange.changePrimaryKey(primaryKeyChange -> primaryKeyChange.changeColumns("k1"));

                            ((ExtendedTableChange) tableChange).changeSchemaId(1);
                        }));
            });
        });

        assertThat(future, willCompleteSuccessfully());
    }

    private void createRebalanceEngine(TablesConfiguration tablesConfiguration) {
        createRebalanceEngine(tablesConfiguration, metaStorageManager);
    }

    private void createRebalanceEngine(TablesConfiguration tablesConfiguration, MetaStorageManager metaStorageManager) {
        rebalanceEngine = new DistributionZoneRebalanceEngine(
                new AtomicBoolean(),
                new IgniteSpinBusyLock(),
                zonesConfig,
                tablesConfiguration,
                metaStorageManager,
                distributionZoneManager
        );
    }

    private void checkAssignments(
            TablesConfiguration tablesConfiguration,
            Map<Integer, Set<String>> zoneNodes,
            Function<TablePartitionId, ByteArray> assignmentFunction
    ) {
        tablesConfiguration.tables().value().forEach(tableView -> {
            int tableId = tableView.id();

            DistributionZoneConfiguration distributionZoneConfiguration =
                    getZoneById(zonesConfig, tableView.zoneId());

            for (int j = 0; j < distributionZoneConfiguration.partitions().value(); j++) {
                TablePartitionId partId = new TablePartitionId(tableId, j);

                byte[] actualAssignmentsBytes = keyValueStorage.get(assignmentFunction.apply(partId).bytes()).value();

                Set<String> expectedNodes = zoneNodes.get(tableView.zoneId());

                if (expectedNodes != null) {
                    Set<String> expectedAssignments =
                            calculateAssignmentForPartition(expectedNodes, j, distributionZoneConfiguration.replicas().value())
                                    .stream().map(assignment -> assignment.consistentId()).collect(Collectors.toSet());

                    assertNotNull(actualAssignmentsBytes);

                    Set<String> actualAssignments = ((Set<Assignment>) fromBytes(actualAssignmentsBytes))
                            .stream().map(assignment -> assignment.consistentId()).collect(Collectors.toSet());

                    assertTrue(expectedAssignments.containsAll(actualAssignments));

                    assertEquals(expectedAssignments.size(), actualAssignments.size());
                } else {
                    assertNull(actualAssignmentsBytes);
                }
            }
        });
    }

    private void watchListenerOnUpdate(int zoneId, Set<String> nodes, long rev) {
        byte[] newLogicalTopology;

        if (nodes != null) {
            newLogicalTopology = toBytes(toDataNodesMap(nodes.stream()
                    .map(n -> new Node(n, n))
                    .collect(Collectors.toSet())));
        } else {
            newLogicalTopology = null;
        }

        Entry newEntry = new EntryImpl(zoneDataNodesKey(zoneId).bytes(), newLogicalTopology, rev, 1);

        EntryEvent entryEvent = new EntryEvent(null, newEntry);

        WatchEvent evt = new WatchEvent(entryEvent);

        watchListener.onUpdate(evt);
    }

    private int getZoneId(String zoneName) {
        Integer zoneId = DistributionZonesTestUtil.getZoneId(zonesConfig, zoneName);

        assertNotNull(zoneId, zoneName);

        return zoneId;
    }
}
