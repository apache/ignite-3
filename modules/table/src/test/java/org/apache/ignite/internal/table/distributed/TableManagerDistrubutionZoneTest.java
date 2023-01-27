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

package org.apache.ignite.internal.table.distributed;

import static java.util.Collections.emptySet;
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.internal.affinity.AffinityUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.metastorage.impl.MetaStorageServiceImpl.toIfInfo;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.command.info.StatementResultInfo;
import org.apache.ignite.internal.metastorage.dsl.If;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.ExtendedTableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableChange;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.utils.RebalanceUtil;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;


/**
 * Tests the distribution zone watch listener in {@link TableManager}.
 */
@ExtendWith({MockitoExtension.class, ConfigurationExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class TableManagerDistrubutionZoneTest extends IgniteAbstractTest {
    private SimpleInMemoryKeyValueStorage keyValueStorage;

    private ConfigurationManager clusterCfgMgr;

    @Mock()
    private ClusterService cs;

    private TablesConfiguration tablesConfiguration;

    private WatchListener watchListener;

    private TableManager tableManager;

    @BeforeEach
    public void setUp() {
        clusterCfgMgr = new ConfigurationManager(
                List.of(DistributionZonesConfiguration.KEY),
                Set.of(),
                new TestConfigurationStorage(DISTRIBUTED),
                List.of(),
                List.of()
        );

        MetaStorageManager metaStorageManager = mock(MetaStorageManager.class);

        doAnswer(invocation -> {
            WatchListener listener = invocation.getArgument(1);

            if (watchListener == null) {
                watchListener = listener;
            }

            return null;
        }).when(metaStorageManager).registerPrefixWatch(any(), any());

        tablesConfiguration = mock(TablesConfiguration.class);

        clusterCfgMgr.start();

        AtomicLong raftIndex = new AtomicLong();

        keyValueStorage = spy(new SimpleInMemoryKeyValueStorage("test"));

        MetaStorageListener metaStorageListener = new MetaStorageListener(keyValueStorage);

        RaftGroupService metaStorageService = mock(RaftGroupService.class);

        // Delegate directly to listener.
        lenient().doAnswer(
                invocationClose -> {
                    Command cmd = invocationClose.getArgument(0);

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
            If iif = invocationClose.getArgument(0);

            MultiInvokeCommand multiInvokeCommand =
                    commandsFactory.multiInvokeCommand().iif(toIfInfo(iif, commandsFactory)).build();

            return metaStorageService.run(multiInvokeCommand)
                    .thenApply(bi -> new StatementResult(((StatementResultInfo) bi).result()));
        }).when(metaStorageManager).invoke(any());

        when(cs.messagingService()).thenAnswer(invocation -> {
            MessagingService ret = mock(MessagingService.class);

            return ret;
        });

        tableManager = new TableManager(
                "node1",
                (x) -> {},
                tablesConfiguration,
                cs,
                null,
                null,
                null,
                null,
                null,
                mock(TopologyService.class),
                null,
                null,
                null,
                metaStorageManager,
                mock(SchemaManager.class),
                null,
                null,
                mock(OutgoingSnapshotsManager.class)
        );
    }

    @Test
    void dataNodesTriggersAssignmentsChanging() {
        IgniteBiTuple<TableView, ExtendedTableConfiguration> table0 = mockTable(0, 1, 0);
        IgniteBiTuple<TableView, ExtendedTableConfiguration> table1 = mockTable(1, 2, 0);
        IgniteBiTuple<TableView, ExtendedTableConfiguration> table2 = mockTable(2, 1, 1);
        IgniteBiTuple<TableView, ExtendedTableConfiguration> table3 = mockTable(3, 2, 1);
        IgniteBiTuple<TableView, ExtendedTableConfiguration> table4 = mockTable(4, 1, 1);
        IgniteBiTuple<TableView, ExtendedTableConfiguration> table5 = mockTable(5, 2, 1);

        List<IgniteBiTuple<TableView, ExtendedTableConfiguration>> mockedTables =
                List.of(table0, table1, table2, table3, table4, table5);

        mockTables(mockedTables);

        tableManager.start();

        Set<String> nodes = Set.of("node0", "node1", "node2");

        watchListenerOnUpdate(1, nodes, 1);

        Map<Integer, Set<String>> zoneNodes = new HashMap<>();

        zoneNodes.put(1, nodes);

        checkAssignments(mockedTables, zoneNodes, RebalanceUtil::pendingPartAssignmentsKey);

        verify(keyValueStorage, timeout(1000).times(6)).invoke(any());
    }

    @Test
    void sequentialAssignmentsChanging() {
        IgniteBiTuple<TableView, ExtendedTableConfiguration> table = mockTable(0, 1, 1);

        List<IgniteBiTuple<TableView, ExtendedTableConfiguration>> mockedTables = List.of(table);

        mockTables(mockedTables);

        tableManager.start();

        Set<String> nodes = Set.of("node0", "node1", "node2");

        watchListenerOnUpdate(1, nodes, 1);

        Map<Integer, Set<String>> zoneNodes = new HashMap<>();

        zoneNodes.put(1, nodes);

        checkAssignments(mockedTables, zoneNodes, RebalanceUtil::pendingPartAssignmentsKey);

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        nodes = Set.of("node3", "node4", "node5");

        watchListenerOnUpdate(1, nodes, 2);

        zoneNodes.clear();
        zoneNodes.put(1, nodes);

        checkAssignments(mockedTables, zoneNodes, RebalanceUtil::plannedPartAssignmentsKey);

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any());
    }

    @Test
    void sequentialEmptyAssignmentsChanging() {
        IgniteBiTuple<TableView, ExtendedTableConfiguration> table = mockTable(0, 1, 1);

        List<IgniteBiTuple<TableView, ExtendedTableConfiguration>> mockedTables = List.of(table);

        mockTables(mockedTables);

        tableManager.start();

        Set<String> nodes = Set.of("node0", "node1", "node2");

        watchListenerOnUpdate(1, nodes, 1);

        Map<Integer, Set<String>> zoneNodes = new HashMap<>();

        zoneNodes.put(1, nodes);

        checkAssignments(mockedTables, zoneNodes, RebalanceUtil::pendingPartAssignmentsKey);

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        nodes = emptySet();

        watchListenerOnUpdate(1, nodes, 2);

        zoneNodes.clear();
        zoneNodes.put(1, nodes);

        checkAssignments(mockedTables, zoneNodes, RebalanceUtil::plannedPartAssignmentsKey);

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any());
    }

    @Test
    void staleDataNodesEvent() {
        IgniteBiTuple<TableView, ExtendedTableConfiguration> table = mockTable(0, 1, 1);

        List<IgniteBiTuple<TableView, ExtendedTableConfiguration>> mockedTables = List.of(table);

        mockTables(mockedTables);

        tableManager.start();

        Set<String> nodes = Set.of("node0", "node1", "node2");

        watchListenerOnUpdate(1, nodes, 1);

        Map<Integer, Set<String>> zoneNodes = new HashMap<>();

        zoneNodes.put(1, nodes);

        checkAssignments(mockedTables, zoneNodes, RebalanceUtil::pendingPartAssignmentsKey);

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        Set<String> nodes2 = Set.of("node3", "node4", "node5");

        watchListenerOnUpdate(1, nodes2, 1);

        checkAssignments(mockedTables, zoneNodes, RebalanceUtil::pendingPartAssignmentsKey);

        TablePartitionId partId = new TablePartitionId(new UUID(0, 0), 0);

        assertNull(keyValueStorage.get(RebalanceUtil.plannedPartAssignmentsKey(partId).bytes()).value());

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any());
    }

    private void checkAssignments(
            List<IgniteBiTuple<TableView, ExtendedTableConfiguration>> mockedTables,
            Map<Integer, Set<String>> zoneNodes,
            Function<TablePartitionId, ByteArray> assignmentFunction
    ) {
        for (int i = 0; i < mockedTables.size(); i++) {
            TableView tableView = mockedTables.get(i).get1();

            for (int j = 0; j < tableView.partitions(); j++) {
                TablePartitionId partId = new TablePartitionId(new UUID(0, i), j);

                byte[] actualAssignmentsBytes = keyValueStorage.get(assignmentFunction.apply(partId).bytes()).value();

                Set<String> expectedNodes = zoneNodes.get(tableView.zoneId());

                if (expectedNodes != null) {
                    Set<String> expectedAssignments =
                            calculateAssignmentForPartition(expectedNodes, j, tableView.replicas())
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
        }
    }

    private void watchListenerOnUpdate(int zoneId, Set<String> nodes, long rev) {
        byte[] newLogicalTopology = toBytes(nodes);

        Entry newEntry = new EntryImpl(zoneDataNodesKey(zoneId).bytes(), newLogicalTopology, rev, 1);

        EntryEvent entryEvent = new EntryEvent(null, newEntry);

        WatchEvent evt = new WatchEvent(entryEvent);

        watchListener.onUpdate(evt);
    }

    private IgniteBiTuple<TableView, ExtendedTableConfiguration> mockTable(int tableNum, int partNum, int zoneId) {
        TableView tableView = mock(TableView.class);
        when(tableView.zoneId()).thenReturn(zoneId);
        when(tableView.name()).thenReturn("table" + tableNum);
        when(tableView.replicas()).thenReturn(1);
        when(tableView.partitions()).thenReturn(partNum);

        ExtendedTableConfiguration tableCfg = mock(ExtendedTableConfiguration.class);
        ConfigurationValue valueId = mock(ConfigurationValue.class);
        when(valueId.value()).thenReturn(new UUID(0, tableNum));
        when(tableCfg.id()).thenReturn(valueId);

        return new IgniteBiTuple<>(tableView, tableCfg);

    }

    private void mockTables(List<IgniteBiTuple<TableView, ExtendedTableConfiguration>> mockedTables) {
        NamedListView<TableView> valueList = mock(NamedListView.class);

        when(valueList.namedListKeys()).thenReturn(new ArrayList<>());

        NamedConfigurationTree<TableConfiguration, TableView, TableChange> tables = mock(NamedConfigurationTree.class);

        when(valueList.size()).thenReturn(mockedTables.size());
        when(tables.value()).thenReturn(valueList);

        for (int i = 0; i < mockedTables.size(); i++) {
            IgniteBiTuple<TableView, ExtendedTableConfiguration> mockedTable = mockedTables.get(i);

            TableView tableView = mockedTable.get1();

            when(valueList.get(i)).thenReturn(tableView);

            when(tables.get(tableView.name())).thenReturn(mockedTable.get2());
        }

        ExtendedTableConfiguration tableCfg = mock(ExtendedTableConfiguration.class);
        when(tables.any()).thenReturn(tableCfg);
        when(tableCfg.replicas()).thenReturn(mock(ConfigurationValue.class));
        when(tableCfg.assignments()).thenReturn(mock(ConfigurationValue.class));

        when(tablesConfiguration.tables()).thenReturn(tables);
    }
}
