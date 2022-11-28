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

package org.apache.ignite.internal.configuration.storage;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.UnsafeMemoryAllocatorConfigurationSchema;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.storage.impl.LocalLogStorageFactory;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.rest.configuration.RestConfiguration;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.ExtendedTableConfiguration;
import org.apache.ignite.internal.schema.configuration.ExtendedTableConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.defaultvalue.ConstantValueDefaultConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.defaultvalue.FunctionCallDefaultConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.defaultvalue.NullValueDefaultConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.index.HashIndexConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.storage.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.schema.testutils.definition.TableDefinition;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryDataStorageModule;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.storage.rocksdb.RocksDbDataStorageModule;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.TableMessageGroup;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.ReverseIterator;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

/**
 * Test suite for rebalance process, when replicas' number changed.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ItRebalanceDistributedTest {
    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ItRebalanceDistributedTest.class);

    public static final int BASE_PORT = 20_000;

    public static final String HOST = "localhost";

    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

    @WorkDirectory
    private Path workDir;

    private StaticNodeFinder finder;

    private List<Node> nodes;

    @BeforeEach
    void before(TestInfo testInfo) throws Exception {
        nodes = new ArrayList<>();

        List<NetworkAddress> nodeAddresses = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            nodeAddresses.add(new NetworkAddress(HOST, BASE_PORT + i));
        }

        finder = new StaticNodeFinder(nodeAddresses);

        for (NetworkAddress addr : nodeAddresses) {
            var node = new Node(testInfo, addr);

            nodes.add(node);

            node.start();
        }

        nodes.get(0).cmgManager.initCluster(List.of(nodes.get(2).name), List.of(), "cluster");
    }

    @AfterEach
    void after() throws Exception {
        for (Node node : nodes) {
            node.stop();
        }
    }

    @Test
    void testOneRebalance() throws Exception {
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        await(nodes.get(0).tableManager.createTableAsync(
                "TBL1",
                tblChanger -> SchemaConfigurationConverter.convert(schTbl1, tblChanger)
                        .changeReplicas(1)
                        .changePartitions(1)));

        assertEquals(1, nodes.get(0).clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY)
                .tables().get("TBL1").replicas().value());

        await(nodes.get(0).tableManager.alterTableAsync("TBL1", ch -> {
            ch.changeReplicas(2);
            return true;
        }));

        waitPartitionAssignmentsSyncedToExpected(0, 2);

        assertEquals(2, getPartitionClusterNodes(0, 0).size());
        assertEquals(2, getPartitionClusterNodes(1, 0).size());
        assertEquals(2, getPartitionClusterNodes(2, 0).size());
    }

    @Test
    void testTwoQueuedRebalances() {
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        await(nodes.get(0).tableManager.createTableAsync(
                "TBL1",
                tblChanger -> SchemaConfigurationConverter.convert(schTbl1, tblChanger)
                        .changeReplicas(1)
                        .changePartitions(1)));

        assertEquals(1, nodes.get(0).clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables()
                .get("TBL1").replicas().value());

        await(nodes.get(0).tableManager.alterTableAsync("TBL1", ch -> {
            ch.changeReplicas(2);
            return true;
        }));

        await(nodes.get(0).tableManager.alterTableAsync("TBL1", ch -> {
            ch.changeReplicas(3);
            return true;
        }));

        waitPartitionAssignmentsSyncedToExpected(0, 3);

        assertEquals(3, getPartitionClusterNodes(0, 0).size());
        assertEquals(3, getPartitionClusterNodes(1, 0).size());
        assertEquals(3, getPartitionClusterNodes(2, 0).size());
    }

    @Test
    void testThreeQueuedRebalances() throws Exception {
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        await(nodes.get(0).tableManager.createTableAsync(
                "TBL1",
                tblChanger -> SchemaConfigurationConverter.convert(schTbl1, tblChanger)
                        .changeReplicas(1)
                        .changePartitions(1)));

        assertEquals(1, nodes.get(0).clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables()
                .get("TBL1").replicas().value());

        await(nodes.get(0).tableManager.alterTableAsync("TBL1", ch -> {
            ch.changeReplicas(2);
            return true;
        }));

        await(nodes.get(0).tableManager.alterTableAsync("TBL1", ch -> {
            ch.changeReplicas(3);
            return true;
        }));

        await(nodes.get(0).tableManager.alterTableAsync("TBL1", ch -> {
            ch.changeReplicas(2);
            return true;
        }));

        waitPartitionAssignmentsSyncedToExpected(0, 2);

        assertEquals(2, getPartitionClusterNodes(0, 0).size());
        assertEquals(2, getPartitionClusterNodes(1, 0).size());
        assertEquals(2, getPartitionClusterNodes(2, 0).size());
    }

    @Test
    void testOnLeaderElectedRebalanceRestart() throws Exception {
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "TBL1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        TableImpl table = (TableImpl) await(nodes.get(1).tableManager.createTableAsync(
                "TBL1",
                tblChanger -> SchemaConfigurationConverter.convert(schTbl1, tblChanger)
                        .changeReplicas(2)
                        .changePartitions(1)));

        Set<String> partitionNodesConsistentIds = getPartitionClusterNodes(0, 0).stream()
                .map(ClusterNode::name)
                .collect(Collectors.toSet());

        Node newNode = nodes.stream().filter(n -> !partitionNodesConsistentIds.contains(n.name)).findFirst().orElseThrow();

        Node leaderNode = findNodeByConsistentId(table.leaderAssignment(0).name());

        String nonLeaderNodeConsistentId = partitionNodesConsistentIds.stream()
                .filter(n -> !n.equals(leaderNode.name))
                .findFirst()
                .orElseThrow();

        TableImpl nonLeaderTable = (TableImpl) findNodeByConsistentId(nonLeaderNodeConsistentId).tableManager.table("TBL1");

        var countDownLatch = new CountDownLatch(1);

        ReplicationGroupId raftGroupNodeName = leaderNode.raftManager.server().startedGroups()
                .stream().filter(grp -> grp.toString().contains("part")).findFirst().get();

        ((JraftServerImpl) leaderNode.raftManager.server()).blockMessages(
                raftGroupNodeName, (msg, peerId) -> {
                    if (peerId.equals(newNode.name) && msg instanceof RpcRequests.PingRequest) {
                        countDownLatch.countDown();

                        return true;
                    }
                    return false;
                });

        await(nodes.get(0).tableManager.alterTableAsync("TBL1", ch -> {
            ch.changeReplicas(3);
            return true;
        }));

        countDownLatch.await();

        nonLeaderTable.internalTable().partitionRaftGroupService(0).transferLeadership(new Peer(nonLeaderNodeConsistentId)).get();

        ((JraftServerImpl) leaderNode.raftManager.server()).stopBlockMessages(raftGroupNodeName);

        waitPartitionAssignmentsSyncedToExpected(0, 3);

        assertEquals(3, getPartitionClusterNodes(0, 0).size());
        assertEquals(3, getPartitionClusterNodes(1, 0).size());
        assertEquals(3, getPartitionClusterNodes(2, 0).size());
    }

    @Test
    void testRebalanceRetryWhenCatchupFailed() throws Exception {
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        await(nodes.get(0).tableManager.createTableAsync(
                "TBL1",
                tblChanger -> SchemaConfigurationConverter.convert(schTbl1, tblChanger)
                        .changeReplicas(1)
                        .changePartitions(1)));

        assertEquals(1, nodes.get(0).clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY)
                .tables().get("TBL1").replicas().value());

        await(nodes.get(0).tableManager.alterTableAsync("TBL1", ch -> {
            ch.changeReplicas(1);
            return true;
        }));

        waitPartitionAssignmentsSyncedToExpected(0, 1);

        JraftServerImpl raftServer = (JraftServerImpl) nodes.stream()
                .filter(n -> n.raftManager.startedGroups().stream().anyMatch(grp -> grp.toString().contains("_part_"))).findFirst()
                .get().raftManager.server();

        AtomicInteger counter = new AtomicInteger(0);

        ReplicationGroupId partGrpId = raftServer.startedGroups().stream().filter(grp -> grp.toString().contains("_part_")).findFirst()
                .get();

        raftServer.blockMessages(partGrpId, (msg, peerId) -> {
            if (msg instanceof RpcRequests.PingRequest) {
                // We block ping request to prevent starting replicator, hence we fail catch up and fail rebalance.
                assertEquals(1, getPartitionClusterNodes(0, 0).size());
                assertEquals(1, getPartitionClusterNodes(1, 0).size());
                assertEquals(1, getPartitionClusterNodes(2, 0).size());
                return counter.incrementAndGet() <= 5;
            }
            return false;
        });

        await(nodes.get(0).tableManager.alterTableAsync("TBL1", ch -> {
            ch.changeReplicas(3);
            return true;
        }));

        waitPartitionAssignmentsSyncedToExpected(0, 3);

        assertEquals(3, getPartitionClusterNodes(0, 0).size());
        assertEquals(3, getPartitionClusterNodes(1, 0).size());
        assertEquals(3, getPartitionClusterNodes(2, 0).size());
    }

    private void waitPartitionAssignmentsSyncedToExpected(int partNum, int replicasNum) {
        while (!IntStream.range(0, nodes.size()).allMatch(n -> getPartitionClusterNodes(n, partNum).size() == replicasNum)) {
            LockSupport.parkNanos(100_000_000);
        }
    }

    private Node findNodeByConsistentId(String consistentId) {
        return nodes.stream().filter(n -> n.name.equals(consistentId)).findFirst().orElseThrow();
    }

    private Set<ClusterNode> getPartitionClusterNodes(int nodeNum, int partNum) {
        var table = ((ExtendedTableConfiguration) nodes.get(nodeNum).clusterCfgMgr.configurationRegistry()
                .getConfiguration(TablesConfiguration.KEY).tables().get("TBL1"));

        if (table != null) {
            var assignments = table.assignments().value();

            if (assignments != null) {
                return ((List<Set<ClusterNode>>) ByteUtils.fromBytes(assignments)).get(partNum);
            }
        }

        return Set.of();
    }

    private class Node {
        private final String name;

        private final VaultManager vaultManager;

        private final ClusterService clusterService;

        private final LockManager lockManager;

        private final TxManager txManager;

        private final Loza raftManager;

        private final ReplicaManager replicaManager;

        private final MetaStorageManager metaStorageManager;

        private final DistributedConfigurationStorage cfgStorage;

        private final DataStorageManager dataStorageMgr;

        private final TableManager tableManager;

        private final BaselineManager baselineMgr;

        private final ConfigurationManager nodeCfgMgr;

        private final ConfigurationManager clusterCfgMgr;

        private final ClusterManagementGroupManager cmgManager;

        private final SchemaManager schemaManager;

        private List<IgniteComponent> nodeComponents;

        /**
         * Constructor that simply creates a subset of components of this node.
         */
        Node(TestInfo testInfo, NetworkAddress addr) {

            name = testNodeName(testInfo, addr.port());

            Path dir = workDir.resolve(name);

            vaultManager = createVault(dir);

            nodeCfgMgr = new ConfigurationManager(
                    List.of(NetworkConfiguration.KEY,
                            RestConfiguration.KEY,
                            ClientConnectorConfiguration.KEY),
                    Map.of(),
                    new LocalConfigurationStorage(vaultManager),
                    List.of(),
                    List.of()
            );

            clusterService = ClusterServiceTestUtils.clusterService(
                    testInfo,
                    addr.port(),
                    finder
            );

            lockManager = new HeapLockManager();

            raftManager = new Loza(clusterService, raftConfiguration, dir, new HybridClockImpl());

            replicaManager = new ReplicaManager(
                    clusterService,
                    new HybridClockImpl(),
                    Set.of(TableMessageGroup.class, TxMessageGroup.class)
            );

            HybridClock hybridClock = new HybridClockImpl();

            ReplicaService replicaSvc = new ReplicaService(
                    clusterService.messagingService(),
                    hybridClock
            );

            txManager = new TxManagerImpl(replicaSvc, lockManager, hybridClock);

            var clusterStateStorage = new TestClusterStateStorage();
            var logicalTopologyService = new LogicalTopologyImpl(clusterStateStorage);

            cmgManager = new ClusterManagementGroupManager(
                    vaultManager,
                    clusterService,
                    raftManager,
                    clusterStateStorage,
                    logicalTopologyService
            );

            metaStorageManager = new MetaStorageManager(
                    vaultManager,
                    clusterService,
                    cmgManager,
                    raftManager,
                    new SimpleInMemoryKeyValueStorage()
            );

            cfgStorage = new DistributedConfigurationStorage(metaStorageManager, vaultManager);

            clusterCfgMgr = new ConfigurationManager(
                    List.of(RocksDbStorageEngineConfiguration.KEY,
                            VolatilePageMemoryStorageEngineConfiguration.KEY,
                            TablesConfiguration.KEY),
                    Map.of(),
                    cfgStorage,
                    List.of(ExtendedTableConfigurationSchema.class),
                    List.of(UnknownDataStorageConfigurationSchema.class,
                            VolatilePageMemoryDataStorageConfigurationSchema.class,
                            UnsafeMemoryAllocatorConfigurationSchema.class,
                            RocksDbDataStorageConfigurationSchema.class,
                            HashIndexConfigurationSchema.class,
                            ConstantValueDefaultConfigurationSchema.class,
                            FunctionCallDefaultConfigurationSchema.class,
                            NullValueDefaultConfigurationSchema.class
                    )
            );

            Consumer<Function<Long, CompletableFuture<?>>> registry = (Function<Long, CompletableFuture<?>> function) -> {
                clusterCfgMgr.configurationRegistry().listenUpdateStorageRevision(
                        newStorageRevision -> function.apply(newStorageRevision));
            };

            TablesConfiguration tablesCfg = clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY);

            DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                    new RocksDbDataStorageModule(), new VolatilePageMemoryDataStorageModule()));

            Path storagePath = dir.resolve("storage");

            dataStorageMgr = new DataStorageManager(
                    tablesCfg,
                    dataStorageModules.createStorageEngines(
                            name,
                            clusterCfgMgr.configurationRegistry(),
                            dir.resolve("storage"),
                            null));

            baselineMgr = new BaselineManager(
                    clusterCfgMgr,
                    metaStorageManager,
                    clusterService);

            schemaManager = new SchemaManager(registry, tablesCfg, metaStorageManager);

            tableManager = new TableManager(
                    name,
                    registry,
                    tablesCfg,
                    raftManager,
                    Mockito.mock(ReplicaManager.class),
                    Mockito.mock(LockManager.class),
                    replicaSvc,
                    baselineMgr,
                    clusterService.topologyService(),
                    txManager,
                    dataStorageMgr,
                    storagePath,
                    metaStorageManager,
                    schemaManager,
                    view -> new LocalLogStorageFactory(),
                    new HybridClockImpl(),
                    new OutgoingSnapshotsManager(clusterService.messagingService())
            );
        }

        /**
         * Starts the created components.
         */
        void start() throws Exception {
            nodeComponents = List.of(
                    vaultManager,
                    nodeCfgMgr,
                    clusterService,
                    raftManager,
                    cmgManager,
                    metaStorageManager,
                    clusterCfgMgr,
                    replicaManager,
                    txManager,
                    baselineMgr,
                    dataStorageMgr,
                    schemaManager,
                    tableManager
            );

            nodeComponents.forEach(IgniteComponent::start);

            CompletableFuture.allOf(
                    nodeCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners(),
                    clusterCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners()
            ).get();

            // deploy watches to propagate data from the metastore into the vault
            metaStorageManager.deployWatches();
        }

        /**
         * Stops the created components.
         */
        void stop() throws Exception {
            new ReverseIterator<>(nodeComponents).forEachRemaining(component -> {
                try {
                    component.beforeNodeStop();
                } catch (Exception e) {
                    LOG.error("Unable to execute before node stop [component={}]", e, component);
                }
            });

            new ReverseIterator<>(nodeComponents).forEachRemaining(component -> {
                try {
                    component.stop();
                } catch (Exception e) {
                    LOG.error("Unable to stop component [component={}]", e, component);
                }
            });

        }

        NetworkAddress address() {
            return clusterService.topologyService().localMember().address();
        }
    }

    /**
     * Starts the Vault component.
     */
    private static VaultManager createVault(Path workDir) {
        Path vaultPath = workDir.resolve(Paths.get("vault"));

        try {
            Files.createDirectories(vaultPath);
        } catch (IOException e) {
            throw new IgniteInternalException(e);
        }

        return new VaultManager(new PersistentVaultService(vaultPath));
    }
}
