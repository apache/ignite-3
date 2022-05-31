/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.configuration.schemas.rest.RestConfiguration;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.raft.ConcurrentMapClusterStateStorage;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfiguration;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfigurationSchema;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.rest.RestComponent;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.rocksdb.RocksDbDataStorageModule;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.TableTxManagerImpl;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test suite for rebalance process, when replicas' number changed.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItRebalanceDistributedTest {

    public static final int BASE_PORT = 20_000;

    public static final String HOST = "localhost";

    private static StaticNodeFinder finder;

    private static List<Node> nodes;

    @BeforeEach
    private void before(@WorkDirectory Path workDir, TestInfo testInfo) throws Exception {
        nodes = new ArrayList<>();

        List<NetworkAddress> nodeAddresses = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            nodeAddresses.add(new NetworkAddress(HOST, BASE_PORT + i));
        }

        finder = new StaticNodeFinder(nodeAddresses);

        for (NetworkAddress addr : nodeAddresses) {
            Files.createDirectory(workDir.resolve("" + addr));

            var node = new Node(testInfo, workDir.resolve(addr.toString()), addr);

            nodes.add(node);

            node.start();
        }

    }

    @AfterEach
    private void after() throws Exception {
        for (Node node : nodes) {
            node.stop();
        }
    }

    @Test
    void testOneRebalance(@WorkDirectory Path workDir, TestInfo testInfo) throws Exception {

        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        nodes.get(0).tableManager.createTable(
                "PUBLIC.tbl1",
                tblChanger -> SchemaConfigurationConverter.convert(schTbl1, tblChanger)
                        .changeReplicas(1)
                        .changePartitions(1));

        assertEquals(1, nodes.get(0).clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY)
                .tables().get("PUBLIC.TBL1").replicas().value());

        nodes.get(0).tableManager.alterTable("PUBLIC.TBL1", ch -> ch.changeReplicas(2));

        waitPartitionAssignmentsSyncedToExpected(0, 2);

        assertEquals(2, getAssignments(0, 0).size());
        assertEquals(2, getAssignments(1, 0).size());
        assertEquals(2, getAssignments(2, 0).size());
    }

    @Test
    void testTwoQueuedRebalances(@WorkDirectory Path workDir, TestInfo testInfo) {

        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        nodes.get(0).tableManager.createTable(
                "PUBLIC.tbl1",
                tblChanger -> SchemaConfigurationConverter.convert(schTbl1, tblChanger)
                        .changeReplicas(1)
                        .changePartitions(1));

        assertEquals(1, nodes.get(0).clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables()
                .get("PUBLIC.TBL1").replicas().value());

        nodes.get(0).tableManager.alterTable("PUBLIC.TBL1", ch -> ch.changeReplicas(2));
        nodes.get(0).tableManager.alterTable("PUBLIC.TBL1", ch -> ch.changeReplicas(3));

        waitPartitionAssignmentsSyncedToExpected(0, 3);

        assertEquals(3, getAssignments(0, 0).size());
        assertEquals(3, getAssignments(1, 0).size());
        assertEquals(3, getAssignments(2, 0).size());
    }

    @Test
    void testThreeQueuedRebalances(@WorkDirectory Path workDir, TestInfo testInfo) throws Exception {

        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        nodes.get(0).tableManager.createTable(
                "PUBLIC.tbl1",
                tblChanger -> SchemaConfigurationConverter.convert(schTbl1, tblChanger)
                        .changeReplicas(1)
                        .changePartitions(1));

        assertEquals(1, nodes.get(0).clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables()
                .get("PUBLIC.TBL1").replicas().value());

        nodes.get(0).tableManager.alterTable("PUBLIC.TBL1", ch -> ch.changeReplicas(2));
        nodes.get(0).tableManager.alterTable("PUBLIC.TBL1", ch -> ch.changeReplicas(3));
        nodes.get(0).tableManager.alterTable("PUBLIC.TBL1", ch -> ch.changeReplicas(2));

        waitPartitionAssignmentsSyncedToExpected(0, 2);

        assertEquals(2, getAssignments(0, 0).size());
        assertEquals(2, getAssignments(1, 0).size());
        assertEquals(2, getAssignments(2, 0).size());
    }

    private void waitPartitionAssignmentsSyncedToExpected(int partNum, int replicasNum) {
        while (!IntStream.range(0, 3).allMatch(n -> getAssignments(n, partNum).size() == replicasNum)) {
            LockSupport.parkNanos(10_000_000);
        }
    }

    private List<ClusterNode> getAssignments(int nodeNum, int partNum) {
        var table = ((ExtendedTableConfiguration) nodes.get(nodeNum).clusterCfgMgr.configurationRegistry()
                .getConfiguration(TablesConfiguration.KEY).tables().get("PUBLIC.TBL1"));

        if (table != null) {
            var assignments = table.assignments().value();

            if (assignments != null) {
                return ((List<List<ClusterNode>>) ByteUtils.fromBytes(assignments)).get(partNum);
            }
        }

        return List.of();
    }

    private static class Node {
        private final String name;

        private final VaultManager vaultManager;

        private final ClusterService clusterService;

        private final LockManager lockManager;

        private final TxManager txManager;

        private final Loza raftManager;

        private final MetaStorageManager metaStorageManager;

        private final DistributedConfigurationStorage cfgStorage;

        private final DataStorageManager dataStorageMgr;

        private final TableManager tableManager;

        private final BaselineManager baselineMgr;

        private final ConfigurationManager nodeCfgMgr;

        private final ConfigurationManager clusterCfgMgr;

        private final ClusterManagementGroupManager cmgManager;

        private final SchemaManager schemaManager;

        /**
         * Constructor that simply creates a subset of components of this node.
         */
        Node(TestInfo testInfo, Path workDir, NetworkAddress addr) {

            name = testNodeName(testInfo, addr.port());

            vaultManager = new VaultManager(new PersistentVaultService(workDir.resolve("vault")));

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

            raftManager = new Loza(clusterService, workDir);

            txManager = new TableTxManagerImpl(clusterService, lockManager);

            List<RootKey<?, ?>> rootKeys = List.of(
                    TablesConfiguration.KEY);

            cmgManager = new ClusterManagementGroupManager(
                    vaultManager,
                    clusterService,
                    raftManager,
                    mock(RestComponent.class),
                    new ConcurrentMapClusterStateStorage()
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
                            TablesConfiguration.KEY),
                    Map.of(),
                    cfgStorage,
                    List.of(ExtendedTableConfigurationSchema.class),
                    List.of(UnknownDataStorageConfigurationSchema.class,
                            RocksDbDataStorageConfigurationSchema.class,
                            HashIndexConfigurationSchema.class)
            );

            Consumer<Function<Long, CompletableFuture<?>>> registry = (Function<Long, CompletableFuture<?>> function) -> {
                clusterCfgMgr.configurationRegistry().listenUpdateStorageRevision(
                        newStorageRevision -> function.apply(newStorageRevision));
            };

            TablesConfiguration tablesCfg = clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY);

            DataStorageModules dataStorageModules = new DataStorageModules(List.of(new RocksDbDataStorageModule()));

            dataStorageMgr = new DataStorageManager(
                    tablesCfg,
                    dataStorageModules.createStorageEngines(clusterCfgMgr.configurationRegistry(), workDir.resolve("storage")));

            baselineMgr = new BaselineManager(
                    clusterCfgMgr,
                    metaStorageManager,
                    clusterService);

            schemaManager = new SchemaManager(registry, tablesCfg);

            tableManager = new TableManager(
                    registry,
                    tablesCfg,
                    raftManager,
                    baselineMgr,
                    clusterService.topologyService(),
                    txManager,
                    dataStorageMgr,
                    metaStorageManager,
                    schemaManager);
        }

        /**
         * Starts the created components.
         */
        void start() throws Exception {
            vaultManager.start();

            nodeCfgMgr.start();

            Stream.of(clusterService, clusterCfgMgr, dataStorageMgr, raftManager, txManager, cmgManager,
                    metaStorageManager, baselineMgr, schemaManager, tableManager).forEach(IgniteComponent::start);

            cmgManager.initCluster(List.of(nodes.get(0).name), List.of(), "testCluster");

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
            var components =
                    List.of(tableManager, schemaManager, baselineMgr, metaStorageManager, cmgManager, dataStorageMgr, raftManager,
                            txManager, clusterCfgMgr, clusterService, nodeCfgMgr, vaultManager);

            for (IgniteComponent igniteComponent : components) {
                igniteComponent.beforeNodeStop();
            }

            for (IgniteComponent component : components) {
                component.stop();
            }
        }
    }

}
