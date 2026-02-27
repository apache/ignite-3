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

package org.apache.ignite.distributed;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.waitForTopology;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignments;
import static org.apache.ignite.internal.replicator.ReplicatorConstants.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CompletableFutures.emptySetCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.RaftGroupOptionsConfigHelper;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.TestMetricManager;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NodeFinder;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager.TablePartitionReplicaProcessorFactory;
import org.apache.ignite.internal.partition.replicator.ZonePartitionReplicaListener;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.raft.ZonePartitionRaftListener;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.PartitionsSnapshots;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.TestLozaFactory;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.VolatileLogStorageManagerCreator;
import org.apache.ignite.internal.raft.util.SharedLogStorageManagerUtils;
import org.apache.ignite.internal.replicator.Replica;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaChangeCommand;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.schema.AlwaysSyncedSchemaSyncService;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor.StorageHashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.HashIndexLocker;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService;
import org.apache.ignite.internal.table.distributed.raft.PartitionSafeTimeValidator;
import org.apache.ignite.internal.table.distributed.raft.TablePartitionProcessor;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.schema.ConstantSchemaVersions;
import org.apache.ignite.internal.table.distributed.schema.ThreadLocalPartitionCommandsMarshaller;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.table.impl.DummyValidationSchemasSource;
import org.apache.ignite.internal.table.metrics.TableMetricSource;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.ResourceVacuumManager;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.impl.TransactionStateResolver;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.impl.TxMessageSender;
import org.apache.ignite.internal.tx.impl.VolatileTxStateMetaStorage;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStatePartitionStorage;
import org.apache.ignite.internal.tx.test.TestLocalRwTxCounter;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.SafeTimeValuesTracker;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.QualifiedNameHelper;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.TestInfo;

/**
 * Class that allows to mock a cluster for transaction tests' purposes.
 */
public class ItTxTestCluster {
    private static final int SCHEMA_VERSION = 1;

    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private final List<NetworkAddress> localAddresses;

    private final NodeFinder nodeFinder;

    private final RaftConfiguration raftConfig;

    private final TransactionConfiguration txConfiguration;

    private final SystemLocalConfiguration systemLocalConfig;

    private final SystemDistributedConfiguration systemDistributedConfig;

    private final Path workDir;

    private final int nodes;

    private final int replicas;

    private final boolean startClient;

    private static final IgniteLogger LOG = Loggers.forClass(ItTxTestCluster.class);

    public static final int NODE_PORT_BASE = 20_000;

    private ClusterService client;

    private HybridClock clientClock;
    private ClockWaiter clientClockWaiter;
    private ClockService clientClockService;

    private Map<String, HybridClock> clocks;
    private Collection<ClockWaiter> clockWaiters;
    protected Map<String, ClockService> clockServices;

    private ReplicaService clientReplicaSvc;

    protected Map<String, Loza> raftServers = new HashMap<>();

    protected Map<String, ReplicaManager> replicaManagers;

    protected Map<String, RaftGroupOptionsConfigurer> raftConfigurers;

    protected Map<String, LogStorageManager> logStorageFactories;

    protected Map<String, ReplicaService> replicaServices;

    protected Map<String, TxManager> txManagers;

    private Map<String, ResourceVacuumManager> resourceCleanupManagers;

    protected Map<String, TransactionInflights> txInflights;

    protected Map<String, RemotelyTriggeredResourceRegistry> cursorRegistries;

    private TransactionInflights clientTransactionInflights;

    protected TxManager clientTxManager;

    private ResourceVacuumManager clientResourceVacuumManager;

    protected TransactionStateResolver clientTxStateResolver;

    protected Map<String, TableImpl> tables = new HashMap<>();

    protected Map<String, TxStatePartitionStorage> txStateStorages;

    private Map<String, ClusterService> clusterServices;

    protected final List<ClusterService> cluster = new CopyOnWriteArrayList<>();

    protected PlacementDriver placementDriver;

    private ScheduledThreadPoolExecutor executor;

    private ExecutorService partitionOperationsExecutor;

    protected IgniteTransactions igniteTransactions;

    protected String localNodeName;

    private SchemaSyncService schemaSyncService;

    private final CompoundValidationSchemasSource validationSchemasSource = new CompoundValidationSchemasSource();

    private final Map<String, Map<ZonePartitionId, ZonePartitionRaftListener>> zonePartitionRaftGroupListeners = new HashMap<>();

    private final Map<String, Map<ZonePartitionId, ZonePartitionReplicaListener>> zonePartitionReplicaListeners = new HashMap<>();

    private final Map<String, Map<ZonePartitionId, SafeTimeValuesTracker>> safeTimeTrackers = new ConcurrentHashMap<>();

    private final ClusterNodeResolver nodeResolver = new ClusterNodeResolver() {
        @Override
        public @Nullable InternalClusterNode getById(UUID id) {
            for (ClusterService service : cluster) {
                InternalClusterNode clusterNode = service.topologyService().localMember();

                if (clusterNode.id().equals(id)) {
                    return clusterNode;
                }
            }

            if (client != null && client.topologyService().localMember().id().equals(id)) {
                return client.topologyService().localMember();
            }

            return null;
        }

        @Override
        public @Nullable InternalClusterNode getByConsistentId(String consistentId) {
            for (ClusterService service : cluster) {
                InternalClusterNode clusterNode = service.topologyService().localMember();

                if (clusterNode.name().equals(consistentId)) {
                    return clusterNode;
                }
            }

            return null;
        }
    };

    private final TestInfo testInfo;

    /** Observable timestamp tracker. */
    private final HybridTimestampTracker timestampTracker;

    private final ReplicationConfiguration replicationConfiguration;

    private CatalogService catalogService;

    private Catalog catalog;

    private final AtomicInteger globalCatalogId = new AtomicInteger();

    protected final TestLowWatermark lowWatermark = new TestLowWatermark();

    /**
     * The constructor.
     */
    public ItTxTestCluster(
            TestInfo testInfo,
            RaftConfiguration raftConfig,
            TransactionConfiguration txConfiguration,
            SystemLocalConfiguration systemLocalConfig,
            SystemDistributedConfiguration systemDistributedConfig,
            Path workDir,
            int nodes,
            int replicas,
            boolean startClient,
            HybridTimestampTracker timestampTracker,
            ReplicationConfiguration replicationConfiguration
    ) {
        this.raftConfig = raftConfig;
        this.txConfiguration = txConfiguration;
        this.systemLocalConfig = systemLocalConfig;
        this.systemDistributedConfig = systemDistributedConfig;
        this.workDir = workDir;
        this.nodes = nodes;
        this.replicas = replicas;
        this.startClient = startClient;
        this.testInfo = testInfo;
        this.timestampTracker = timestampTracker;
        this.replicationConfiguration = replicationConfiguration;

        localAddresses = findLocalAddresses(NODE_PORT_BASE, NODE_PORT_BASE + nodes);
        nodeFinder = new StaticNodeFinder(localAddresses);
    }

    /**
     * Initialize the test state.
     */
    public void prepareCluster() throws Exception {
        assertTrue(nodes > 0);
        assertTrue(replicas > 0);

        clusterServices = new ConcurrentHashMap<>(nodes);

        localAddresses.parallelStream()
                .forEach(addr -> {
                    ClusterService svc = startNode(testInfo, addr.toString(), addr.port(), nodeFinder);
                    cluster.add(svc);
                    clusterServices.put(extractConsistentId(svc), svc);
                });

        for (ClusterService node : cluster) {
            assertTrue(waitForTopology(node, nodes, 1000));
        }

        InternalClusterNode firstNode = first(cluster).topologyService().localMember();

        placementDriver = new TestPlacementDriver(firstNode);

        catalogService = mock(CatalogService.class);
        catalog = mock(Catalog.class);
        lenient().when(catalogService.activeCatalog(anyLong())).thenReturn(catalog);
        lenient().when(catalogService.catalog(anyInt())).thenReturn(catalog);

        LOG.info("The cluster has been started");

        if (startClient) {
            startClient();
        }

        // Start raft servers. Each raft server can hold multiple groups.
        clocks = new HashMap<>(nodes);
        clockWaiters = new ArrayList<>(nodes);
        clockServices = new HashMap<>(nodes);
        raftServers = new HashMap<>(nodes);
        replicaManagers = new HashMap<>(nodes);
        replicaServices = new HashMap<>(nodes);
        txManagers = new HashMap<>(nodes);
        resourceCleanupManagers = new HashMap<>(nodes);
        txInflights = new HashMap<>(nodes);
        cursorRegistries = new HashMap<>(nodes);
        txStateStorages = new HashMap<>(nodes);
        raftConfigurers = new HashMap<>(nodes);
        logStorageFactories = new HashMap<>(nodes);

        executor = new ScheduledThreadPoolExecutor(20,
                IgniteThreadFactory.create("test", Loza.CLIENT_POOL_NAME, LOG));

        partitionOperationsExecutor = Executors.newFixedThreadPool(
                5,
                IgniteThreadFactory.create("test", "partition-operations", LOG)
        );

        for (int i = 0; i < nodes; i++) {
            ClusterService clusterService = cluster.get(i);

            InternalClusterNode node = clusterService.topologyService().localMember();

            HybridClock clock = createClock(node);
            ClockWaiter clockWaiter = new ClockWaiter("test-node" + i, clock, executor);
            assertThat(clockWaiter.startAsync(new ComponentContext()), willCompleteSuccessfully());
            TestClockService clockService = new TestClockService(clock, clockWaiter);

            String nodeName = node.name();

            clocks.put(nodeName, clock);
            clockWaiters.add(clockWaiter);
            clockServices.put(nodeName, clockService);

            Path partitionsWorkDir = workDir.resolve("node" + i);

            LogStorageManager partitionsLogStorageManager = SharedLogStorageManagerUtils.create(
                    "test",
                    clusterService.nodeName(),
                    partitionsWorkDir.resolve("log"),
                    raftConfig.fsync().value()
            );

            logStorageFactories.put(nodeName, partitionsLogStorageManager);

            assertThat(partitionsLogStorageManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

            RaftGroupOptionsConfigurer partitionRaftConfigurer =
                    RaftGroupOptionsConfigHelper.configureProperties(partitionsLogStorageManager, partitionsWorkDir.resolve("meta"));

            raftConfigurers.put(nodeName, partitionRaftConfigurer);

            var raftSrv = TestLozaFactory.create(
                    clusterService,
                    raftConfig,
                    systemLocalConfig,
                    clock,
                    new RaftGroupEventsClientListener()
            );

            assertThat(raftSrv.startAsync(new ComponentContext()), willCompleteSuccessfully());

            raftServers.put(nodeName, raftSrv);

            var cmgManager = mock(ClusterManagementGroupManager.class);

            // This test is run without Meta storage.
            when(cmgManager.metaStorageNodes()).thenReturn(emptySetCompletedFuture());

            var commandMarshaller = new ThreadLocalPartitionCommandsMarshaller(clusterService.serializationRegistry());

            var raftClientFactory = new TopologyAwareRaftGroupServiceFactory(
                    clusterService,
                    logicalTopologyService(clusterService),
                    Loza.FACTORY,
                    new RaftGroupEventsClientListener()
            );

            schemaSyncService = new AlwaysSyncedSchemaSyncService();

            ReplicaManager replicaMgr = new ReplicaManager(
                    nodeName,
                    clusterService,
                    cmgManager,
                    groupId -> completedFuture(Assignments.EMPTY),
                    clockService,
                    Set.of(PartitionReplicationMessageGroup.class, TxMessageGroup.class),
                    placementDriver,
                    partitionOperationsExecutor,
                    this::getSafeTimePropagationTimeout,
                    new NoOpFailureManager(),
                    commandMarshaller,
                    new PartitionSafeTimeValidator(validationSchemasSource, catalogService, schemaSyncService),
                    raftClientFactory,
                    raftSrv,
                    partitionRaftConfigurer,
                    new VolatileLogStorageManagerCreator(nodeName, workDir.resolve("volatile-log-spillout")),
                    executor,
                    replicaGrpId -> nullCompletedFuture(),
                    ForkJoinPool.commonPool()
            );

            assertThat(replicaMgr.startAsync(new ComponentContext()), willCompleteSuccessfully());

            replicaManagers.put(nodeName, replicaMgr);

            LOG.info("Replica manager has been started, node=[" + node + ']');

            ReplicaService replicaSvc = spy(new ReplicaService(
                    clusterService.messagingService(),
                    clockService,
                    partitionOperationsExecutor,
                    replicationConfiguration,
                    executor
            ));

            replicaServices.put(nodeName, replicaSvc);

            var resourcesRegistry = new RemotelyTriggeredResourceRegistry();

            VolatileTxStateMetaStorage txStateVolatileStorage = VolatileTxStateMetaStorage.createStarted();

            TransactionInflights transactionInflights = new TransactionInflights(placementDriver, clockService, txStateVolatileStorage);

            txInflights.put(nodeName, transactionInflights);

            cursorRegistries.put(nodeName, resourcesRegistry);

            TxManagerImpl txMgr = newTxManager(
                    clusterService,
                    replicaSvc,
                    clockService,
                    new TransactionIdGenerator(i),
                    node,
                    placementDriver,
                    resourcesRegistry,
                    txStateVolatileStorage,
                    transactionInflights,
                    lowWatermark
            );

            ResourceVacuumManager resourceVacuumManager = new ResourceVacuumManager(
                    nodeName,
                    resourcesRegistry,
                    clusterService.topologyService(),
                    clusterService.messagingService(),
                    transactionInflights,
                    txMgr,
                    lowWatermark,
                    new NoOpFailureManager(),
                    new TestMetricManager()
            );

            assertThat(txMgr.startAsync(new ComponentContext()), willCompleteSuccessfully());
            txManagers.put(nodeName, txMgr);

            assertThat(resourceVacuumManager.startAsync(new ComponentContext()), willCompleteSuccessfully());
            resourceCleanupManagers.put(nodeName, resourceVacuumManager);

            txStateStorages.put(nodeName, new TestTxStatePartitionStorage());
        }

        LOG.info("Raft servers have been started");

        LOG.info("Partition groups have been started");

        localNodeName = extractConsistentId(cluster.get(0));

        if (startClient) {
            initializeClientTxComponents();
        } else {
            // Collocated mode.
            clientTxManager = txManagers.get(localNodeName);
            clientResourceVacuumManager = resourceCleanupManagers.get(localNodeName);
            clientTransactionInflights = txInflights.get(localNodeName);
        }

        igniteTransactions = new IgniteTransactionsImpl(clientTxManager, timestampTracker);

        assertNotNull(clientTxManager);
    }

    protected HybridClock createClock(InternalClusterNode node) {
        return new HybridClockImpl();
    }

    protected long getSafeTimePropagationTimeout() {
        return DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
    }

    protected TxManagerImpl newTxManager(
            ClusterService clusterService,
            ReplicaService replicaSvc,
            ClockService clockService,
            TransactionIdGenerator generator,
            InternalClusterNode node,
            PlacementDriver placementDriver,
            RemotelyTriggeredResourceRegistry resourcesRegistry,
            VolatileTxStateMetaStorage txStateVolatileStorage,
            TransactionInflights transactionInflights,
            LowWatermark lowWatermark
    ) {
        return new TxManagerImpl(
                node.name(),
                txConfiguration,
                systemDistributedConfig,
                clusterService.messagingService(),
                clusterService.topologyService(),
                replicaSvc,
                HeapLockManager.smallInstance(),
                txStateVolatileStorage,
                clockService,
                generator,
                placementDriver,
                () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS,
                new TestLocalRwTxCounter(),
                partitionOperationsExecutor,
                resourcesRegistry,
                transactionInflights,
                lowWatermark,
                executor,
                new NoOpFailureManager(),
                new TestMetricManager()
        );
    }

    public IgniteTransactions igniteTransactions() {
        return igniteTransactions;
    }

    /**
     * Starts a table.
     *
     * @param tableName Table name.
     * @param schemaDescriptor Schema descriptor.
     * @return Started instance.
     */
    public TableViewInternal startTable(String tableName, SchemaDescriptor schemaDescriptor) throws Exception {
        int predefinedZoneId = 2;
        int tableId = globalCatalogId.getAndIncrement();

        CatalogTableDescriptor tableDescriptor = mock(CatalogTableDescriptor.class);
        when(tableDescriptor.id()).thenReturn(tableId);
        when(tableDescriptor.latestSchemaVersion()).thenReturn(SCHEMA_VERSION);

        lenient().when(catalog.table(eq(tableId))).thenReturn(tableDescriptor);

        List<Set<Assignment>> calculatedAssignments = calculateAssignments(
                cluster.stream().map(ItTxTestCluster::extractConsistentId).collect(toList()),
                1,
                replicas,
                replicas
        );

        List<Set<String>> assignments = calculatedAssignments.stream()
                .map(a -> a.stream().map(Assignment::consistentId).collect(toSet()))
                .collect(toList());

        List<ZonePartitionId> grpIds = IntStream.range(0, assignments.size())
                .mapToObj(i -> new ZonePartitionId(predefinedZoneId, i))
                .collect(toList());

        List<CompletableFuture<?>> partitionReadyFutures = new ArrayList<>();

        int indexId = globalCatalogId.getAndIncrement();

        CatalogIndexDescriptor pkCatalogIndexDescriptor = mock(CatalogIndexDescriptor.class);
        when(pkCatalogIndexDescriptor.id()).thenReturn(indexId);

        when(catalog.indexes(eq(tableId))).thenReturn(List.of(pkCatalogIndexDescriptor));

        InternalTableImpl internalTable = new InternalTableImpl(
                QualifiedNameHelper.fromNormalized(SqlCommon.DEFAULT_SCHEMA_NAME, tableName),
                predefinedZoneId,
                tableId,
                1, // number of partitions.
                nodeResolver,
                clientTxManager,
                mock(MvTableStorage.class),
                startClient ? clientReplicaSvc : replicaServices.get(localNodeName),
                startClient ? clientClockService : clockServices.get(localNodeName),
                timestampTracker,
                placementDriver,
                clientTransactionInflights,
                null,
                mock(StreamerReceiverRunner.class),
                new TableMetricSource(QualifiedName.fromSimple(tableName))
        );

        TableImpl table = new TableImpl(
                internalTable,
                new DummySchemaManagerImpl(schemaDescriptor),
                clientTxManager.lockManager(),
                new ConstantSchemaVersions(SCHEMA_VERSION),
                mock(IgniteSql.class),
                pkCatalogIndexDescriptor.id()
        );

        tables.put(tableName, table);

        for (int p = 0; p < assignments.size(); p++) {
            Set<String> partAssignments = assignments.get(p);

            PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(partAssignments);

            ZonePartitionId grpId = grpIds.get(p);

            for (String assignment : partAssignments) {
                int partId = p;

                var mvPartStorage = new TestMvPartitionStorage(partId);
                var txStateStorage = txStateStorages.get(assignment);
                TxMessageSender txMessageSender =
                        new TxMessageSender(
                                clusterServices.get(assignment).messagingService(),
                                replicaServices.get(assignment),
                                clockServices.get(assignment)
                        );

                var transactionStateResolver = new TransactionStateResolver(
                        txManagers.get(assignment),
                        clockServices.get(assignment),
                        nodeResolver,
                        clusterServices.get(assignment).messagingService(),
                        placementDriver,
                        txMessageSender
                );
                transactionStateResolver.start();

                ColumnsExtractor row2Tuple = BinaryRowConverter.keyExtractor(schemaDescriptor);

                StorageHashIndexDescriptor pkIndexDescriptor = mock(StorageHashIndexDescriptor.class);

                when(pkIndexDescriptor.columns()).then(invocation -> Collections.nCopies(
                        schemaDescriptor.keyColumns().size(),
                        mock(StorageHashIndexColumnDescriptor.class)
                ));

                Lazy<TableSchemaAwareIndexStorage> pkStorage = new Lazy<>(() -> new TableSchemaAwareIndexStorage(
                        indexId,
                        new TestHashIndexStorage(partId, pkIndexDescriptor),
                        row2Tuple
                ));

                IndexLocker pkLocker = new HashIndexLocker(indexId, true, txManagers.get(assignment).lockManager(), row2Tuple);

                SafeTimeValuesTracker safeTime = safeTimeTrackers.computeIfAbsent(assignment, k -> new ConcurrentHashMap<>())
                        .computeIfAbsent(grpId, k -> new SafeTimeValuesTracker(HybridTimestamp.MIN_VALUE));
                PendingComparableValuesTracker<Long, Void> storageIndexTracker = new PendingComparableValuesTracker<>(0L);

                PartitionDataStorage partitionDataStorage = new TestPartitionDataStorage(tableId, partId, mvPartStorage);

                IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(
                        DummyInternalTableImpl.createTableIndexStoragesSupplier(Map.of(pkStorage.get().id(), pkStorage.get()))
                );

                StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(
                        partId,
                        partitionDataStorage,
                        indexUpdateHandler,
                        replicationConfiguration,
                        TableTestUtils.NOOP_PARTITION_MODIFICATION_COUNTER
                );

                var schemaManager = new DummySchemaManagerImpl(schemaDescriptor);
                var tableValidationSchemasSource = new DummyValidationSchemasSource(schemaManager);

                validationSchemasSource.registerSource(tableId, tableValidationSchemasSource);

                RaftGroupListener raftGroupListener = getOrCreateAndPopulateRaftGroupListener(
                        assignment,
                        predefinedZoneId,
                        partId,
                        tableId,
                        partitionDataStorage,
                        storageUpdateHandler,
                        txStateStorage,
                        safeTime,
                        storageIndexTracker,
                        catalogService,
                        schemaManager
                );

                Function<RaftGroupService, ReplicaListener> replicaListenerProvider =
                        raftClient -> getOrCreateAndPopulateReplicaListenerProvider(
                                assignment,
                                mvPartStorage,
                                raftClient,
                                txManagers.get(assignment),
                                new ZonePartitionId(predefinedZoneId, partId),
                                tableId,
                                () -> Map.of(pkLocker.id(), pkLocker),
                                pkStorage,
                                Map::of,
                                clockServices.get(assignment),
                                safeTime,
                                txStateStorage,
                                transactionStateResolver,
                                storageUpdateHandler,
                                validationSchemasSource,
                                nodeResolver.getByConsistentId(assignment),
                                schemaSyncService,
                                catalogService,
                                placementDriver,
                                nodeResolver,
                                cursorRegistries.get(assignment),
                                schemaManager
                        );

                CompletableFuture<?> partitionReadyFuture = startReplica(
                        assignment,
                        raftGroupListener,
                        replicaListenerProvider,
                        storageIndexTracker,
                        grpId,
                        configuration
                );

                partitionReadyFutures.add(partitionReadyFuture);
            }
        }

        allOf(partitionReadyFutures.toArray(new CompletableFuture[0])).join();

        for (int p = 0; p < assignments.size(); p++) {
            ZonePartitionId grpId = grpIds.get(p);
            CompletableFuture<ReplicaMeta> primaryFuture = placementDriver.getPrimaryReplica(grpId,
                    clockServices.values().iterator().next().now());

            // TestPlacementDriver always returns completed futures.
            assert primaryFuture.isDone();

            ReplicaMeta primary = primaryFuture.join();

            assert primary.getLeaseholderId() != null;

            PrimaryReplicaChangeCommand cmd = REPLICA_MESSAGES_FACTORY.primaryReplicaChangeCommand()
                    .leaseStartTime(primary.getStartTime().longValue())
                    .primaryReplicaNodeId(primary.getLeaseholderId())
                    .primaryReplicaNodeName(primary.getLeaseholder())
                    .build();

            CompletableFuture<RaftGroupService> raftClientFuture = getRaftClientForGroup(grpId);

            assertThat(raftClientFuture, willCompleteSuccessfully());

            CompletableFuture<?> primaryReplicaChangePropagationFuture = raftClientFuture.join().run(cmd);

            partitionReadyFutures.add(primaryReplicaChangePropagationFuture);
        }

        allOf(partitionReadyFutures.toArray(new CompletableFuture[0])).join();

        return table;
    }

    private static String extractConsistentId(ClusterService nodeService) {
        return nodeService.topologyService().localMember().name();
    }

    /**
     * Creates zone raft listener for specific zone partition if it wasn't created previously and
     * populates it with table raft processor.
     */
    // TODO https://issues.apache.org/jira/browse/IGNITE-24798 Simplify after switching main towards colocation.
    private RaftGroupListener getOrCreateAndPopulateRaftGroupListener(
            String assignment,
            int zoneId,
            int partId,
            int tableId,
            PartitionDataStorage partitionDataStorage,
            StorageUpdateHandler storageUpdateHandler,
            TxStatePartitionStorage txStateStorage,
            SafeTimeValuesTracker safeTimeTracker,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker,
            CatalogService catalogService,
            SchemaRegistry schemaRegistry
    ) {
        ZonePartitionId zonePartitionId = new ZonePartitionId(zoneId, partId);

        var nodeSpecificZonePartitionRaftGroupListeners = zonePartitionRaftGroupListeners.computeIfAbsent(assignment,
                k -> new HashMap<>());

        ZonePartitionRaftListener zonePartitionRaftListener = nodeSpecificZonePartitionRaftGroupListeners.computeIfAbsent(
                zonePartitionId,
                k -> new ZonePartitionRaftListener(
                        zonePartitionId,
                        txStateStorage,
                        txManagers.get(assignment),
                        safeTimeTracker,
                        storageIndexTracker,
                        mock(PartitionsSnapshots.class, RETURNS_DEEP_STUBS),
                        partitionOperationsExecutor
                )
        );

        TablePartitionProcessor tablePartitionRaftListener = new TablePartitionProcessor(
                txManagers.get(assignment),
                partitionDataStorage,
                storageUpdateHandler,
                catalogService,
                schemaRegistry,
                mock(IndexMetaStorage.class),
                clusterServices.get(assignment).topologyService().getByConsistentId(assignment).id(),
                mock(MinimumRequiredTimeCollectorService.class),
                placementDriver,
                clockServices.get(assignment),
                zonePartitionId
        );

        zonePartitionRaftListener.addTableProcessor(tableId, tablePartitionRaftListener);

        return zonePartitionRaftListener;
    }

    /**
     * Creates zone listener for specific zone partition if it wasn't created previously and
     * populate it with table processor.
     */
    // TODO https://issues.apache.org/jira/browse/IGNITE-24798 Simplify after switching main towards colocation.
    private ReplicaListener getOrCreateAndPopulateReplicaListenerProvider(
            String assignment,
            MvPartitionStorage mvDataStorage,
            RaftGroupService raftClient,
            TxManager txManager,
            ZonePartitionId zonePartitionId,
            int tableId,
            Supplier<Map<Integer, IndexLocker>> indexesLockers,
            Lazy<TableSchemaAwareIndexStorage> pkIndexStorage,
            Supplier<Map<Integer, TableSchemaAwareIndexStorage>> secondaryIndexStorages,
            ClockService clockService,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTime,
            TxStatePartitionStorage txStatePartitionStorage,
            TransactionStateResolver transactionStateResolver,
            StorageUpdateHandler storageUpdateHandler,
            ValidationSchemasSource validationSchemasSource,
            InternalClusterNode localNode,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            PlacementDriver placementDriver,
            ClusterNodeResolver clusterNodeResolver,
            RemotelyTriggeredResourceRegistry resourcesRegistry,
            SchemaRegistry schemaRegistry
    ) {
        Map<ZonePartitionId, ZonePartitionReplicaListener> nodeSpecificZonePartitionReplicaListeners =
                zonePartitionReplicaListeners.computeIfAbsent(assignment, k -> new HashMap<>());

        TxMessageSender txMessageSender = new TxMessageSender(
                clusterServices.get(assignment).messagingService(),
                replicaServices.get(assignment),
                clockService
        );

        ZonePartitionReplicaListener zonePartitionReplicaListener = nodeSpecificZonePartitionReplicaListeners.computeIfAbsent(
                zonePartitionId,
                partitionId -> new ZonePartitionReplicaListener(
                        txStatePartitionStorage,
                        clockService,
                        txManager,
                        validationSchemasSource,
                        schemaSyncService,
                        catalogService,
                        placementDriver,
                        clusterNodeResolver,
                        raftClient,
                        new NoOpFailureManager(),
                        localNode,
                        partitionId,
                        transactionStateResolver,
                        txMessageSender
                )
        );

        TablePartitionReplicaProcessorFactory createReplicaListener = (raftGroupService, txStateResolver) -> newReplicaListener(
                mvDataStorage,
                (RaftGroupService) raftGroupService,
                txManager,
                Runnable::run,
                zonePartitionId,
                tableId,
                indexesLockers,
                pkIndexStorage,
                secondaryIndexStorages,
                clockService,
                safeTime,
                transactionStateResolver,
                storageUpdateHandler,
                validationSchemasSource,
                localNode,
                schemaSyncService,
                catalogService,
                placementDriver,
                clusterNodeResolver,
                resourcesRegistry,
                schemaRegistry
        );

        zonePartitionReplicaListener.addTableReplicaProcessor(tableId, createReplicaListener);

        return zonePartitionReplicaListener;
    }

    private CompletableFuture<Replica> startReplica(
            String assignment,
            RaftGroupListener raftGroupListener,
            Function<RaftGroupService, ReplicaListener> replicaListenerProvider,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker,
            ZonePartitionId replicaGrpId,
            PeersAndLearners newConfiguration
    ) throws NodeStoppingException {
        CompletableFuture<Replica> replicaPendingFuture = replicaManagers.get(assignment).replica(replicaGrpId);
        if (replicaPendingFuture != null) {
            return replicaPendingFuture.thenApply(replica -> {
                replicaListenerProvider.apply(replica.raftClient());
                return replica;
            });
        } else {
            return replicaManagers.get(assignment).startReplica(
                    RaftGroupEventsListener.noopLsnr,
                    raftGroupListener,
                    false,
                    null,
                    replicaListenerProvider,
                    storageIndexTracker,
                    replicaGrpId,
                    newConfiguration
            );
        }
    }

    protected PartitionReplicaListener newReplicaListener(
            MvPartitionStorage mvDataStorage,
            RaftGroupService raftClient,
            TxManager txManager,
            Executor scanRequestExecutor,
            ZonePartitionId replicationGroupId,
            int tableId,
            Supplier<Map<Integer, IndexLocker>> indexesLockers,
            Lazy<TableSchemaAwareIndexStorage> pkIndexStorage,
            Supplier<Map<Integer, TableSchemaAwareIndexStorage>> secondaryIndexStorages,
            ClockService clockService,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTime,
            TransactionStateResolver transactionStateResolver,
            StorageUpdateHandler storageUpdateHandler,
            ValidationSchemasSource validationSchemasSource,
            InternalClusterNode localNode,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            PlacementDriver placementDriver,
            ClusterNodeResolver clusterNodeResolver,
            RemotelyTriggeredResourceRegistry resourcesRegistry,
            SchemaRegistry schemaRegistry
    ) {
        return new PartitionReplicaListener(
                mvDataStorage,
                raftClient,
                txManager,
                txManager.lockManager(),
                Runnable::run,
                replicationGroupId,
                tableId,
                indexesLockers,
                pkIndexStorage,
                secondaryIndexStorages,
                clockService,
                safeTime,
                transactionStateResolver,
                storageUpdateHandler,
                validationSchemasSource,
                localNode,
                schemaSyncService,
                catalogService,
                placementDriver,
                clusterNodeResolver,
                resourcesRegistry,
                schemaRegistry,
                mock(IndexMetaStorage.class),
                lowWatermark,
                new NoOpFailureManager(),
                new TableMetricSource(QualifiedName.fromSimple("test_table"))
        );
    }

    private static LogicalTopologyService logicalTopologyService(ClusterService clusterService) {
        return new LogicalTopologyService() {
            private final UUID clusterId = UUID.randomUUID();

            @Override
            public void addEventListener(LogicalTopologyEventListener listener) {

            }

            @Override
            public void removeEventListener(LogicalTopologyEventListener listener) {

            }

            @Override
            public CompletableFuture<LogicalTopologySnapshot> logicalTopologyOnLeader() {
                return completedFuture(new LogicalTopologySnapshot(
                        1,
                        clusterService.topologyService().allMembers().stream().map(LogicalNode::new).collect(toSet()),
                        clusterId
                ));
            }

            @Override
            public LogicalTopologySnapshot localLogicalTopology() {
                return new LogicalTopologySnapshot(
                        1,
                        clusterService.topologyService().allMembers().stream().map(LogicalNode::new).collect(toSet()),
                        clusterId
                );
            }

            @Override
            public CompletableFuture<Set<InternalClusterNode>> validatedNodesOnLeader() {
                return completedFuture(Set.copyOf(clusterService.topologyService().allMembers()));
            }
        };
    }

    private CompletableFuture<RaftGroupService> getRaftClientForGroup(ZonePartitionId groupId) {
        int partId = 0;

        return replicaManagers.get(extractConsistentId(cluster.get(partId)))
                .replica(groupId)
                .thenApply(Replica::raftClient);
    }

    protected Peer getLeaderId(QualifiedName tableName) {
        int partId = 0;
        TableImpl tableImpl = tables.get(tableName.objectName());

        return replicaManagers.get(extractConsistentId(cluster.get(partId)))
                .replica(new ZonePartitionId(tableImpl.internalTable().zoneId(), partId))
                .thenApply(replica -> replica.raftClient().leader())
                .join();
    }

    /**
     * Shutdowns all cluster nodes after each test.
     */
    public void shutdownCluster() {
        assertThat(stopAsync(new ComponentContext(), cluster), willCompleteSuccessfully());
        assertThat(stopAsync(new ComponentContext(), client), willCompleteSuccessfully());

        for (Entry<String, Loza> entry : raftServers.entrySet()) {
            Loza rs = entry.getValue();

            ReplicaManager replicaMgr = replicaManagers.get(entry.getKey());

            CompletableFuture<?>[] replicaStopFutures = replicaMgr.startedGroups().stream()
                    .map(grp -> {
                        try {
                            return replicaMgr.stopReplica(grp);
                        } catch (NodeStoppingException e) {
                            throw new AssertionError(e);
                        }
                    })
                    .toArray(CompletableFuture[]::new);

            assertThat(allOf(replicaStopFutures), willCompleteSuccessfully());

            rs.localNodes().parallelStream().forEach(nodeId -> {
                try {
                    rs.stopRaftNode(nodeId);
                } catch (NodeStoppingException e) {
                    throw new AssertionError(e);
                }
            });

            assertThat(replicaMgr.stopAsync(new ComponentContext()), willCompleteSuccessfully());
            assertThat(rs.stopAsync(new ComponentContext()), willCompleteSuccessfully());
        }

        if (logStorageFactories != null) {
            for (LogStorageManager logStorageManager : logStorageFactories.values()) {
                assertThat(logStorageManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());
            }
        }

        if (resourceCleanupManagers != null) {
            for (ResourceVacuumManager resourceVacuumManager : resourceCleanupManagers.values()) {
                assertThat(resourceVacuumManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());
            }
        }

        if (clientResourceVacuumManager != null) {
            assertThat(clientResourceVacuumManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());
        }

        if (txManagers != null) {
            for (TxManager txMgr : txManagers.values()) {
                assertThat(txMgr.stopAsync(new ComponentContext()), willCompleteSuccessfully());
            }
        }

        if (clientTxManager != null) {
            assertThat(clientTxManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());
        }

        if (clientClockWaiter != null) {
            assertThat(clientClockWaiter.stopAsync(new ComponentContext()), willCompleteSuccessfully());
        }

        if (clockWaiters != null) {
            for (ClockWaiter clockWaiter : clockWaiters) {
                assertThat(clockWaiter.stopAsync(new ComponentContext()), willCompleteSuccessfully());
            }
        }

        if (executor != null) {
            IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
        }

        if (partitionOperationsExecutor != null) {
            IgniteUtils.shutdownAndAwaitTermination(partitionOperationsExecutor, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Starts a node.
     *
     * @param name Node name.
     * @param port Local port.
     * @param nodeFinder Node finder.
     * @return The client cluster view.
     */
    protected static ClusterService startNode(TestInfo testInfo, String name, int port,
            NodeFinder nodeFinder) {
        var network = ClusterServiceTestUtils.clusterService(testInfo, port, nodeFinder);

        assertThat(network.startAsync(new ComponentContext()), willCompleteSuccessfully());

        return network;
    }

    private void startClient() throws InterruptedException {
        client = startNode(testInfo, "client", NODE_PORT_BASE - 1, nodeFinder);

        assertTrue(waitForTopology(client, nodes + 1, 1000));

        clientClock = createClock(client.topologyService().localMember());
        clientClockWaiter = new ClockWaiter("client-node", clientClock, executor);
        assertThat(clientClockWaiter.startAsync(new ComponentContext()), willCompleteSuccessfully());
        clientClockService = new TestClockService(clientClock, clientClockWaiter);

        LOG.info("Replica manager has been started, node=[" + client.topologyService().localMember() + ']');

        clientReplicaSvc = spy(new ReplicaService(
                client.messagingService(),
                clientClockService,
                partitionOperationsExecutor,
                replicationConfiguration,
                executor
        ));

        LOG.info("The client has been started");
    }

    private void initializeClientTxComponents() {
        RemotelyTriggeredResourceRegistry resourceRegistry = new RemotelyTriggeredResourceRegistry();

        VolatileTxStateMetaStorage txStateVolatileStorage = VolatileTxStateMetaStorage.createStarted();

        clientTransactionInflights = new TransactionInflights(placementDriver, clientClockService, txStateVolatileStorage);

        clientTxManager = new TxManagerImpl(
                "client",
                txConfiguration,
                systemDistributedConfig,
                client.messagingService(),
                client.topologyService(),
                clientReplicaSvc,
                HeapLockManager.smallInstance(),
                txStateVolatileStorage,
                clientClockService,
                new TransactionIdGenerator(-1),
                placementDriver,
                () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS,
                new TestLocalRwTxCounter(),
                partitionOperationsExecutor,
                resourceRegistry,
                clientTransactionInflights,
                lowWatermark,
                executor,
                new NoOpFailureManager(),
                new TestMetricManager()
        );

        clientResourceVacuumManager = new ResourceVacuumManager(
                "client",
                resourceRegistry,
                client.topologyService(),
                client.messagingService(),
                clientTransactionInflights,
                clientTxManager,
                lowWatermark,
                new NoOpFailureManager(),
                new TestMetricManager()
        );

        clientTxStateResolver = new TransactionStateResolver(
                clientTxManager,
                clientClockService,
                nodeResolver,
                client.messagingService(),
                placementDriver,
                new TxMessageSender(
                        client.messagingService(),
                        clientReplicaSvc,
                        clientClockService
                )
        );

        clientTxStateResolver.start();
        assertThat(startAsync(new ComponentContext(), clientTxManager, clientResourceVacuumManager), willCompleteSuccessfully());
    }

    public Map<String, Loza> raftServers() {
        return raftServers;
    }

    public Map<String, TxManager> txManagers() {
        return txManagers;
    }

    public PlacementDriver placementDriver() {
        return placementDriver;
    }

    public TxManager clientTxManager() {
        return clientTxManager;
    }

    public String localNodeName() {
        return localNodeName;
    }

    public Map<String, HybridClock> clocks() {
        return clocks;
    }

    public HybridClock clientClock() {
        return clientClock;
    }

    public Map<String, ReplicaManager> replicaManagers() {
        return replicaManagers;
    }

    public Map<String, ClusterService> clusterServices() {
        return clusterServices;
    }

    public List<TxStateMeta> states() {
        return new ArrayList<>(((TxManagerImpl) clientTxManager).states());
    }
}
