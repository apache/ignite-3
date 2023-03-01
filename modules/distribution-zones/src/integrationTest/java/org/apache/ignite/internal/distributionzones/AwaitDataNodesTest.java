package org.apache.ignite.internal.distributionzones;

import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageServiceImpl;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.raft.jraft.util.concurrent.DefaultFixedThreadsExecutorGroup;
import org.apache.ignite.raft.jraft.util.concurrent.SingleThreadExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateLogicalTopologyAndVersion;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.flow.TestFlowUtils.subscribeToList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBeCancelledFast;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.apache.ignite.utils.ClusterServiceTestUtils.clusterService;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(ConfigurationExtension.class)
public class AwaitDataNodesTest extends IgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(AwaitDataNodesTest.class);

    private VaultManager vaultManager;

    private ClusterService clusterService;

    private MetaStorageManager metaStorageManager;

    private DistributionZoneManager distributionZoneManager;

    private LogicalTopology logicalTopology;

    private ClusterStateStorage clusterStateStorage;

    private DistributionZonesConfiguration zonesConfiguration;

    private ConfigurationManager clusterCfgMgr;

    private ClusterManagementGroupManager cmgManager;

    private WatchListener topologyWatchListener;

    private WatchListener dataNodesWatchListener;

    @BeforeEach
    void setUp(TestInfo testInfo, @InjectConfiguration RaftConfiguration raftConfiguration) throws NodeStoppingException {
        var addr = new NetworkAddress("localhost", 10_000);

        clusterService = clusterService(testInfo, addr.port(), new StaticNodeFinder(List.of(addr)));

        vaultManager = new VaultManager(new InMemoryVaultService());

        cmgManager = mock(ClusterManagementGroupManager.class);

        when(cmgManager.metaStorageNodes())
                .thenReturn(completedFuture(Set.of(clusterService.localConfiguration().getName())));

        metaStorageManager = mock(MetaStorageManager.class);

        MetaStorageWrapper metaStorageWrapper = new MetaStorageWrapper(metaStorageManager);

        doAnswer(invocation -> {
            ByteArray key = invocation.getArgument(0);

            WatchListener watchListener = invocation.getArgument(1);

            if (Arrays.equals(key.bytes(), zonesLogicalTopologyVersionKey().bytes())) {
                topologyWatchListener = watchListener;
            } else if (Arrays.equals(key.bytes(), zoneDataNodesPrefix().bytes())) {
                dataNodesWatchListener = watchListener;
            }

            return null;
        }).when(metaStorageManager).registerExactWatch(any(), any());

        vaultManager.start();
        clusterService.start();
//        metaStorageManager.start();

//        metaStorageManager.deployWatches();

        //+++++

        clusterStateStorage = new TestClusterStateStorage();

        logicalTopology = new LogicalTopologyImpl(clusterStateStorage);

        LogicalTopologyServiceImpl logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgManager);

        TablesConfiguration tablesConfiguration = mock(TablesConfiguration.class);

        clusterCfgMgr = new ConfigurationManager(
                List.of(DistributionZonesConfiguration.KEY),
                Set.of(),
                new TestConfigurationStorage(DISTRIBUTED),
                List.of(),
                List.of()
        );

        clusterCfgMgr.start();

        zonesConfiguration = clusterCfgMgr.configurationRegistry()
                .getConfiguration(DistributionZonesConfiguration.KEY);

        distributionZoneManager = new DistributionZoneManager(
                zonesConfiguration,
                tablesConfiguration,
                metaStorageManager,
                logicalTopologyService,
                vaultManager,
                "node"
        );

//        mockVaultZonesLogicalTopologyKey(Set.of());

        mockCmgLocalNodes();

        distributionZoneManager.start();

//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }

        System.out.println("setUp_finish");
    }

    @AfterEach
    void tearDown() throws Exception {
        List<IgniteComponent> components = List.of(metaStorageManager, clusterService, vaultManager);

        IgniteUtils.closeAll(Stream.concat(
                components.stream().map(c -> c::beforeNodeStop),
                components.stream().map(c -> c::stop)
        ));
    }

    @Test
    void testSeveralScaleUpAndSeveralScaleDown1() throws ExecutionException, InterruptedException {
        System.out.println("test1_started");

        TestSeveralScaleUpAndSeveralScaleDownGeneralObject testData = testSeveralScaleUpAndSeveralScaleDownGeneral();

        Set<String> dataNodes = Set.of("node0", "node2");

        topologyWatchListenerOnUpdate(dataNodes, testData.topVer2, testData.dataNodesRevision2);

        assertTrue(testData.topVerUpFut2.isDone());
        assertTrue(testData.topVerDownFut2.isDone());

        assertTrue(waitForCondition(() -> {
                    DistributionZoneManager.DataNodes dataNodesMeta0 = distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID);

                    if (dataNodesMeta0 == null) {
                        return false;
                    }

                    System.out.println("dataNodes.getRevisionScaleUpFutures().size(): " + dataNodesMeta0.getRevisionScaleUpFutures().size());

                    return dataNodesMeta0.getRevisionScaleUpFutures().size() == 1;
                },
                3_000));

        assertTrue(testData.dataNodesMeta.getRevisionScaleDownFutures().isEmpty());

        CompletableFuture<Void> revisionUpFut = testData.dataNodesMeta.getRevisionScaleUpFutures().get((long) testData.dataNodesRevision2);

        assertFalse(revisionUpFut.isDone());

        dataNodesWatchListenerOnUpdate(dataNodes, true, testData.dataNodesRevision2, testData.dataNodesRevision2 + 1);

        assertTrue(waitForCondition(() -> revisionUpFut.isDone(), 3000));

        assertTrue(waitForCondition(() -> {
                    DistributionZoneManager.DataNodes dataNodesMeta0 = distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID);

                    if (dataNodesMeta0 == null) {
                        return false;
                    }

                    System.out.println("dataNodes.getRevisionScaleUpFutures().size(): " + dataNodesMeta0.getRevisionScaleDownFutures().size());

                    return dataNodesMeta0.getRevisionScaleDownFutures().size() == 1;
                },
                3_000));

        CompletableFuture<Void> revisionDownFut = testData.dataNodesMeta.getRevisionScaleDownFutures().get((long) testData.dataNodesRevision2);

        assertFalse(revisionDownFut.isDone());

        assertFalse(testData.dataNodesUpFut3.isDone());
        assertFalse(testData.dataNodesDownFut3.isDone());

        dataNodesWatchListenerOnUpdate(dataNodes, false, testData.dataNodesRevision2, testData.dataNodesRevision2 + 2);

        assertTrue(waitForCondition(() -> revisionDownFut.isDone(), 3000));

        assertTrue(waitForCondition(() -> testData.dataNodesUpFut3.isDone(), 3000));
        assertTrue(waitForCondition(() -> testData.dataNodesDownFut3.isDone(), 3000));

        assertTrue(distributionZoneManager.topVerFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleUpFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleDownFutures().isEmpty());
    }

    @Test
    void testSeveralScaleUpAndSeveralScaleDown2() throws ExecutionException, InterruptedException {
        System.out.println("test1_started");

        TestSeveralScaleUpAndSeveralScaleDownGeneralObject testData = testSeveralScaleUpAndSeveralScaleDownGeneral();

        Set<String> dataNodes = Set.of("node0", "node1", "node2");

        topologyWatchListenerOnUpdate(dataNodes, testData.topVer2, testData.dataNodesRevision2);

        assertTrue(testData.topVerUpFut2.isDone());
        assertTrue(testData.topVerDownFut2.isDone());

        assertTrue(waitForCondition(() -> {
                    DistributionZoneManager.DataNodes dataNodesMeta0 = distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID);

                    if (dataNodesMeta0 == null) {
                        return false;
                    }

                    System.out.println("dataNodes.getRevisionScaleUpFutures().size(): " + dataNodesMeta0.getRevisionScaleUpFutures().size());

                    return dataNodesMeta0.getRevisionScaleUpFutures().size() == 1;
                },
                3_000));

        assertTrue(testData.dataNodesMeta.getRevisionScaleDownFutures().isEmpty());

        CompletableFuture<Void> revisionUpFut = testData.dataNodesMeta.getRevisionScaleUpFutures().get((long) testData.dataNodesRevision2);

        assertFalse(revisionUpFut.isDone());

        dataNodesWatchListenerOnUpdate(dataNodes, true, testData.dataNodesRevision2, testData.dataNodesRevision2 + 1);

        assertTrue(waitForCondition(() -> revisionUpFut.isDone(), 3000));
        assertTrue(waitForCondition(() -> testData.dataNodesUpFut3.isDone(), 3000));
        assertTrue(waitForCondition(() -> testData.dataNodesDownFut3.isDone(), 3000));

        assertTrue(distributionZoneManager.topVerFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleUpFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleDownFutures().isEmpty());
    }

    @Test
    void testSeveralScaleUpAndSeveralScaleDown3() throws ExecutionException, InterruptedException {
        System.out.println("test1_started");

        TestSeveralScaleUpAndSeveralScaleDownGeneralObject testData = testSeveralScaleUpAndSeveralScaleDownGeneral();

        Set<String> dataNodes = Set.of("node0");

        topologyWatchListenerOnUpdate(dataNodes, testData.topVer2, testData.dataNodesRevision2);

        assertTrue(testData.topVerUpFut2.isDone());
        assertTrue(testData.topVerDownFut2.isDone());

        assertTrue(waitForCondition(() -> {
                    DistributionZoneManager.DataNodes dataNodesMeta0 = distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID);

                    if (dataNodesMeta0 == null) {
                        return false;
                    }

                    System.out.println("dataNodes.getRevisionScaleUpFutures().size(): " + dataNodesMeta0.getRevisionScaleDownFutures().size());

                    return dataNodesMeta0.getRevisionScaleDownFutures().size() == 1;
                },
                3_000));

        assertTrue(testData.dataNodesMeta.getRevisionScaleUpFutures().isEmpty());

        CompletableFuture<Void> revisionDownFut = testData.dataNodesMeta.getRevisionScaleDownFutures().get((long) testData.dataNodesRevision2);

        assertFalse(revisionDownFut.isDone());

        dataNodesWatchListenerOnUpdate(dataNodes, false, testData.dataNodesRevision2, testData.dataNodesRevision2 + 1);

        assertTrue(waitForCondition(() -> revisionDownFut.isDone(), 3000));
        assertTrue(waitForCondition(() -> testData.dataNodesUpFut3.isDone(), 3000));
        assertTrue(waitForCondition(() -> testData.dataNodesDownFut3.isDone(), 3000));
    }

    private static class TestSeveralScaleUpAndSeveralScaleDownGeneralObject {
        private long topVer2;
        private long dataNodesRevision2;
        private CompletableFuture<Void> topVerUpFut2;
        private CompletableFuture<Void> topVerDownFut2;
        private DistributionZoneManager.DataNodes dataNodesMeta;
        private CompletableFuture<Set<String>> dataNodesUpFut3;
        private CompletableFuture<Set<String>> dataNodesDownFut3;

        public TestSeveralScaleUpAndSeveralScaleDownGeneralObject(long topVer2, long dataNodesRevision2, CompletableFuture<Void> topVerUpFut2, CompletableFuture<Void> topVerDownFut2, DistributionZoneManager.DataNodes dataNodesMeta, CompletableFuture<Set<String>> dataNodesUpFut3, CompletableFuture<Set<String>> dataNodesDownFut3) {
            this.topVer2 = topVer2;
            this.dataNodesRevision2 = dataNodesRevision2;
            this.topVerUpFut2 = topVerUpFut2;
            this.topVerDownFut2 = topVerDownFut2;
            this.dataNodesMeta = dataNodesMeta;
            this.dataNodesUpFut3 = dataNodesUpFut3;
            this.dataNodesDownFut3 = dataNodesDownFut3;
        }
    }

    private TestSeveralScaleUpAndSeveralScaleDownGeneralObject testSeveralScaleUpAndSeveralScaleDownGeneral() throws ExecutionException, InterruptedException {
        System.out.println("Added_nodes");

        CompletableFuture<Set<String>> dataNodesUpFut0 = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 1);
        CompletableFuture<Set<String>> dataNodesUpFut1 = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 1);
        CompletableFuture<Set<String>> dataNodesUpFut2 = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 2);
        CompletableFuture<Set<String>> dataNodesUpFut3 = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 11);

        assertTrue(waitForCondition(() -> distributionZoneManager.topVerFutures().size() == 3,
                3_000));

        int topVer0 = 2;

        int dataNodesRevision0 = 2;

        CompletableFuture<Void> topVerUpFut0 = distributionZoneManager.topVerFutures().get(1L);
        CompletableFuture<Void> topVerUpFut1 = distributionZoneManager.topVerFutures().get(2L);
        CompletableFuture<Void> topVerUpFut2 = distributionZoneManager.topVerFutures().get(11L);

        assertFalse(topVerUpFut0.isDone());
        assertFalse(topVerUpFut1.isDone());
        assertFalse(topVerUpFut2.isDone());

        Set<String> threeNodes = Set.of("node0", "node1", "node2");

        topologyWatchListenerOnUpdate(threeNodes, topVer0, dataNodesRevision0);

        assertTrue(waitForCondition(() -> topVerUpFut0.isDone(), 3_000));
        assertTrue(waitForCondition(() -> topVerUpFut1.isDone(), 3_000));
        assertFalse(waitForCondition(() -> topVerUpFut2.isDone(), 3_000));

        assertTrue(waitForCondition(() -> {
                    DistributionZoneManager.DataNodes dataNodesMeta0 = distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID);

                    if (dataNodesMeta0 == null) {
                        return false;
                    }

                    System.out.println("dataNodes.getRevisionScaleUpFutures().size(): " + dataNodesMeta0.getRevisionScaleUpFutures().size());

                    return dataNodesMeta0.getRevisionScaleUpFutures().size() == 1;
                },
                3_000));

        DistributionZoneManager.DataNodes dataNodesMeta = distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID);

        CompletableFuture<Void> revision2Fut = dataNodesMeta.getRevisionScaleUpFutures().get((long) dataNodesRevision0);

        assertFalse(revision2Fut.isDone());

        dataNodesWatchListenerOnUpdate(threeNodes, true, dataNodesRevision0, dataNodesRevision0 + 1);

        assertTrue(waitForCondition(() -> revision2Fut.isDone(),
                3_000));

        assertEquals(threeNodes, dataNodesUpFut0.get());
        assertEquals(threeNodes, dataNodesUpFut1.get());
        assertEquals(threeNodes, dataNodesUpFut2.get());
        assertFalse(dataNodesUpFut3.isDone());

        assertTrue(distributionZoneManager.topVerFutures().size() == 1);
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleUpFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleDownFutures().isEmpty());


        System.out.println("Removed_nodes");

        CompletableFuture<Set<String>> dataNodesDownFut0 = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 4);
        CompletableFuture<Set<String>> dataNodesDownFut1 = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 4);
        CompletableFuture<Set<String>> dataNodesDownFut2 = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 5);
        CompletableFuture<Set<String>> dataNodesDownFut3 = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 6);

        assertTrue(waitForCondition(() -> distributionZoneManager.topVerFutures().size() == 4,
                3_000));

        CompletableFuture<Void> topVerDownFut0 = distributionZoneManager.topVerFutures().get(4L);
        CompletableFuture<Void> topVerDownFut1 = distributionZoneManager.topVerFutures().get(5L);
        CompletableFuture<Void> topVerDownFut2 = distributionZoneManager.topVerFutures().get(6L);

        assertFalse(topVerDownFut0.isDone());
        assertFalse(topVerDownFut1.isDone());
        assertFalse(topVerDownFut2.isDone());

        int topVer1 = 5;

        int dataNodesRevision1 = dataNodesRevision0 + 2;

        Set<String> twoNodes = Set.of("node0", "node1");

        topologyWatchListenerOnUpdate(twoNodes, topVer1, dataNodesRevision1);

        assertTrue(waitForCondition(() -> topVerDownFut0.isDone(), 3_000));
        assertTrue(waitForCondition(() -> topVerDownFut1.isDone(), 3_000));
        assertFalse(waitForCondition(() -> topVerDownFut2.isDone(), 3_000));

        assertTrue(waitForCondition(() -> {
                    DistributionZoneManager.DataNodes dataNodesMeta0 = distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID);

                    if (dataNodesMeta0 == null) {
                        return false;
                    }

                    System.out.println("dataNodes.getRevisionScaleDownFutures().size(): " + dataNodesMeta0.getRevisionScaleDownFutures().size());

                    return dataNodesMeta0.getRevisionScaleDownFutures().size() == 1;
                },
                3_000));

//        DistributionZoneManager.DataNodes dataNodesMeta = distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID);

        CompletableFuture<Void> revision5Fut = dataNodesMeta.getRevisionScaleDownFutures().get((long) dataNodesRevision1);

        assertFalse(revision5Fut.isDone());

        dataNodesWatchListenerOnUpdate(twoNodes, false, dataNodesRevision1, dataNodesRevision1 + 1);

        assertTrue(waitForCondition(() -> revision5Fut.isDone(), 3_000));

        assertEquals(twoNodes, dataNodesDownFut0.get());
        assertEquals(twoNodes, dataNodesDownFut1.get());
        assertEquals(twoNodes, dataNodesDownFut2.get());
        assertFalse(dataNodesDownFut3.isDone());

        assertEquals(2, distributionZoneManager.topVerFutures().size());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleDownFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleDownFutures().isEmpty());


        System.out.println("Added_and_removed_nodes");

        int topVer2 = 20;

        int dataNodesRevision2 = dataNodesRevision1 + 2;

        return new TestSeveralScaleUpAndSeveralScaleDownGeneralObject(topVer2, dataNodesRevision2, topVerUpFut2, topVerDownFut2, dataNodesMeta, dataNodesUpFut3, dataNodesDownFut3);
    }

    @Test
    void testScaleUpAndThenScaleDown() throws ExecutionException, InterruptedException {
        System.out.println("test1_started");

        System.out.println("Added_nodes");

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 5);

        AtomicReference<CompletableFuture<Void>> topVerFut = new AtomicReference<>();

        assertTrue(waitForCondition(() -> {
                    topVerFut.set(distributionZoneManager.topVerFutures().get(5L));

                    return topVerFut.get() != null;
                },
                3_000));

        assertFalse(topVerFut.get().isDone());

        topologyWatchListenerOnUpdate(Set.of("node0", "node1"), 100, 10);

        assertTrue(waitForCondition(() -> topVerFut.get().isDone(),
                3_000));

        AtomicReference<CompletableFuture<Void>> revisionFut = new AtomicReference<>();

        assertTrue(waitForCondition(() -> {
                    DistributionZoneManager.DataNodes dataNodes = distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID);

                    if (dataNodes == null) {
                        return false;
                    }

                    revisionFut.set(dataNodes.getRevisionScaleUpFutures().get(10L));

                    return revisionFut.get() != null;
                },
                3_000));

        assertFalse(revisionFut.get().isDone());

        dataNodesWatchListenerOnUpdate(Set.of("node0", "node1"), true, 10, 11);

        assertTrue(waitForCondition(() -> revisionFut.get().isDone(),
                3_000));

        assertEquals(Set.of("node0", "node1"), dataNodesFut.get());

        assertTrue(distributionZoneManager.topVerFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleUpFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleDownFutures().isEmpty());

        System.out.println("Removed_nodes");

        dataNodesFut = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 106);

        topVerFut.set(null);

        assertTrue(waitForCondition(() -> {
                    topVerFut.set(distributionZoneManager.topVerFutures().get(106L));

                    return topVerFut.get() != null;
                },
                3_000));

        assertFalse(topVerFut.get().isDone());

        topologyWatchListenerOnUpdate(Set.of("node0"), 200, 12);

        assertTrue(waitForCondition(() -> topVerFut.get().isDone(),
                3_000));

        revisionFut.set(null);

        assertTrue(waitForCondition(() -> {
                    DistributionZoneManager.DataNodes dataNodes = distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID);

                    if (dataNodes == null) {
                        return false;
                    }

                    revisionFut.set(dataNodes.getRevisionScaleDownFutures().get(12L));

                    return revisionFut.get() != null;
                },
                3_000));

        assertFalse(revisionFut.get().isDone());

        dataNodesWatchListenerOnUpdate(Set.of("node0"), false, 12, 13);

        assertTrue(waitForCondition(() -> revisionFut.get().isDone(),
                3_000));

        assertEquals(Set.of("node0"), dataNodesFut.get());

        assertTrue(distributionZoneManager.topVerFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleUpFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleDownFutures().isEmpty());
    }

//    private void mockVaultZonesLogicalTopologyKey(Set<String> nodes) {
//        byte[] newLogicalTopology = toBytes(nodes);
//
//        when(vaultManager.get(zonesLogicalTopologyKey()))
//                .thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), newLogicalTopology)));
//    }

    private void topologyWatchListenerOnUpdate(Set<String> nodes, long topVer, long rev) {
        byte[] newLogicalTopology = toBytes(nodes);
        byte[] newTopVer = toBytes(topVer);

        Entry newEntry0 = new EntryImpl(zonesLogicalTopologyKey().bytes(), newLogicalTopology, rev, 1);
        Entry newEntry1 = new EntryImpl(zonesLogicalTopologyVersionKey().bytes(), newTopVer, rev, 1);

        EntryEvent entryEvent0 = new EntryEvent(null, newEntry0);
        EntryEvent entryEvent1 = new EntryEvent(null, newEntry1);

        WatchEvent evt = new WatchEvent(List.of(entryEvent0, entryEvent1), rev);

        topologyWatchListener.onUpdate(evt);
    }

    private void dataNodesWatchListenerOnUpdate(Set<String> nodes, boolean isScaleUp, long scaleRevision, /*long scaleDownRevision, */long rev) {
        byte[] newDataNodes = toBytes(nodes);
        byte[] newScaleRevision = toBytes(scaleRevision);

        Entry newEntry0 = new EntryImpl(zoneDataNodesKey(DEFAULT_ZONE_ID).bytes(), newDataNodes, rev, 1);
        Entry newEntry1 = new EntryImpl(
                isScaleUp ? zoneScaleUpChangeTriggerKey(DEFAULT_ZONE_ID).bytes() : zoneScaleDownChangeTriggerKey(DEFAULT_ZONE_ID).bytes(),
                newScaleRevision,
                rev,
                1);

        EntryEvent entryEvent0 = new EntryEvent(null, newEntry0);
        EntryEvent entryEvent1 = new EntryEvent(null, newEntry1);
//        EntryEvent entryEvent2 = new EntryEvent(null, newEntry2);

        WatchEvent evt = new WatchEvent(List.of(entryEvent0, entryEvent1/*, entryEvent2*/), rev);

        dataNodesWatchListener.onUpdate(evt);
    }

    private void mockCmgLocalNodes() {
        when(cmgManager.logicalTopology()).thenReturn(completedFuture(logicalTopology.getLogicalTopology()));
    }

    private void updateLogicalTopologyInMetaStorage(LogicalTopologySnapshot newTopology) throws ExecutionException, InterruptedException {
        Set<String> topologyFromCmg = newTopology.nodes().stream().map(ClusterNode::name).collect(toSet());
        System.out.println("updateLogicalTopologyInMetaStorage_1");
        Condition updateCondition = or(value(zonesLogicalTopologyVersionKey()).lt(ByteUtils.longToBytes(newTopology.version())), notExists(zonesLogicalTopologyVersionKey()));

        Iif iff = iif(
                updateCondition,
                updateLogicalTopologyAndVersion(topologyFromCmg, newTopology.version()),
                ops().yield(false)
        );

        metaStorageManager.invoke(iff).thenAccept(res -> {
            System.out.println("updateLogicalTopologyInMetaStorage_2");

            if (res.getAsBoolean()) {
                LOG.info(
                        "QWER_1",//"Distribution zones' logical topology and version keys were updated [topology = {}, version = {}]",
                        Arrays.toString(topologyFromCmg.toArray()),
                        newTopology.version()
                );
            } else {
                LOG.info(
                        "QWER_2",//"Failed to update distribution zones' logical topology and version keys [topology = {}, version = {}]",
                        Arrays.toString(topologyFromCmg.toArray()),
                        newTopology.version()
                );
            }
        }).get();
    }

    private static class MetaStorageWrapper {
        private MetaStorageManager metaStorageManager;

        public MetaStorageWrapper(MetaStorageManager metaStorageManager) {
            this.metaStorageManager = metaStorageManager;
        }
    }
}
