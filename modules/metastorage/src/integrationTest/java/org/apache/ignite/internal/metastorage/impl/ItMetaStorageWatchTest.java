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

package org.apache.ignite.internal.metastorage.impl;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.apache.ignite.internal.cluster.management.ClusterIdHolder;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.NodeAttributesCollector;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.RaftGroupOptionsConfigHelper;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.disaster.system.SystemDisasterRecoveryStorage;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.TestLozaFactory;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for Meta Storage Watches.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
public class ItMetaStorageWatchTest extends IgniteAbstractTest {

    @InjectConfiguration
    private NodeAttributesConfiguration nodeAttributes;

    @InjectConfiguration
    private StorageConfiguration storageConfiguration;

    @InjectConfiguration
    private SystemDistributedConfiguration systemConfiguration;

    @InjectExecutorService
    private ScheduledExecutorService scheduledExecutorService;

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private SystemLocalConfiguration systemLocalConfiguration;

    private class Node {
        private final List<IgniteComponent> components = new ArrayList<>();

        private final ClusterService clusterService;

        private final MetaStorageManager metaStorageManager;

        private final ClusterManagementGroupManager cmgManager;

        Node(ClusterService clusterService, Path dataPath) {
            var vaultManager = new VaultManager(new InMemoryVaultService());

            components.add(vaultManager);

            this.clusterService = clusterService;

            components.add(clusterService);

            Path basePath = dataPath.resolve(name());

            HybridClock clock = new HybridClockImpl();

            var raftGroupEventsClientListener = new RaftGroupEventsClientListener();

            ComponentWorkingDir workingDir = new ComponentWorkingDir(basePath.resolve("raft"));

            LogStorageFactory partitionsLogStorageFactory = SharedLogStorageFactoryUtils.create(
                    clusterService.nodeName(),
                    workingDir.raftLogPath()
            );

            components.add(partitionsLogStorageFactory);

            var raftManager = TestLozaFactory.create(
                    clusterService,
                    raftConfiguration,
                    systemLocalConfiguration,
                    clock,
                    raftGroupEventsClientListener
            );

            components.add(raftManager);

            var clusterStateStorage = new TestClusterStateStorage();

            components.add(clusterStateStorage);

            FailureManager failureManager = new NoOpFailureManager();

            var logicalTopology = new LogicalTopologyImpl(clusterStateStorage, failureManager);

            var clusterInitializer = new ClusterInitializer(
                    clusterService,
                    hocon -> hocon,
                    new TestConfigurationValidator()
            );

            components.add(failureManager);

            ComponentWorkingDir cmgWorkDir = new ComponentWorkingDir(basePath.resolve("cmg"));

            LogStorageFactory cmgLogStorageFactory =
                    SharedLogStorageFactoryUtils.create(clusterService.nodeName(), cmgWorkDir.raftLogPath());

            components.add(cmgLogStorageFactory);

            RaftGroupOptionsConfigurer cmgRaftConfigurer =
                    RaftGroupOptionsConfigHelper.configureProperties(cmgLogStorageFactory, cmgWorkDir.metaPath());

            MetricManager metricManager = new NoOpMetricManager();

            components.add(metricManager);

            this.cmgManager = new ClusterManagementGroupManager(
                    vaultManager,
                    new SystemDisasterRecoveryStorage(vaultManager),
                    clusterService,
                    clusterInitializer,
                    raftManager,
                    clusterStateStorage,
                    logicalTopology,
                    new NodeAttributesCollector(nodeAttributes, storageConfiguration),
                    failureManager,
                    new ClusterIdHolder(),
                    cmgRaftConfigurer,
                    metricManager
            );

            components.add(cmgManager);

            var logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgManager);

            var topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                    clusterService,
                    logicalTopologyService,
                    Loza.FACTORY,
                    raftGroupEventsClientListener
            );

            ComponentWorkingDir metastorageWorkDir = new ComponentWorkingDir(basePath.resolve("storage"));

            LogStorageFactory msLogStorageFactory = SharedLogStorageFactoryUtils.create(
                    clusterService.nodeName(),
                    metastorageWorkDir.raftLogPath()
            );

            components.add(msLogStorageFactory);

            RaftGroupOptionsConfigurer msRaftConfigurer =
                    RaftGroupOptionsConfigHelper.configureProperties(msLogStorageFactory, metastorageWorkDir.metaPath());

            var readOperationForCompactionTracker = new ReadOperationForCompactionTracker();

            var storage = new RocksDbKeyValueStorage(
                    name(),
                    metastorageWorkDir.dbPath(),
                    new NoOpFailureManager(),
                    readOperationForCompactionTracker,
                    scheduledExecutorService
            );

            this.metaStorageManager = new MetaStorageManagerImpl(
                    clusterService,
                    cmgManager,
                    logicalTopologyService,
                    raftManager,
                    storage,
                    clock,
                    topologyAwareRaftGroupServiceFactory,
                    metricManager,
                    systemConfiguration,
                    msRaftConfigurer,
                    readOperationForCompactionTracker
            );
        }

        CompletableFuture<Void> start() {
            var context = new ComponentContext();

            return startAsync(context, components)
                    .thenCompose(v -> cmgManager.joinFuture())
                    .thenCompose(v -> metaStorageManager.startAsync(context))
                    .thenCompose(v -> metaStorageManager.recoveryFinishedFuture())
                    .thenCompose(v -> cmgManager.onJoinReady());
        }

        String name() {
            return clusterService.nodeName();
        }

        void stop() throws Exception {
            List<IgniteComponent> componentsToStop = new ArrayList<>(components);
            componentsToStop.add(metaStorageManager);

            Collections.reverse(componentsToStop);

            Stream<AutoCloseable> beforeNodeStop = componentsToStop.stream().map(c -> c::beforeNodeStop);

            Stream<AutoCloseable> nodeStop = Stream.of(() ->
                    assertThat(stopAsync(new ComponentContext(), componentsToStop), willCompleteSuccessfully())
            );

            IgniteUtils.closeAll(Stream.concat(beforeNodeStop, nodeStop));
        }
    }

    private final List<Node> nodes = new ArrayList<>();

    @BeforeEach
    public void beforeTest(TestInfo testInfo) throws NodeStoppingException {
        startCluster(testInfo, 3);
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(nodes.parallelStream().map(node -> node::stop));
    }

    private void startCluster(TestInfo testInfo, int size) throws NodeStoppingException {
        List<NetworkAddress> localAddresses = findLocalAddresses(10_000, 10_000 + nodes.size() + size);

        var nodeFinder = new StaticNodeFinder(localAddresses);

        localAddresses.stream()
                .map(addr -> ClusterServiceTestUtils.clusterService(testInfo, addr.port(), nodeFinder))
                .forEach(clusterService -> nodes.add(new Node(clusterService, workDir)));

        CompletableFuture<?>[] startFutures = nodes.parallelStream().map(Node::start).toArray(CompletableFuture[]::new);

        String name = nodes.get(0).name();

        nodes.get(0).cmgManager.initCluster(List.of(name), List.of(name), "test");

        assertThat(allOf(startFutures), willCompleteSuccessfully());
    }

    @Test
    void testExactWatch() throws Exception {
        testWatches((node, latch) -> node.metaStorageManager.registerExactWatch(new ByteArray("foo"), event -> {
            assertThat(event.entryEvent().newEntry().key(), is("foo".getBytes(StandardCharsets.UTF_8)));
            assertThat(event.entryEvent().newEntry().value(), is("bar".getBytes(StandardCharsets.UTF_8)));

            latch.countDown();

            return nullCompletedFuture();
        }));
    }

    @Test
    void testPrefixWatch() throws Exception {
        testWatches((node, latch) -> node.metaStorageManager.registerPrefixWatch(new ByteArray("fo"), event -> {
            assertThat(event.entryEvent().newEntry().key(), is("foo".getBytes(StandardCharsets.UTF_8)));
            assertThat(event.entryEvent().newEntry().value(), is("bar".getBytes(StandardCharsets.UTF_8)));

            latch.countDown();

            return nullCompletedFuture();
        }));
    }

    @Test
    void testRangeWatch() throws Exception {
        testWatches((node, latch) -> {
            var startRange = new ByteArray("fo" + ('o' - 1));
            var endRange = new ByteArray("foz");

            node.metaStorageManager.registerRangeWatch(startRange, endRange, event -> {
                assertThat(event.entryEvent().newEntry().key(), is("foo".getBytes(StandardCharsets.UTF_8)));
                assertThat(event.entryEvent().newEntry().value(), is("bar".getBytes(StandardCharsets.UTF_8)));

                latch.countDown();

                return nullCompletedFuture();
            });
        });
    }

    private void testWatches(BiConsumer<Node, CountDownLatch> registerWatchAction) throws Exception {
        var latch = new CountDownLatch(nodes.size());

        for (Node node : nodes) {
            registerWatchAction.accept(node, latch);

            assertThat("Watches were not deployed", node.metaStorageManager.deployWatches(), willCompleteSuccessfully());
        }

        var key = new ByteArray("foo");

        CompletableFuture<Boolean> invokeFuture = nodes.get(0).metaStorageManager.invoke(
                Conditions.notExists(key),
                Operations.put(key, "bar".getBytes(StandardCharsets.UTF_8)),
                Operations.noop()
        );

        assertThat(invokeFuture, willBe(true));

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    /**
     * Tests that missed metastorage events are replayed after deploying watches.
     */
    @Test
    void testReplayUpdates() throws Exception {
        var exactLatch = new CountDownLatch(nodes.size());
        var prefixLatch = new CountDownLatch(nodes.size());

        for (Node node : nodes) {
            node.metaStorageManager.registerExactWatch(new ByteArray("foo"), event -> {
                assertThat(event.entryEvent().newEntry().key(), is("foo".getBytes(StandardCharsets.UTF_8)));
                assertThat(event.entryEvent().newEntry().value(), is("bar".getBytes(StandardCharsets.UTF_8)));

                exactLatch.countDown();

                return nullCompletedFuture();
            });

            node.metaStorageManager.registerPrefixWatch(new ByteArray("ba"), event -> {
                List<String> keys = event.entryEvents().stream()
                        .map(e -> new String(e.newEntry().key(), StandardCharsets.UTF_8))
                        .collect(toList());

                List<String> values = event.entryEvents().stream()
                        .map(e -> new String(e.newEntry().value(), StandardCharsets.UTF_8))
                        .collect(toList());

                assertThat(keys, containsInAnyOrder("bar", "baz"));
                assertThat(values, containsInAnyOrder("one", "two"));

                prefixLatch.countDown();

                return nullCompletedFuture();
            });
        }

        CompletableFuture<Boolean> invokeFuture = nodes.get(0).metaStorageManager.invoke(
                Conditions.notExists(new ByteArray("foo")),
                Operations.put(new ByteArray("foo"), "bar".getBytes(StandardCharsets.UTF_8)),
                Operations.noop()
        );

        assertThat(invokeFuture, willBe(true));

        invokeFuture = nodes.get(0).metaStorageManager.invoke(
                Conditions.exists(new ByteArray("foo")),
                List.of(
                        Operations.put(new ByteArray("bar"), "one".getBytes(StandardCharsets.UTF_8)),
                        Operations.put(new ByteArray("baz"), "two".getBytes(StandardCharsets.UTF_8))
                ),
                List.of()
        );

        assertThat(invokeFuture, willBe(true));

        nodes.forEach(node -> assertThat("Watches were not deployed", node.metaStorageManager.deployWatches(), willCompleteSuccessfully()));

        assertTrue(exactLatch.await(10, TimeUnit.SECONDS));
        assertTrue(prefixLatch.await(10, TimeUnit.SECONDS));
    }

    /**
     * Tests that missed metastorage events are replayed with correct timestamps.
     */
    @Test
    void updatesAreReplayedWithCorrectTimestamps() throws Exception {
        List<RevisionAndTimestamp> seenRevisionsAndTimestamps = new CopyOnWriteArrayList<>();

        for (Node node : nodes) {
            node.metaStorageManager.registerPrefixWatch(new ByteArray("prefix"), event -> {
                seenRevisionsAndTimestamps.add(new RevisionAndTimestamp(event.revision(), event.timestamp()));

                return nullCompletedFuture();
            });
        }

        MetaStorageManager metaStorageManager0 = nodes.get(0).metaStorageManager;

        ByteArray key1 = new ByteArray("prefix.1");
        ByteArray key2 = new ByteArray("prefix.2");

        assertThat(metaStorageManager0.put(key1, new byte[0]), willCompleteSuccessfully());
        assertThat(metaStorageManager0.put(key2, new byte[0]), willCompleteSuccessfully());

        nodes.forEach(node -> assertThat("Watches were not deployed", node.metaStorageManager.deployWatches(), willCompleteSuccessfully()));

        assertTrue(waitForCondition(() -> seenRevisionsAndTimestamps.size() == nodes.size() * 2, TimeUnit.SECONDS.toMillis(10)));

        // Each revision must be accompanied with the same timestamp on each node.
        Set<RevisionAndTimestamp> revsAndTssSet = new HashSet<>(seenRevisionsAndTimestamps);
        assertThat(revsAndTssSet, hasSize(2));

        Map<Long, HybridTimestamp> revToTs = revsAndTssSet.stream()
                .collect(toMap(rvAndTs -> rvAndTs.revision, rvAndTs -> rvAndTs.timestamp));

        assertThat(revToTs.values().stream().distinct().count(), is(2L));

        // Make sure that timestamps from WatchEvents are same as in the storage.
        Entry entry1 = metaStorageManager0.getLocally(key1, Long.MAX_VALUE);
        Entry entry2 = metaStorageManager0.getLocally(key2, Long.MAX_VALUE);

        assertThat(revToTs.get(entry1.revision()), is(metaStorageManager0.timestampByRevisionLocally(entry1.revision())));
        assertThat(revToTs.get(entry2.revision()), is(metaStorageManager0.timestampByRevisionLocally(entry2.revision())));
    }

    private static class RevisionAndTimestamp {
        private final long revision;
        private final HybridTimestamp timestamp;

        private RevisionAndTimestamp(long revision, HybridTimestamp timestamp) {
            this.revision = revision;
            this.timestamp = timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RevisionAndTimestamp that = (RevisionAndTimestamp) o;
            return revision == that.revision && Objects.equals(timestamp, that.timestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(revision, timestamp);
        }
    }
}
