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

package org.apache.ignite.internal.raft;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static org.apache.ignite.internal.network.ConstantClusterIdSupplier.withoutClusterId;
import static org.apache.ignite.internal.network.configuration.NetworkConfigurationSchema.DEFAULT_PORT;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.defaultChannelTypeRegistry;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.apache.ignite.internal.raft.PeersAndLearners.fromConsistentIds;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.apache.ignite.raft.TestWriteCommand.testWriteCommand;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.configuration.StaticNodeFinderChange;
import org.apache.ignite.internal.network.recovery.InMemoryStaleIds;
import org.apache.ignite.internal.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.OnHeapLogs;
import org.apache.ignite.internal.raft.storage.impl.UnlimitedBudget;
import org.apache.ignite.internal.raft.storage.impl.VolatileLogStorage;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.version.DefaultIgniteProductVersionSource;
import org.apache.ignite.internal.worker.fixtures.NoOpCriticalWorkerRegistry;
import org.apache.ignite.raft.TestWriteCommand;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.conf.ConfigurationEntry;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test that checks various scenarios where raft node re-enter the group with lost data. Data loss may occur in case when
 * "sync" is disabled, and last portion of the RAFT log wasn't stored durably due to power outage, for example.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ItTruncateSuffixAndRestartTest extends BaseIgniteAbstractTest {
    private static final int NODES = 3;

    private static final String GROUP_NAME = "foo";

    private static final ReplicationGroupId GROUP_ID = new TestReplicationGroupId(GROUP_NAME);

    private final PeersAndLearners raftGroupConfiguration = fromConsistentIds(
            range(0, NODES).mapToObj(ItTruncateSuffixAndRestartTest::nodeName).collect(toSet())
    );

    private final HybridClock hybridClock = new HybridClockImpl();

    /** List of resources to be released after each test. Greatly simplifies the code. */
    private final List<ManuallyCloseable> cleanup = new ArrayList<>();

    @WorkDirectory
    private Path workDir;

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private SystemLocalConfiguration systemLocalConfiguration;

    @InjectConfiguration
    private NetworkConfiguration networkConfiguration;

    private List<SimpleIgniteNode> nodes;

    @BeforeEach
    void setUp() {
        CompletableFuture<Void> changeFuture = networkConfiguration.change(cfg -> cfg
                .changeNodeFinder().convert(StaticNodeFinderChange.class)
                .changeNetClusterNodes(
                        range(port(0), port(NODES)).mapToObj(port -> "localhost:" + port).toArray(String[]::new)
                )
        );

        assertThat(changeFuture, willCompleteSuccessfully());

        nodes = range(0, NODES).mapToObj(SimpleIgniteNode::new).collect(toList());

        nodes.forEach(SimpleIgniteNode::startService);
    }

    @AfterEach
    void tearDown() throws Exception {
        Collections.reverse(cleanup);

        closeAllManually(cleanup.stream());
    }

    /**
     * Simple Ignite node, that only contains messaging service and RAFT manager. Capable of starting RAFT groups, that's it.
     * Uses volatile log storage, that's "persisted" in HEAP between restarts.
     */
    private class SimpleIgniteNode {
        final LogStorage logStorage = new VolatileLogStorage(new UnlimitedBudget(), new ReusableOnHeapLogs(), new OnHeapLogs());

        final LogStorageFactory logStorageFactory = new TestLogStorageFactory(logStorage);

        final String nodeName;

        final ClusterService clusterSvc;

        final Path nodeDir;

        final ComponentWorkingDir partitionsWorkDir;

        final Loza raftMgr;

        private @Nullable RaftGroupService raftGroupService;

        private TestRaftGroupListener raftGroupListener;

        private SimpleIgniteNode(int i) {
            nodeName = nodeName(i);
            nodeDir = workDir.resolve(nodeName);

            assertThat(networkConfiguration.port().update(port(i)), willCompleteSuccessfully());

            var nettyBootstrapFactory = new NettyBootstrapFactory(networkConfiguration, nodeName);

            assertThat(nettyBootstrapFactory.startAsync(new ComponentContext()), willCompleteSuccessfully());
            cleanup.add(() -> assertThat(nettyBootstrapFactory.stopAsync(new ComponentContext()), willCompleteSuccessfully()));

            clusterSvc = new TestScaleCubeClusterServiceFactory().createClusterService(
                    nodeName,
                    networkConfiguration,
                    nettyBootstrapFactory,
                    defaultSerializationRegistry(),
                    new InMemoryStaleIds(),
                    withoutClusterId(),
                    new NoOpCriticalWorkerRegistry(),
                    mock(FailureManager.class),
                    new NoOpMetricManager(),
                    defaultChannelTypeRegistry(),
                    new DefaultIgniteProductVersionSource()
            );

            assertThat(clusterSvc.startAsync(new ComponentContext()), willCompleteSuccessfully());
            cleanup.add(() -> assertThat(clusterSvc.stopAsync(new ComponentContext()), willCompleteSuccessfully()));

            partitionsWorkDir = new ComponentWorkingDir(nodeDir);

            raftMgr = TestLozaFactory.create(clusterSvc, raftConfiguration, systemLocalConfiguration, hybridClock);

            assertThat(raftMgr.startAsync(new ComponentContext()), willCompleteSuccessfully());
            cleanup.add(() -> assertThat(raftMgr.stopAsync(new ComponentContext()), willCompleteSuccessfully()));

            cleanup.add(this::stopService);
        }

        void startService() {
            // Refresh listener, to forget the old "last value".
            raftGroupListener = new TestRaftGroupListener();

            try {
                raftGroupService = raftMgr.startRaftGroupNode(
                        new RaftNodeId(GROUP_ID, new Peer(nodeName)),
                        raftGroupConfiguration,
                        raftGroupListener,
                        RaftGroupEventsListener.noopLsnr,
                        RaftGroupOptions.defaults()
                                .setLogStorageFactory(logStorageFactory)
                                .serverDataPath(partitionsWorkDir.metaPath())
                );
            } catch (NodeStoppingException e) {
                fail(e.getMessage());
            }
        }

        RaftGroupService getService() {
            assertNotNull(raftGroupService);

            return raftGroupService;
        }

        void stopService() {
            if (raftGroupService != null) {
                raftGroupService.shutdown();

                raftGroupService = null;

                try {
                    raftMgr.stopRaftNode(new RaftNodeId(GROUP_ID, new Peer(nodeName)));
                } catch (NodeStoppingException e) {
                    fail(e.getMessage());
                }
            }
        }
    }

    /**
     * Tests a situation when one of the nodes in RAFT group lost its last update upon restart.
     */
    @Test
    void testRestartSingleNode() throws Exception {
        SimpleIgniteNode node0 = nodes.get(0);
        SimpleIgniteNode node1 = nodes.get(1);
        SimpleIgniteNode node2 = nodes.get(2);

        assertThat(node1.getService().run(testWriteCommand("1")), willCompleteSuccessfully());
        waitForValueReplication(nodes, "1");

        node1.stopService();
        node1.logStorage.truncateSuffix(1);
        node1.startService();

        waitForValueReplication(nodes, "1");

        assertThat(node1.getService().run(testWriteCommand("2")), willCompleteSuccessfully());
        waitForValueReplication(nodes, "2");

        assertEquals(node0.logStorage.getLastLogIndex(), node1.logStorage.getLastLogIndex());
        assertEquals(node1.logStorage.getLastLogIndex(), node2.logStorage.getLastLogIndex());
    }

    /**
     * Tests a situation when 2 nodes out of 3 are being restarted, one of which experienced data loss in RAFT log.
     */
    @Test
    void testRestartTwoNodes() throws Exception {
        SimpleIgniteNode node0 = nodes.get(0);
        SimpleIgniteNode node1 = nodes.get(1);
        SimpleIgniteNode node2 = nodes.get(2);

        assertThat(node1.getService().run(testWriteCommand("1")), willCompleteSuccessfully());
        waitForValueReplication(nodes, "1");

        node1.stopService();
        node2.stopService();

        node1.logStorage.truncateSuffix(1);

        node2.startService();
        node1.startService();

        waitForValueReplication(nodes, "1");

        assertThat(node1.getService().run(testWriteCommand("2")), willCompleteSuccessfully());
        waitForValueReplication(nodes, "2");

        assertEquals(node0.logStorage.getLastLogIndex(), node1.logStorage.getLastLogIndex());
        assertEquals(node1.logStorage.getLastLogIndex(), node2.logStorage.getLastLogIndex());
    }

    private static String nodeName(int i) {
        return "foo" + i;
    }

    private static int port(int i) {
        return DEFAULT_PORT + i;
    }

    private static void waitForValueReplication(List<SimpleIgniteNode> nodes, String value) throws InterruptedException {
        for (SimpleIgniteNode node : nodes) {
            assertTrue(waitForCondition(() -> value.equals(node.raftGroupListener.lastValue), 2, 10_000));
        }
    }

    /**
     * Version of {@link OnHeapLogs} that recovers raft configuration on start, because it may not be empty at that time.
     */
    private static class ReusableOnHeapLogs extends OnHeapLogs {
        @Override
        public boolean init(LogStorageOptions opts) {
            ConfigurationManager confManager = opts.getConfigurationManager();

            // Log always starts with "1". There's also no truncation in this test.
            assertNull(getEntry(0));

            for (int i = 1;; i++) {
                LogEntry entry = getEntry(i);

                // Loop until there are no more log entries. That's the only way to iterate the on-heap log.
                if (entry == null) {
                    break;
                }

                if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                    ConfigurationEntry confEntry = new ConfigurationEntry();

                    confEntry.setId(new LogId(entry.getId().getIndex(), entry.getId().getTerm()));
                    confEntry.setConf(new Configuration(entry.getPeers(), entry.getLearners()));

                    if (entry.getOldPeers() != null) {
                        confEntry.setOldConf(new Configuration(entry.getOldPeers(), entry.getOldLearners()));
                    }

                    confManager.add(confEntry);
                }
            }

            return super.init(opts);
        }
    }

    private static class TestRaftGroupListener implements RaftGroupListener {
        volatile String lastValue;

        @Override
        public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        }

        @Override
        public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
            iterator.forEachRemaining(closure -> {
                lastValue = ((TestWriteCommand) closure.command()).value();

                closure.result(null);
            });
        }

        @Override
        public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
            doneClo.accept(null);
        }

        @Override
        public boolean onSnapshotLoad(Path path) {
            return false;
        }

        @Override
        public void onShutdown() {
        }
    }

    private static class TestLogStorageFactory implements LogStorageFactory {
        private final LogStorage logStorage;

        TestLogStorageFactory(LogStorage logStorage) {
            this.logStorage = logStorage;
        }

        @Override
        public LogStorage createLogStorage(String uri, RaftOptions raftOptions) {
            return logStorage;
        }

        @Override
        public void destroyLogStorage(String uri) {
            // No-op.
        }

        @Override
        public Set<String> raftNodeStorageIdsOnDisk() {
            // There is nothing on disk.
            return Set.of();
        }

        @Override
        public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
            return nullCompletedFuture();
        }

        @Override
        public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
            return nullCompletedFuture();
        }

        @Override
        public void sync(){
        }
    }
}
