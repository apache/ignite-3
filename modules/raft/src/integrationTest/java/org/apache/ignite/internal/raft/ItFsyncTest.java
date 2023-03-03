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
import static org.apache.ignite.internal.network.configuration.NetworkConfigurationSchema.DEFAULT_PORT;
import static org.apache.ignite.internal.raft.PeersAndLearners.fromConsistentIds;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.apache.ignite.raft.TestWriteCommand.testWriteCommand;
import static org.apache.ignite.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
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
import org.apache.ignite.raft.jraft.storage.impl.OnHeapLogs;
import org.apache.ignite.raft.jraft.storage.impl.UnlimitedBudget;
import org.apache.ignite.raft.jraft.storage.impl.VolatileLogStorage;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test that checks various scenarios where raft node re-enter the group with lost data.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ItFsyncTest {
    private static final int NODES = 3;

    private static final String GROUP_NAME = "foo";

    private static final ReplicationGroupId GROUP_ID = new TestReplicationGroupId(GROUP_NAME);

    private final PeersAndLearners configuration = fromConsistentIds(range(0, NODES).mapToObj(this::nodeName).collect(toSet()));

    private final HybridClock hybridClock = new HybridClockImpl();

    /** List of resources to be released after each test. Greatly simplifies the code. */
    private final List<ManuallyCloseable> cleanup = new ArrayList<>();

    @WorkDirectory
    private Path workDir;

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private NetworkConfiguration networkConfiguration;

    private List<SimpleIgniteNode> nodes;

    @BeforeEach
    void setUp() {
        CompletableFuture<Void> changeFuture = networkConfiguration.change(cfg -> cfg
                .changePortRange(0)
                .changeNodeFinder().changeNetClusterNodes(
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

        final Loza raftMgr;

        private @Nullable CompletableFuture<RaftGroupService> serviceFuture;

        private TestRaftGroupListener raftGroupListener;

        private SimpleIgniteNode(int i) {
            nodeName = nodeName(i);
            nodeDir = workDir.resolve(nodeName);

            assertThat(networkConfiguration.port().update(port(i)), willCompleteSuccessfully());

            var clusterLocalConfiguration = new ClusterLocalConfiguration(nodeName, defaultSerializationRegistry());

            var nettyBootstrapFactory = new NettyBootstrapFactory(networkConfiguration, clusterLocalConfiguration.getName());

            nettyBootstrapFactory.start();
            cleanup.add(nettyBootstrapFactory::stop);

            clusterSvc = new TestScaleCubeClusterServiceFactory().createClusterService(
                    clusterLocalConfiguration,
                    networkConfiguration,
                    nettyBootstrapFactory
            );

            clusterSvc.start();
            cleanup.add(clusterSvc::stop);

            raftMgr = new Loza(clusterSvc, raftConfiguration, nodeDir, hybridClock);

            raftMgr.start();
            cleanup.add(raftMgr::stop);

            cleanup.add(this::stopService);
        }

        void startService() {
            // Refresh listener, to forget the old "last value".
            raftGroupListener = new TestRaftGroupListener();

            try {
                serviceFuture = raftMgr.startRaftGroupNode(
                        new RaftNodeId(GROUP_ID, new Peer(nodeName)),
                        configuration,
                        raftGroupListener,
                        RaftGroupEventsListener.noopLsnr,
                        RaftGroupOptions.defaults().setLogStorageFactory(logStorageFactory)
                );
            } catch (NodeStoppingException e) {
                fail(e.getMessage());
            }
        }

        RaftGroupService getService() {
            assertNotNull(serviceFuture);
            assertThat(serviceFuture, willCompleteSuccessfully());

            return serviceFuture.join();
        }

        void stopService() {
            if (serviceFuture != null) {
                serviceFuture = null;

                try {
                    raftMgr.stopRaftNode(new RaftNodeId(GROUP_ID, new Peer(nodeName)));
                } catch (NodeStoppingException e) {
                    fail(e.getMessage());
                }
            }
        }
    }

    @Test
    void testFsync() throws Exception {
        SimpleIgniteNode node0 = nodes.get(0);
        SimpleIgniteNode node1 = nodes.get(1);
        SimpleIgniteNode node2 = nodes.get(2);

        assertThat(node1.getService().run(testWriteCommand("1")), willCompleteSuccessfully());

        waitForValueReplication(nodes, "1");

//        node0.stopService();

        node1.stopService();
        node1.logStorage.truncateSuffix(1);

        node2.stopService();
//        node2.logStorage.truncateSuffix(1);

        System.out.println("<%> Restart services");

//        node0.startService();
        node1.startService();
//        Thread.sleep(5000);

        node2.startService();

        assertThat(node1.getService().run(testWriteCommand("2")), willCompleteSuccessfully());

        waitForValueReplication(nodes, "2");

        assertEquals(node0.logStorage.getLastLogIndex(), node1.logStorage.getLastLogIndex());
        assertEquals(node1.logStorage.getLastLogIndex(), node2.logStorage.getLastLogIndex());
    }

    private String nodeName(int i) {
        return "foo" + i;
    }

    private static int port(int i) {
        return DEFAULT_PORT + i;
    }

    private static void waitForValueReplication(List<SimpleIgniteNode> nodes, String value) throws InterruptedException {
        for (SimpleIgniteNode node : nodes) {
            assertTrue(waitForCondition(() -> value.equals(node.raftGroupListener.lastValue), 2,1000));
        }
    }

    /**
     * Version of {@link OnHeapLogs} that recovers raft configuration on start, because it may not be empty at that time.
     */
    private static class ReusableOnHeapLogs extends OnHeapLogs {
        @Override
        public boolean init(LogStorageOptions opts) {
            ConfigurationManager confManager = opts.getConfigurationManager();

            for (int i = 0;; i++) {
                LogEntry entry = getEntry(i);

                // Loop until there are no more log entries. That's the only way to iterate the log.
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
        String lastValue;

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
        public void start() {
        }

        @Override
        public void close() {
        }
    }
}
