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

import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.clusterService;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getFieldValue;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.raft.TestWriteCommand.testWriteCommand;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.RaftGroupOptionsConfigHelper;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.raft.configuration.LogStorageBudgetView;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.VolatileLogStorageFactoryCreator;
import org.apache.ignite.internal.raft.storage.impl.VolatileRaftMetaStorage;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.storage.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link Loza} functionality.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItLozaTest extends IgniteAbstractTest {
    /** Server port offset. */
    private static final int PORT = 20010;

    private final ComponentContext componentContext = new ComponentContext();

    private ClusterService clusterService;

    private Loza loza;

    private final List<IgniteComponent> allComponents = new ArrayList<>();

    @BeforeEach
    void setUp(TestInfo testInfo) {
        var addr = new NetworkAddress("localhost", PORT);

        clusterService = clusterService(testInfo, PORT, new StaticNodeFinder(List.of(addr)));

        assertThat(clusterService.startAsync(componentContext), willCompleteSuccessfully());

        allComponents.add(clusterService);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (loza != null) {
            IgniteUtils.closeAll(loza.localNodes().stream().map(nodeId -> () -> loza.stopRaftNode(nodeId)));

            allComponents.add(loza);
        }

        Collections.reverse(allComponents);

        IgniteUtils.stopAsync(componentContext, allComponents);
    }

    /**
     * Starts a raft group service with a provided group id on a provided Loza instance.
     *
     * @return Raft group service.
     */
    private RaftGroupService startClient(
            TestReplicationGroupId groupId,
            InternalClusterNode node,
            RaftGroupOptionsConfigurer groupOptionsConfigurer) throws Exception {
        RaftGroupListener raftGroupListener = mock(RaftGroupListener.class);

        when(raftGroupListener.onSnapshotLoad(any())).thenReturn(true);

        PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(Set.of(node.name()));

        var nodeId = new RaftNodeId(groupId, configuration.peer(node.name()));

        RaftGroupOptions ops = RaftGroupOptions.defaults();

        groupOptionsConfigurer.configure(ops);

        return loza.startRaftGroupNode(
                nodeId,
                configuration,
                raftGroupListener,
                RaftGroupEventsListener.noopLsnr,
                ops
        );
    }

    /**
     * Tests that RaftGroupServiceImpl uses shared executor for retrying RaftGroupServiceImpl#sendWithRetry().
     */
    @Test
    public void testRaftServiceUsingSharedExecutor(
            @InjectConfiguration RaftConfiguration raftConfiguration,
            @InjectConfiguration SystemLocalConfiguration systemLocalConfiguration
    ) throws Exception {
        ClusterService spyService = spy(clusterService);

        MessagingService messagingServiceMock = spy(spyService.messagingService());

        when(spyService.messagingService()).thenReturn(messagingServiceMock);

        CompletableFuture<NetworkMessage> exception = CompletableFuture.failedFuture(new IOException());

        ComponentWorkingDir partitionsWorkDir = new ComponentWorkingDir(workDir);

        LogStorageFactory partitionsLogStorageFactory = SharedLogStorageFactoryUtils.create(
                spyService.nodeName(),
                partitionsWorkDir.raftLogPath()
        );

        RaftGroupOptionsConfigurer partitionsConfigurer =
                RaftGroupOptionsConfigHelper.configureProperties(partitionsLogStorageFactory, partitionsWorkDir.metaPath());

        allComponents.add(partitionsLogStorageFactory);

        assertThat(partitionsLogStorageFactory.startAsync(componentContext), willCompleteSuccessfully());

        loza = TestLozaFactory.create(spyService, raftConfiguration, systemLocalConfiguration, new HybridClockImpl());

        assertThat(loza.startAsync(componentContext), willCompleteSuccessfully());

        for (int i = 0; i < 5; i++) {
            // return an error on first invocation
            doReturn(exception)
                    // assert that a retry has been issued on the executor
                    .doAnswer(invocation -> {
                        assertThat(Thread.currentThread().getName(), containsString(Loza.CLIENT_POOL_NAME));

                        return exception;
                    })
                    // finally call the real method
                    .doCallRealMethod()
                    .when(messagingServiceMock).invoke(any(InternalClusterNode.class), any(), anyLong());

            RaftGroupService client = startClient(
                    new TestReplicationGroupId(Integer.toString(i)),
                    spyService.topologyService().localMember(),
                    partitionsConfigurer
            );

            assertThat(client.refreshLeader(), willCompleteSuccessfully());

            verify(messagingServiceMock, times(3 * (i + 1)))
                    .invoke(any(InternalClusterNode.class), any(), anyLong());
        }
    }

    /**
     * Tests a scenario when two types of Raft Log Storages are bound to the same stripe and validates that data gets correctly saved
     * to both of them during writes.
     */
    @RepeatedTest(100)
    void testVolatileAndPersistentGroupsOnSameStripe(
            @InjectConfiguration("mock.logStripesCount=1")
            RaftConfiguration raftConfiguration,
            @InjectConfiguration
            SystemLocalConfiguration systemLocalConfiguration
    ) throws Exception {
        ComponentWorkingDir partitionsWorkDir = new ComponentWorkingDir(workDir);

        LogStorageFactory logStorageFactory = SharedLogStorageFactoryUtils.create(
                clusterService.nodeName(),
                partitionsWorkDir.raftLogPath()
        );

        allComponents.add(logStorageFactory);

        assertThat(logStorageFactory.startAsync(componentContext), willCompleteSuccessfully());

        loza = TestLozaFactory.create(clusterService, raftConfiguration, systemLocalConfiguration, new HybridClockImpl());

        assertThat(loza.startAsync(componentContext), willCompleteSuccessfully());

        String nodeName = clusterService.nodeName();

        var volatileLogStorageFactoryCreator = new VolatileLogStorageFactoryCreator(nodeName, workDir.resolve("spill"));

        assertThat(volatileLogStorageFactoryCreator.startAsync(componentContext), willCompleteSuccessfully());

        allComponents.add(volatileLogStorageFactoryCreator);

        PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(Set.of(nodeName));

        Peer peer = configuration.peer(nodeName);

        RaftGroupListener raftGroupListener = new DrainingRaftGroupListener();

        LogStorageBudgetView volatileCfg = raftConfiguration.volatileRaft().logStorageBudget().value();

        LogStorageFactory volatileLogStorageFactory = volatileLogStorageFactoryCreator.factory(volatileCfg);

        // Start two Raft nodes: one backed by a volatile storage and the other - by "shared" persistent storage.
        var volatileRaftNodeId = new RaftNodeId(new TestReplicationGroupId("volatile"), peer);

        RaftGroupService volatileService = loza.startRaftGroupNode(
                volatileRaftNodeId,
                configuration,
                raftGroupListener,
                RaftGroupEventsListener.noopLsnr,
                RaftGroupOptions.forVolatileStores()
                        .setLogStorageFactory(volatileLogStorageFactory)
                        .raftMetaStorageFactory((groupId, raftOptions) -> new VolatileRaftMetaStorage())
                        .serverDataPath(partitionsWorkDir.metaPath())
        );

        var persistentNodeId = new RaftNodeId(new TestReplicationGroupId("persistent"), peer);

        RaftGroupService persistentService = loza.startRaftGroupNode(
                persistentNodeId,
                configuration,
                raftGroupListener,
                RaftGroupEventsListener.noopLsnr,
                RaftGroupOptions.forPersistentStores()
                        .setLogStorageFactory(logStorageFactory)
                        .serverDataPath(partitionsWorkDir.metaPath())
        );

        // Execute two write command in parallel. We then hope that these commands wil be batched together.
        WriteCommand cmd = testWriteCommand("foo");

        CompletableFuture<Void> f1 = volatileService.run(cmd);
        CompletableFuture<Void> f2 = persistentService.run(cmd);

        assertThat(f1, willCompleteSuccessfully());
        assertThat(f2, willCompleteSuccessfully());

        // Inspect the raft log and validates that both writes have been committed to the storage.
        validateLastLogEntry(volatileRaftNodeId);
        validateLastLogEntry(persistentNodeId);
    }

    private void validateLastLogEntry(RaftNodeId raftNodeId) {
        Node raftNode = ((JraftServerImpl) loza.server()).raftGroupService(raftNodeId).getRaftNode();

        LogManager logManager = getFieldValue(raftNode, "logManager");

        long lastLogIndex = logManager.getLastLogIndex();

        LogEntry entry = logManager.getEntry(lastLogIndex);

        assertThat(entry, is(notNullValue()));
    }

    @Test
    void destroysRaftNodeStorages(
            @InjectConfiguration RaftConfiguration raftConfiguration,
            @InjectConfiguration SystemLocalConfiguration systemLocalConfiguration
    ) throws Exception {
        ComponentWorkingDir partitionsWorkDir = new ComponentWorkingDir(workDir);

        LogStorageFactory logStorageFactory = SharedLogStorageFactoryUtils.create(
                clusterService.nodeName(),
                partitionsWorkDir.raftLogPath()
        );
        logStorageFactory = spy(logStorageFactory);

        allComponents.add(logStorageFactory);

        assertThat(logStorageFactory.startAsync(componentContext), willCompleteSuccessfully());

        loza = TestLozaFactory.create(clusterService, raftConfiguration, systemLocalConfiguration, new HybridClockImpl());

        assertThat(loza.startAsync(componentContext), willCompleteSuccessfully());

        PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(Set.of(clusterService.nodeName()));
        Peer peer = configuration.peer(clusterService.nodeName());

        RaftGroupListener raftGroupListener = new DrainingRaftGroupListener();

        var replicationGroupId = new TestReplicationGroupId("persistent");
        var nodeId = new RaftNodeId(replicationGroupId, peer);

        RaftGroupOptionsConfigurer configurer = RaftGroupOptionsConfigHelper.configureProperties(
                logStorageFactory,
                partitionsWorkDir.metaPath()
        );

        RaftGroupService service = loza.startRaftGroupNode(
                nodeId,
                configuration,
                raftGroupListener,
                RaftGroupEventsListener.noopLsnr,
                null,
                configurer
        );

        assertThat(service.run(testWriteCommand("foo")), willCompleteSuccessfully());

        loza.stopRaftNodes(replicationGroupId);

        String groupUri = nodeId.groupId().toString() + "-" + nodeId.peer().idx();
        Path groupRaftStoragesPath = partitionsWorkDir.metaPath().resolve(groupUri);

        assertTrue(Files.isDirectory(groupRaftStoragesPath));

        loza.destroyRaftNodeStorages(nodeId, configurer);

        verify(logStorageFactory).destroyLogStorage(groupUri);
        assertFalse(Files.exists(groupRaftStoragesPath));
    }

    private static class DrainingRaftGroupListener implements RaftGroupListener {
        @Override
        public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
            iterator.forEachRemaining(c -> c.result(null));
        }

        @Override
        public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        }

        @Override
        public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        }

        @Override
        public boolean onSnapshotLoad(Path path) {
            return true;
        }

        @Override
        public void onShutdown() {
        }
    }
}
