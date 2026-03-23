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

package org.apache.ignite.raft.server;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.configuration.IgnitePaths.vaultPath;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.raft.jraft.test.TestUtils.getLocalAddress;
import static org.apache.ignite.raft.jraft.test.TestUtils.waitForTopology;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.ThrottlingContextHolderImpl;
import org.apache.ignite.internal.raft.client.RaftGroupServiceImpl;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.TestJraftServerFactory;
import org.apache.ignite.internal.raft.server.impl.GroupStoragesContextResolver;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.storage.GroupStoragesDestructionIntents;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.VaultGroupStoragesDestructionIntents;
import org.apache.ignite.internal.raft.util.SharedLogStorageManagerUtils;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Abstract class for raft tests using JRaftServer.
 */
@ExtendWith(ConfigurationExtension.class)
public abstract class JraftAbstractTest extends RaftServerAbstractTest {
    /** Nodes count. */
    protected static final int NODES = 3;

    /**
     * The server port offset.
     */
    protected static final int PORT = 5003;

    /**
     * The client port offset.
     */
    private static final int CLIENT_PORT = 6003;

    /**
     * Initial configuration.
     */
    protected PeersAndLearners initialMembersConf;

    /**
     * Servers list.
     */
    protected final List<JraftServerImpl> servers = new ArrayList<>();

    protected final List<LogStorageManager> logStorageFactories = new ArrayList<>();

    protected final List<VaultManager> vaultManagers = new ArrayList<>();

    protected final List<ComponentWorkingDir> serverWorkingDirs = new ArrayList<>();

    protected final List<ClusterService> serverServices = new ArrayList<>();

    /**
     * Clients list.
     */
    protected final List<RaftGroupService> clients = new ArrayList<>();

    /** Executor for raft group services. */
    private ScheduledExecutorService executor;

    /**
     * Before each.
     */
    @BeforeEach
    void before() {
        executor = new ScheduledThreadPoolExecutor(20, IgniteThreadFactory.create("common", Loza.CLIENT_POOL_NAME, logger()));

        initialMembersConf = IntStream.range(0, nodesCount())
                .mapToObj(i -> testNodeName(testInfo, PORT + i))
                .collect(collectingAndThen(toSet(), PeersAndLearners::fromConsistentIds));
    }

    /**
     * After each.
     */
    @AfterEach
    @Override
    protected void after() throws Exception {
        super.after();

        shutdownCluster();

        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        TestUtils.assertAllJraftThreadsStopped();
    }

    protected void shutdownCluster() throws Exception {
        logger().info("Start client shutdown");

        Iterator<RaftGroupService> iterClients = clients.iterator();

        while (iterClients.hasNext()) {
            RaftGroupService client = iterClients.next();

            iterClients.remove();

            client.shutdown();
        }

        clients.clear();

        logger().info("Start server shutdown servers={}", servers.size());

        Iterator<JraftServerImpl> iterSrv = servers.iterator();

        while (iterSrv.hasNext()) {
            JraftServerImpl server = iterSrv.next();

            iterSrv.remove();

            if (server == null) {
                // This means it was already stopped.
                continue;
            }

            for (RaftNodeId nodeId : server.localNodes()) {
                server.stopRaftNode(nodeId);
            }

            server.beforeNodeStop();

            assertThat(server.stopAsync(new ComponentContext()), willCompleteSuccessfully());
        }

        servers.clear();

        assertThat(IgniteUtils.stopAsync(new ComponentContext(), serverServices), willCompleteSuccessfully());
        serverServices.clear();

        assertThat(IgniteUtils.stopAsync(new ComponentContext(), logStorageFactories), willCompleteSuccessfully());
        logStorageFactories.clear();

        assertThat(IgniteUtils.stopAsync(new ComponentContext(), vaultManagers), willCompleteSuccessfully());
        vaultManagers.clear();
    }

    /**
     * Starts server.
     *
     * @param idx  The index.
     * @param clo  Init closure.
     * @param optionsUpdater Node options updater.
     * @return Raft server instance.
     */
    protected JraftServerImpl startServer(int idx, Consumer<RaftServer> clo, Consumer<NodeOptions> optionsUpdater) {
        var addr = new NetworkAddress(getLocalAddress(), PORT);

        ClusterService service = clusterService(PORT + idx, List.of(addr), true);

        ComponentWorkingDir workingDir = new ComponentWorkingDir(workDir.resolve("node" + idx + "/partitions"));

        serverWorkingDirs.add(workingDir);

        LogStorageManager partitionsLogStorageManager = spy(SharedLogStorageManagerUtils.create(
                service.nodeName(),
                workingDir.raftLogPath()
        ));

        assertThat(partitionsLogStorageManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        logStorageFactories.add(partitionsLogStorageManager);

        NodeOptions opts = new NodeOptions();

        optionsUpdater.accept(opts);

        VaultManager vaultManager = new VaultManager(new PersistentVaultService(vaultPath(workingDir.basePath())));

        vaultManagers.add(vaultManager);

        assertThat(vaultManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        GroupStoragesDestructionIntents groupStoragesDestructionIntents = new VaultGroupStoragesDestructionIntents(vaultManager);

        String groupName = "testGroupName";

        GroupStoragesContextResolver groupStoragesContextResolver = new GroupStoragesContextResolver(
                replicationGroupId -> groupName,
                Map.of(groupName, workingDir.metaPath()),
                Map.of(groupName, partitionsLogStorageManager)
        );

        JraftServerImpl server = TestJraftServerFactory.create(
                service,
                opts,
                new RaftGroupEventsClientListener(),
                groupStoragesDestructionIntents,
                groupStoragesContextResolver
        );

        assertThat(server.startAsync(new ComponentContext()), willCompleteSuccessfully());

        clo.accept(server);

        servers.add(server);

        serverServices.add(service);

        assertTrue(waitForTopology(service, servers.size(), 15_000));

        return server;
    }

    /**
     * Starts client.
     *
     * @param groupId Group id.
     * @return The client.
     * @throws Exception If failed.
     */
    protected RaftGroupService startClient(ReplicationGroupId groupId) throws Exception {
        var addr = new NetworkAddress(getLocalAddress(), PORT);

        String consistentId = testNodeName(testInfo, PORT);

        PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(Set.of(consistentId));

        ClusterService clientNode = clusterService(CLIENT_PORT + clients.size(), List.of(addr), true);

        var commandsMarshaller = new ThreadLocalOptimizedMarshaller(clientNode.serializationRegistry());

        RaftGroupService client = RaftGroupServiceImpl.start(
                groupId,
                clientNode,
                FACTORY,
                raftConfiguration,
                configuration,
                executor,
                commandsMarshaller,
                new ThrottlingContextHolderImpl(raftConfiguration)
        );

        clients.add(client);

        return client;
    }

    protected int nodesCount() {
        return NODES;
    }
}
