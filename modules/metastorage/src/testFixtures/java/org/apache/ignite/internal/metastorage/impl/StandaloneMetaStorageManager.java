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

import static java.util.Collections.singleton;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.Serializable;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.service.BeforeApplyHandler;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.mockito.ArgumentCaptor;
import org.mockito.MockSettings;
import org.mockito.quality.Strictness;

/**
 * MetaStorageManager dummy implementation.
 *
 * <p>The manager sends Raft commands to the MetaStorageListener directly bypassing the Raft service.
 * The manager is also responsible for starting key-value storage as the parent MetaStorageManagerImpl class do.
 * Note: {@link #deployWatches()} should be called manually after all dependent components are started.
 */
@TestOnly
public class StandaloneMetaStorageManager extends MetaStorageManagerImpl {
    private static final String TEST_NODE_NAME = "standalone-ms-node";

    private static final UUID TEST_NODE_ID = UUID.randomUUID();

    private static final MockSettings LENIENT_SETTINGS = withSettings().strictness(Strictness.LENIENT);

    /** Creates standalone MetaStorage manager. */
    public static StandaloneMetaStorageManager create() {
        return create(TEST_NODE_NAME);
    }

    /** Creates standalone MetaStorage manager. */
    public static StandaloneMetaStorageManager create(HybridClock clock) {
        return create(TEST_NODE_NAME, clock);
    }

    /** Creates standalone MetaStorage manager. */
    public static StandaloneMetaStorageManager create(String nodeName) {
        var tracker = new ReadOperationForCompactionTracker();

        return create(new SimpleInMemoryKeyValueStorage(nodeName, tracker), tracker);
    }

    /** Creates standalone MetaStorage manager. */
    public static StandaloneMetaStorageManager create(String nodeName, HybridClock clock) {
        var tracker = new ReadOperationForCompactionTracker();

        return create(new SimpleInMemoryKeyValueStorage(nodeName, tracker), clock, tracker);
    }

    /**
     * Creates standalone MetaStorage manager for provided key-value storage. The manager is responsible for starting/stopping provided
     * key-value storage.
     *
     * @param keyValueStorage Key-value storage.
     */
    public static StandaloneMetaStorageManager create(KeyValueStorage keyValueStorage) {
        return create(keyValueStorage, new HybridClockImpl(), new ReadOperationForCompactionTracker());
    }

    /**
     * Creates standalone MetaStorage manager for provided key-value storage. The manager is responsible for starting/stopping provided
     * key-value storage.
     *
     * @param keyValueStorage Key-value storage.
     * @param readOperationForCompactionTracker Read operation tracker for metastorage compaction.
     */
    public static StandaloneMetaStorageManager create(
            KeyValueStorage keyValueStorage,
            ReadOperationForCompactionTracker readOperationForCompactionTracker
    ) {
        return create(keyValueStorage, new HybridClockImpl(), readOperationForCompactionTracker);
    }

    /**
     * Creates standalone MetaStorage manager for provided key-value storage. The manager is responsible for starting/stopping provided
     * key-value storage.
     *
     * @param keyValueStorage Key-value storage.
     * @param clock Clock.
     * @param readOperationForCompactionTracker Read operation tracker for metastorage compaction.
     */
    public static StandaloneMetaStorageManager create(
            KeyValueStorage keyValueStorage,
            HybridClock clock,
            ReadOperationForCompactionTracker readOperationForCompactionTracker
    ) {
        return new StandaloneMetaStorageManager(
                mockClusterService(),
                mockClusterGroupManager(),
                mock(LogicalTopologyService.class),
                mockRaftManager(),
                keyValueStorage,
                mock(TopologyAwareRaftGroupServiceFactory.class),
                mockConfiguration(),
                clock,
                RaftGroupOptionsConfigurer.EMPTY,
                readOperationForCompactionTracker
        );
    }

    private StandaloneMetaStorageManager(
            ClusterService clusterService,
            ClusterManagementGroupManager cmgMgr,
            LogicalTopologyService logicalTopologyService,
            RaftManager raftMgr,
            KeyValueStorage storage,
            TopologyAwareRaftGroupServiceFactory raftServiceFactory,
            MetaStorageConfiguration configuration,
            HybridClock clock,
            RaftGroupOptionsConfigurer raftGroupOptionsConfigurer,
            ReadOperationForCompactionTracker readOperationForCompactionTracker
    ) {
        super(
                clusterService,
                cmgMgr,
                logicalTopologyService,
                raftMgr,
                storage,
                clock,
                raftServiceFactory,
                new NoOpMetricManager(),
                configuration,
                raftGroupOptionsConfigurer,
                readOperationForCompactionTracker
        );
    }

    private static ClusterService mockClusterService() {
        ClusterService clusterService = mock(ClusterService.class, LENIENT_SETTINGS);

        when(clusterService.nodeName()).thenReturn(TEST_NODE_NAME);

        TopologyService topologyService = mock(TopologyService.class, LENIENT_SETTINGS);
        when(topologyService.localMember()).thenReturn(new ClusterNodeImpl(TEST_NODE_ID, TEST_NODE_NAME, mock(NetworkAddress.class)));

        when(clusterService.topologyService()).thenReturn(topologyService);

        return clusterService;
    }

    private static ClusterManagementGroupManager mockClusterGroupManager() {
        ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);

        when(cmgManager.metaStorageInfo()).thenReturn(completedFuture(
                new CmgMessagesFactory().metaStorageInfo().metaStorageNodes(Set.of(TEST_NODE_NAME)).build()
        ));

        configureCmgManagerToStartMetastorage(cmgManager);

        return cmgManager;
    }

    /**
     * Configures {@link ClusterManagementGroupManager} mock to return cluster state needed for {@link MetaStorageManagerImpl} start.
     *
     * @param cmgManagerMock Mock to configure.
     */
    public static void configureCmgManagerToStartMetastorage(ClusterManagementGroupManager cmgManagerMock) {
        ClusterState clusterState = mock(ClusterState.class);
        ClusterTag clusterTag = ClusterTag.randomClusterTag(new CmgMessagesFactory(), "cluster");

        when(clusterState.clusterTag()).thenReturn(clusterTag);
        when(cmgManagerMock.clusterState()).thenReturn(completedFuture(clusterState));
    }

    private static RaftManager mockRaftManager() {
        ArgumentCaptor<RaftGroupListener> listenerCaptor = ArgumentCaptor.forClass(RaftGroupListener.class);
        RaftManager raftManager = mock(RaftManager.class, LENIENT_SETTINGS);
        TopologyAwareRaftGroupService raftGroupService = mock(TopologyAwareRaftGroupService.class);

        try {
            when(raftManager.startRaftGroupNodeAndWaitNodeReady(
                    any(),
                    any(),
                    listenerCaptor.capture(),
                    any(),
                    any(),
                    any()
            )).thenReturn(raftGroupService);

            when(raftManager.startRaftGroupNodeAndWaitNodeReady(
                    any(),
                    any(),
                    listenerCaptor.capture(),
                    any(),
                    any(),
                    any(),
                    any()
            )).thenReturn(raftGroupService);
        } catch (NodeStoppingException e) {
            throw new RuntimeException(e);
        }

        when(raftGroupService.run(any())).thenAnswer(invocation -> {
            Command command = invocation.getArgument(0);
            RaftGroupListener listener = listenerCaptor.getValue();

            if (listener instanceof BeforeApplyHandler) {
                ((BeforeApplyHandler) listener).onBeforeApply(command);
            }

            return runCommand(command, listener);
        });

        return raftManager;
    }

    private static MetaStorageConfiguration mockConfiguration() {
        MetaStorageConfiguration configuration = mock(MetaStorageConfiguration.class, LENIENT_SETTINGS);
        ConfigurationValue<Long> value = mock(ConfigurationValue.class, LENIENT_SETTINGS);

        when(configuration.idleSyncTimeInterval()).thenReturn(value);
        when(value.value()).thenReturn(1000L);

        return configuration;
    }

    private static RaftConfiguration mockRaftConfiguration() {
        return mock(RaftConfiguration.class, LENIENT_SETTINGS);
    }

    private static CompletableFuture<Serializable> runCommand(Command command, RaftGroupListener listener) {
        CompletableFuture<Serializable> future = new CompletableFuture<>();

        CommandClosure<? extends Command> closure = new CommandClosure<>() {
            @Override
            public Command command() {
                return command;
            }

            @Override
            public void result(@Nullable Serializable res) {
                if (res instanceof Throwable) {
                    future.completeExceptionally((Throwable) res);
                } else {
                    future.complete(res);
                }
            }
        };

        try {
            if (command instanceof ReadCommand) {
                listener.onRead(singleton((CommandClosure<ReadCommand>) closure).iterator());
            } else {
                listener.onWrite(singleton((CommandClosure<WriteCommand>) closure).iterator());
            }
        } catch (Throwable e) {
            future.completeExceptionally(e);
        }

        return future;
    }
}
