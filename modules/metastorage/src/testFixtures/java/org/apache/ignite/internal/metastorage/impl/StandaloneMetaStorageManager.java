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
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.service.BeforeApplyHandler;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
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

    private static final MockSettings LENIENT_SETTINGS = withSettings().strictness(Strictness.LENIENT);

    /**
     * Creates standalone MetaStorage manager.
     */
    public static StandaloneMetaStorageManager create() {
        return create(new SimpleInMemoryKeyValueStorage(TEST_NODE_NAME));
    }

    /**
     * Creates standalone MetaStorage manager for provided key-value storage. The manager is responsible for starting/stopping provided
     * key-value storage.
     *
     * @param keyValueStorage Key-value storage.
     */
    public static StandaloneMetaStorageManager create(KeyValueStorage keyValueStorage) {
        return create(keyValueStorage, new HybridClockImpl());
    }

    /**
     * Creates standalone MetaStorage manager for provided key-value storage. The manager is responsible for starting/stopping provided
     * key-value storage.
     *
     * @param keyValueStorage Key-value storage.
     * @param clock Clock.
     */
    public static StandaloneMetaStorageManager create(KeyValueStorage keyValueStorage, HybridClock clock) {
        return new StandaloneMetaStorageManager(
                mockClusterService(),
                mockClusterGroupManager(),
                mock(LogicalTopologyService.class),
                mockRaftManager(),
                keyValueStorage,
                mock(TopologyAwareRaftGroupServiceFactory.class),
                mockConfiguration(),
                clock
        );
    }

    /**
     * The constructor.
     *
     * @param clusterService Cluster network service.
     * @param cmgMgr Cluster management service Manager.
     * @param logicalTopologyService Logical topology service.
     * @param raftMgr Raft manager.
     * @param storage Storage. This component owns this resource and will manage its lifecycle.
     */
    private StandaloneMetaStorageManager(
            ClusterService clusterService,
            ClusterManagementGroupManager cmgMgr,
            LogicalTopologyService logicalTopologyService,
            RaftManager raftMgr,
            KeyValueStorage storage,
            TopologyAwareRaftGroupServiceFactory raftServiceFactory,
            MetaStorageConfiguration configuration,
            HybridClock clock
    ) {
        super(
                clusterService,
                cmgMgr,
                logicalTopologyService,
                raftMgr,
                storage,
                clock,
                raftServiceFactory,
                configuration
        );
    }

    private static ClusterService mockClusterService() {
        ClusterService clusterService = mock(ClusterService.class);

        when(clusterService.nodeName()).thenReturn(TEST_NODE_NAME);

        return clusterService;
    }

    private static ClusterManagementGroupManager mockClusterGroupManager() {
        ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);
        when(cmgManager.metaStorageNodes()).thenReturn(completedFuture(Set.of(TEST_NODE_NAME)));

        return cmgManager;
    }

    private static RaftManager mockRaftManager() {
        ArgumentCaptor<RaftGroupListener> listenerCaptor = ArgumentCaptor.forClass(RaftGroupListener.class);
        RaftManager raftManager = mock(RaftManager.class, LENIENT_SETTINGS);
        TopologyAwareRaftGroupService raftGroupService = mock(TopologyAwareRaftGroupService.class);

        try {
            when(raftManager.startRaftGroupNodeAndWaitNodeReadyFuture(
                    any(),
                    any(),
                    listenerCaptor.capture(),
                    any(),
                    any()
            )).thenReturn(completedFuture(raftGroupService));

            when(raftManager.startRaftGroupNodeAndWaitNodeReadyFuture(
                    any(),
                    any(),
                    listenerCaptor.capture(),
                    any(),
                    any(),
                    any()
            )).thenReturn(completedFuture(raftGroupService));
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
