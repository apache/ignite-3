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
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.mockito.ArgumentCaptor;

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

    /**
     * Creates standalone MetaStorage manager for provided VaultManager.
     */
    public static StandaloneMetaStorageManager create(VaultManager vaultManager) {
        return create(vaultManager, new SimpleInMemoryKeyValueStorage(TEST_NODE_NAME));
    }

    /**
     * Creates standalone MetaStorage manager for provided VaultManager and key-value storage.
     * The manager is responsible for starting/stopping provided key-value storage.
     *
     * @param vaultManager Vault manager.
     * @param keyValueStorage Key-value storage.
     */
    public static StandaloneMetaStorageManager create(VaultManager vaultManager, KeyValueStorage keyValueStorage) {
        return new StandaloneMetaStorageManager(
                vaultManager,
                mockClusterService(),
                mockClusterGroupManager(),
                mock(LogicalTopologyService.class),
                mockRaftManager(),
                keyValueStorage
        );
    }

    /**
     * The constructor.
     *
     * @param vaultMgr Vault manager.
     * @param clusterService Cluster network service.
     * @param cmgMgr Cluster management service Manager.
     * @param logicalTopologyService Logical topology service.
     * @param raftMgr Raft manager.
     * @param storage Storage. This component owns this resource and will manage its lifecycle.
     */
    private StandaloneMetaStorageManager(VaultManager vaultMgr, ClusterService clusterService, ClusterManagementGroupManager cmgMgr,
            LogicalTopologyService logicalTopologyService, RaftManager raftMgr, KeyValueStorage storage) {
        super(vaultMgr, clusterService, cmgMgr, logicalTopologyService, raftMgr, storage);
    }

    private static ClusterService mockClusterService() {
        ClusterNode localNode = new ClusterNode(TEST_NODE_NAME, TEST_NODE_NAME, mock(NetworkAddress.class));

        ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
        when(clusterService.topologyService().localMember()).thenReturn(localNode);

        return clusterService;
    }

    private static ClusterManagementGroupManager mockClusterGroupManager() {
        ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);
        when(cmgManager.metaStorageNodes()).thenReturn(completedFuture(Set.of(TEST_NODE_NAME)));

        return cmgManager;
    }

    private static RaftManager mockRaftManager() {
        ArgumentCaptor<RaftGroupListener> listenerCaptor = ArgumentCaptor.forClass(RaftGroupListener.class);
        RaftManager raftManager = mock(RaftManager.class);
        RaftGroupService raftGroupService = mock(RaftGroupService.class);

        try {
            when(raftManager.startRaftGroupNode(any(), any(), listenerCaptor.capture(), any())).thenReturn(
                    completedFuture(raftGroupService));
        } catch (NodeStoppingException e) {
            throw new RuntimeException(e);
        }

        when(raftGroupService.run(any())).thenAnswer(invocation -> {
            Command command = invocation.getArgument(0);
            RaftGroupListener listener = listenerCaptor.getValue();
            return runCommand(command, listener);
        });

        return raftManager;
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

        if (command instanceof ReadCommand) {
            listener.onRead(singleton((CommandClosure<ReadCommand>) closure).iterator());
        } else {
            listener.onWrite(singleton((CommandClosure<WriteCommand>) closure).iterator());
        }

        return future;
    }
}
