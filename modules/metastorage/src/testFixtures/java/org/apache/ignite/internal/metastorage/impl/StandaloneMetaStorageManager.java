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
import static java.util.concurrent.CompletableFuture.failedFuture;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/**
 * MetaStorageManager dummy implementation.
 */
@TestOnly
public class StandaloneMetaStorageManager extends MetaStorageManagerImpl {
    private static final String TEST_NODE_NAME = "test";

    /**
     * Creates standalone MetaStorage manager for provided VaultManager.
     */
    public static StandaloneMetaStorageManager create(VaultManager vaultMgr) {
        return create(vaultMgr, new SimpleInMemoryKeyValueStorage(TEST_NODE_NAME));
    }

    /**
     * Creates standalone MetaStorage manager for provided VaultManager.
     */
    public static StandaloneMetaStorageManager create(VaultManager vaultMgr, KeyValueStorage storage) {
        return new StandaloneMetaStorageManager(
                vaultMgr,
                Mockito.mock(ClusterService.class),
                Mockito.mock(ClusterManagementGroupManager.class),
                Mockito.mock(LogicalTopologyService.class),
                Mockito.mock(RaftManager.class),
                storage
        );
    }

    private final KeyValueStorage keyValueStorage;

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
    public StandaloneMetaStorageManager(VaultManager vaultMgr, ClusterService clusterService, ClusterManagementGroupManager cmgMgr,
            LogicalTopologyService logicalTopologyService, RaftManager raftMgr, KeyValueStorage storage) {
        super(vaultMgr, clusterService, cmgMgr, logicalTopologyService, raftMgr, storage);

        keyValueStorage = storage;
    }

    @Override
    public void start() {
        keyValueStorage.start();

        MetaStorageListener metaStorageListener = new MetaStorageListener(keyValueStorage);

        RaftGroupService raftGroupService = Mockito.mock(RaftGroupService.class);
        Mockito.when(raftGroupService.run(ArgumentMatchers.any()))
                .thenAnswer(invocation -> runCommand(invocation.getArgument(0), metaStorageListener));

        var localNode = new ClusterNode(TEST_NODE_NAME, TEST_NODE_NAME, new NetworkAddress("localhost", 10000));

        MetaStorageServiceImpl service = new MetaStorageServiceImpl(raftGroupService, new IgniteSpinBusyLock(), localNode);

        metaStorageServiceFuture().complete(service);
    }

    private static CompletableFuture<Serializable> runCommand(Command command, MetaStorageListener listener) {
        AtomicReference<CompletableFuture<Serializable>> resRef = new AtomicReference<>();

        CommandClosure<? extends Command> closure = new CommandClosure<>() {
            @Override
            public Command command() {
                return command;
            }

            @Override
            public void result(@Nullable Serializable res) {
                resRef.set(res instanceof Throwable ? failedFuture((Throwable) res) : completedFuture(res));
            }
        };

        if (command instanceof ReadCommand) {
            listener.onRead(singleton((CommandClosure<ReadCommand>) closure).iterator());
        } else {
            listener.onWrite(singleton((CommandClosure<WriteCommand>) closure).iterator());
        }

        return resRef.get();
    }
}
