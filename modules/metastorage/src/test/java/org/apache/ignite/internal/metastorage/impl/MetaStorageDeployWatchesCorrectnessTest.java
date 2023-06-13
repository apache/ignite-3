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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterService;
import org.junit.jupiter.api.Test;

/**
 * There are tests of correctness invocation {@link MetaStorageManager#deployWatches()}.
 */
public class MetaStorageDeployWatchesCorrectnessTest extends IgniteAbstractTest {
    @Test
    public void tesMetaStorageManager() throws Exception {
        HybridClock clock = new HybridClockImpl();
        String mcNodeName = "mc-node-1";

        ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);
        ClusterService clusterService = mock(ClusterService.class);
        LogicalTopologyService logicalTopologyService = mock(LogicalTopologyService.class);
        RaftManager raftManager = mock(RaftManager.class);

        when(cmgManager.metaStorageNodes()).thenReturn(completedFuture(Set.of(mcNodeName)));
        when(clusterService.nodeName()).thenReturn(mcNodeName);
        when(raftManager.startRaftGroupNodeAndWaitNodeReadyFuture(any(), any(), any(), any(), any())).thenReturn(completedFuture(null));

        var metastore = new MetaStorageManagerImpl(
                new VaultManager(new InMemoryVaultService()),
                clusterService,
                cmgManager,
                logicalTopologyService,
                raftManager,
                new SimpleInMemoryKeyValueStorage(mcNodeName),
                clock
        );

        checkCorrectness(metastore);
    }

    @Test
    public void tesStandaloneMetaStorageManager() throws Exception {
        var metastore = StandaloneMetaStorageManager.create(new VaultManager(new InMemoryVaultService()));

        checkCorrectness(metastore);
    }

    /**
     * Invokes {@link MetaStorageManager#deployWatches()} and checks result.
     *
     * @param metastore Meta storage.
     * @throws NodeStoppingException If failed.
     */
    private static void checkCorrectness(MetaStorageManager metastore) throws NodeStoppingException {
        var deployWatchesFut = metastore.deployWatches();

        assertFalse(deployWatchesFut.isDone());

        metastore.start();

        assertTrue(deployWatchesFut.isDone());
    }
}
