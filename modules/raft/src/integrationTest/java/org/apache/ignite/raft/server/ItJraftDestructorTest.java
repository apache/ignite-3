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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.internal.cluster.management.CmgGroupId;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.LogStorageException;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests that check that failed storage destruction is finished after server restart. */
public class ItJraftDestructorTest extends JraftAbstractTest {

    private static Stream<ReplicationGroupId> groupIdProvider() {
        return Stream.of(
                new TablePartitionId(0, 0),
                CmgGroupId.INSTANCE,
                MetastorageGroupId.INSTANCE
        );
    }

    private JraftServerImpl server;
    private Path serverDataPath;
    private Peer peer;

    @BeforeEach
    void setUp() {
        server = startServer(0, x -> {}, opts -> {});

        serverDataPath = serverWorkingDirs.get(0).basePath();

        String localNodeName = server.clusterService().topologyService().localMember().name();

        peer = Objects.requireNonNull(initialMembersConf.peer(localNodeName));

        LogStorageFactory logStorageFactory = logStorageFactories.get(0);

        doThrow(LogStorageException.class).doCallRealMethod().when(logStorageFactory).destroyLogStorage(anyString());
    }

    @ParameterizedTest
    @MethodSource("groupIdProvider")
    void testFinishStorageDestructionAfterRestart(ReplicationGroupId replicationGroupId) throws Exception {
        RaftNodeId nodeId = new RaftNodeId(replicationGroupId, peer);

        destroyStorageFails(server, nodeId, serverDataPath, false);

        Path nodeDataPath = createNodeDataDirectory(nodeId);

        restartServer();

        assertFalse(Files.exists(nodeDataPath));

        // New log storage factory was created after restart.
        verify(logStorageFactories.get(0), times(1)).destroyLogStorage(anyString());
    }

    @Test
    void testVolatileLogsDontGetDestroyedOnRestart() throws Exception {
        RaftNodeId nodeId = new RaftNodeId(new TablePartitionId(0, 0), peer);

        destroyStorageFails(server, nodeId, serverDataPath, true);

        Path nodeDataPath = createNodeDataDirectory(nodeId);

        restartServer();

        assertFalse(Files.exists(nodeDataPath));

        // New log storage factory was created after restart.
        verify(logStorageFactories.get(0), times(0)).destroyLogStorage(anyString());
    }

    private void destroyStorageFails(
            JraftServerImpl server,
            RaftNodeId nodeId,
            Path serverDataPath,
            boolean isVolatile
    ) {
        RaftGroupOptions groupOptions = isVolatile ? RaftGroupOptions.forVolatileStores() : RaftGroupOptions.forPersistentStores();

        groupOptions.setLogStorageFactory(logStorageFactories.get(0)).serverDataPath(serverDataPath);

        assertThrows(
                IgniteInternalException.class,
                () -> server.destroyRaftNodeStorages(nodeId, groupOptions),
                "Failed to delete storage for node: "
        );

        verify(groupOptions.getLogStorageFactory(), times(1)).destroyLogStorage(anyString());
    }

    private Path createNodeDataDirectory(RaftNodeId nodeId) throws IOException {
        Path nodeDataPath = JraftServerImpl.getServerDataPath(serverDataPath, nodeId);

        return Files.createDirectories(nodeDataPath);
    }

    private void restartServer() throws Exception {
        shutdownCluster();

        startServer(0, x -> {}, opts -> {});
    }
}
