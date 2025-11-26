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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.StoredRaftNodeId;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.LogStorageException;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.raft.jraft.core.TestCluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ItJraftServerTest extends JraftAbstractTest {
    private static final int SERVER_INDEX = 0;

    private JraftServerImpl server;
    private Path serverDataPath;

    private final TestReplicationGroupId testReplicationGroupId = new TestReplicationGroupId("test");

    @BeforeEach
    void setUp() {
        server = startServer(SERVER_INDEX);
        serverDataPath = serverWorkingDirs.get(SERVER_INDEX).metaPath();
    }

    @Test
    void testDurableStorageDestructionFinishAfterRestart() throws Exception {
        doTestDurableStorageDestructionFinishAfterRestart(false);

        // New log storage factory was created after restart.
        verify(logStorageFactories.get(SERVER_INDEX), times(1)).destroyLogStorage(anyString());
    }

    @Test
    void testVolatileLogStorageIsNotDestroyedOnRestart() throws Exception {
        doTestDurableStorageDestructionFinishAfterRestart(true);

        // New log storage factory was created after restart.
        verify(logStorageFactories.get(SERVER_INDEX), never()).destroyLogStorage(anyString());
    }

    private void doTestDurableStorageDestructionFinishAfterRestart(boolean isVolatile) throws Exception {
        RaftNodeId nodeId = testGroupRaftNodeId();

        Path nodeDataPath = createServerDataPathForNode(serverDataPath, nodeId);

        // Log storage destruction must fail, so raft server will save the intent to destroy the storage
        // and will complete it successfully on restart.
        LogStorageFactory logStorageFactory = logStorageFactories.get(SERVER_INDEX);
        doThrow(LogStorageException.class).doCallRealMethod().when(logStorageFactory).destroyLogStorage(anyString());

        RaftGroupOptions groupOptions = getRaftGroupOptions(isVolatile, logStorageFactory);

        assertThrows(
                IgniteInternalException.class,
                () -> server.destroyRaftNodeStoragesDurably(nodeId, groupOptions),
                "Failed to delete storage for node: "
        );

        verify(logStorageFactory, times(1)).destroyLogStorage(anyString());

        // Node data path deletion happens after log storage destruction, so it should be intact.
        assertTrue(Files.exists(nodeDataPath));

        shutdownCluster();

        startServer(SERVER_INDEX);

        assertFalse(Files.exists(nodeDataPath));
    }

    private RaftNodeId testGroupRaftNodeId() {
        return new RaftNodeId(testReplicationGroupId, localPeer());
    }

    private Peer localPeer() {
        String localNodeName = server.clusterService().topologyService().localMember().name();
        return localPeer(localNodeName);
    }

    private Peer localPeer(String localNodeName) {
        return Objects.requireNonNull(initialMembersConf.peer(localNodeName));
    }

    private static Path createServerDataPathForNode(Path serverDataPath, RaftNodeId nodeId) throws IOException {
        Path nodeDataPath = JraftServerImpl.getServerDataPath(serverDataPath, nodeId);

        Files.createDirectories(nodeDataPath);

        return nodeDataPath;
    }

    private RaftGroupOptions getRaftGroupOptions(boolean isVolatile, LogStorageFactory logStorageFactory) {
        RaftGroupOptions groupOptions = isVolatile ? RaftGroupOptions.forVolatileStores() : RaftGroupOptions.forPersistentStores();
        groupOptions.setLogStorageFactory(logStorageFactory);
        groupOptions.serverDataPath(serverDataPath);
        groupOptions.commandsMarshaller(TestCluster.commandsMarshaller(server.clusterService()));
        return groupOptions;
    }

    private JraftServerImpl startServer(int index) {
        return startServer(index, x -> {}, opts -> {});
    }

    @Test
    void listsGroupIdsOnDisk() {
        RaftNodeId nodeId = testGroupRaftNodeId();

        RaftGroupOptions groupOptions = getRaftGroupOptions(false, logStorageFactories.get(SERVER_INDEX));
        assertTrue(server.startRaftNode(nodeId, initialMembersConf, mock(RaftGroupListener.class), groupOptions));

        assertThat(server.raftNodeIdsOnDisk(), contains(new StoredRaftNodeId(nodeId.groupId().toString(), nodeId.peer())));
    }
}
